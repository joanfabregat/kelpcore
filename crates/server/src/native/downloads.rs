//! File downloads over the pane stream (capability `file-downloads`, an
//! addition to `files`): a regular file's bytes, or files and folders as one
//! gzip-compressed tar archive, streamed to the client in acknowledged
//! chunks, the reverse of uploads.
//!
//! A download runs on its own worker thread, which reads the files and
//! produces chunks into a bounded channel; the event loop takes a chunk only
//! while the client has fewer than [`MAX_CHUNKS_IN_FLIGHT`] unacknowledged
//! ones and its output queue has room (see `poll_downloads` in the server),
//! so a slow client stalls its download rather than filling the server's
//! memory or output queue. Nothing is built whole in memory or on disk.
//!
//! The same care as the other file operations applies: a single file is
//! opened like `file_read` opens it (the type checked first, never a device
//! or FIFO); an archive walks its folders through directory descriptors
//! opened with `O_NOFOLLOW`, stores symlinks as links without following
//! them, opens each file with `O_NOFOLLOW`, never enters another file
//! system, and skips devices, FIFOs and sockets. Depth, entry count and
//! input bytes are bounded: an archive over a bound is refused with
//! `too_large` before its first byte when the planning walk sees it, and
//! fails the same way if the tree grows past it while streaming.
//!
//! Archive format: POSIX ustar headers, with a pax extended header for any
//! name or link target longer than 100 bytes or not ASCII, a size of 8 GiB
//! or more, or a modification time before 1970 or after 2242. Entries carry
//! permission bits (`mode & 0o777`) and the modification time, owner and
//! group 0 without names: the archive does not disclose the server's
//! account names, and an extracting user owns what they extract.
use super::files::{self, Code, Error, Outcome};
use flate2::{Compression, write::GzEncoder};
use serde::{Deserialize, Serialize};
use std::{
    ffi::{CStr, OsStr},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, OwnedFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
};

/// Bytes per chunk (decoded). The base64 record is about 342 KiB, like an
/// upload chunk, so a chunk never delays pane updates for long.
pub const CHUNK_BYTES: usize = 256 * 1024;
/// Chunks sent and not yet acknowledged, per download.
pub const MAX_CHUNKS_IN_FLIGHT: usize = 8;
/// Unfinished downloads per connection.
pub const MAX_DOWNLOADS: usize = 2;
/// Download threads of the whole server (including cancelled downloads
/// whose thread has not returned yet).
pub const MAX_SERVER_DOWNLOADS: usize = 8;
/// Most entries (files, folders and symlinks) in one archive.
pub const MAX_ARCHIVE_ENTRIES: usize = 100_000;
/// Most input bytes (the sum of the archived files' sizes) in one archive.
pub const MAX_ARCHIVE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
/// gzip level: miniz level 2 keeps one stream above gigabit rates on one
/// core for most of the size gain of higher levels (measured by Crabinet,
/// which streams ZIP archives the same way).
const LEVEL: u32 = 2;
/// Chunks a download thread may produce ahead of the event loop.
const AHEAD: usize = 2;
const BLOCK: usize = 512;
/// Largest value of an 11-digit octal header field.
const OCTAL_MAX: u64 = 0o777_7777_7777;

/// Download bounds, advertised in Welcome `file_limits.downloads`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub chunk_bytes: usize,
    pub max_chunks_in_flight: usize,
    pub max_downloads: usize,
    pub max_archive_entries: usize,
    pub max_archive_bytes: u64,
}
pub const LIMITS: Limits = Limits {
    chunk_bytes: CHUNK_BYTES,
    max_chunks_in_flight: MAX_CHUNKS_IN_FLIGHT,
    max_downloads: MAX_DOWNLOADS,
    max_archive_entries: MAX_ARCHIVE_ENTRIES,
    max_archive_bytes: MAX_ARCHIVE_BYTES,
};

/// The archive format of a download.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Archive {
    /// A gzip-compressed POSIX tar archive.
    #[serde(rename = "tar.gz")]
    TarGz,
}

/// What a download thread produces, in this order: `Started` or `Failed`;
/// then chunks; then `Done` or `Failed`.
#[derive(Debug)]
pub enum Piece {
    Started {
        name: String,
        size: Option<u64>,
        archive: Option<Archive>,
        entries: Option<u64>,
        input_bytes: Option<u64>,
    },
    Chunk {
        data: Vec<u8>,
        /// Archives: input bytes read so far.
        input: Option<u64>,
    },
    Done {
        size: u64,
        hash: String,
        skipped: u64,
        changed: u64,
    },
    Failed(Error),
}

/// A running download, as the event loop holds it. Dropping it cancels the
/// download: the thread stops at its next read or chunk.
pub struct Job {
    pub receiver: mpsc::Receiver<Piece>,
    cancel: Arc<AtomicBool>,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}
/// Counts a download thread from its start until it returns.
struct Running(Arc<AtomicUsize>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Start a download on its own thread; `threads` counts running download
/// threads.
pub fn start(
    paths: Vec<String>,
    archive: Option<Archive>,
    threads: &Arc<AtomicUsize>,
) -> io::Result<Job> {
    let (sender, receiver) = mpsc::sync_channel(AHEAD);
    let cancel = Arc::new(AtomicBool::new(false));
    threads.fetch_add(1, Ordering::Relaxed);
    let running = Running(threads.clone());
    let flag = cancel.clone();
    std::thread::Builder::new()
        .name("kelpserver-download".into())
        .spawn(move || {
            let _running = running;
            run(paths, archive, Output::new(sender, flag));
        })?;
    Ok(Job { receiver, cancel })
}

fn cancelled() -> io::Error {
    // Not `Interrupted`, which `write_all` retries.
    io::Error::new(io::ErrorKind::ConnectionAborted, "download cancelled")
}
fn is_cancelled(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::ConnectionAborted && error.to_string() == "download cancelled"
}

/// The bytes sent: hashed, counted and cut into chunks for the channel.
struct Output {
    sender: mpsc::SyncSender<Piece>,
    cancel: Arc<AtomicBool>,
    buffer: Vec<u8>,
    size: u64,
    hash: super::sha256::Sha256,
    /// Archives: input bytes read so far, reported with each chunk.
    input: Option<Arc<AtomicU64>>,
}
impl Output {
    fn new(sender: mpsc::SyncSender<Piece>, cancel: Arc<AtomicBool>) -> Self {
        Self {
            sender,
            cancel,
            buffer: Vec::with_capacity(CHUNK_BYTES),
            size: 0,
            hash: Default::default(),
            input: None,
        }
    }
    fn stopped(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }
    fn send(&self, piece: Piece) -> io::Result<()> {
        if self.stopped() {
            return Err(cancelled());
        }
        // Blocks while the event loop is `AHEAD` chunks behind; fails once
        // the download is dropped.
        self.sender.send(piece).map_err(|_| cancelled())
    }
    fn emit(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let data = std::mem::replace(&mut self.buffer, Vec::with_capacity(CHUNK_BYTES));
        self.hash.update(&data);
        self.size += data.len() as u64;
        let input = self.input.as_ref().map(|n| n.load(Ordering::Relaxed));
        self.send(Piece::Chunk { data, input })
    }
    /// The last chunk; the total size and hash.
    fn finish(mut self) -> io::Result<(u64, String, Self)> {
        self.emit()?;
        let hash = std::mem::take(&mut self.hash).hex();
        Ok((self.size, hash, self))
    }
}
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.stopped() {
            return Err(cancelled());
        }
        let n = bytes.len().min(CHUNK_BYTES - self.buffer.len());
        self.buffer.extend_from_slice(&bytes[..n]);
        if self.buffer.len() == CHUNK_BYTES {
            self.emit()?;
        }
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        // Chunks are whole until the end; `finish` sends the last one.
        Ok(())
    }
}

fn run(paths: Vec<String>, archive: Option<Archive>, output: Output) {
    let sender = output.sender.clone();
    let cancel = output.cancel.clone();
    let failure = match download(paths, archive, output) {
        Ok(done) => {
            let _ = sender.send(done);
            return;
        }
        Err(error) => error,
    };
    if !cancel.load(Ordering::Relaxed) {
        let _ = sender.send(Piece::Failed(failure));
    }
}
fn download(paths: Vec<String>, archive: Option<Archive>, output: Output) -> Outcome<Piece> {
    let home = files::home();
    if paths.is_empty() || paths.len() > files::MAX_PATHS {
        return Err(Error::new(
            Code::InvalidRequest,
            format!("name 1 to {} paths", files::MAX_PATHS),
        ));
    }
    let mut normalized = Vec::with_capacity(paths.len());
    for path in &paths {
        let at = |mut error: Error| {
            error.path = Some(path.clone());
            error
        };
        let checked = files::normalize(path, home.as_deref()).map_err(at)?;
        if checked.file_name().is_none() {
            return Err(at(Error::new(
                Code::InvalidPath,
                "the root directory has no name",
            )));
        }
        normalized.push(checked);
    }
    match archive {
        None if normalized.len() == 1 => raw(&normalized[0], output),
        None => Err(Error::new(
            Code::InvalidRequest,
            "several paths download only as an archive",
        )),
        Some(Archive::TarGz) => tar_gz(&normalized, output),
    }
}
/// Map an output failure: a cancelled download, or an I/O error at `path`.
fn output_error(error: io::Error, path: &Path) -> Error {
    if is_cancelled(&error) {
        Error::new(Code::Io, "download cancelled")
    } else {
        files::io_error(error, path)
    }
}

/// One regular file, as itself.
fn raw(path: &Path, mut output: Output) -> Outcome<Piece> {
    let cpath =
        files::cstring(path.as_os_str().as_bytes()).map_err(|e| files::io_error(e, path))?;
    // The type first (following a final symlink, as `file_read` does):
    // opening a device or FIFO can have side effects.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    files::check(unsafe { libc::fstatat(libc::AT_FDCWD, cpath.as_ptr(), &mut st, 0) })
        .map_err(|e| files::io_error(e, path))?;
    if files::is_dir(&st) {
        return Err(
            Error::new(Code::IsADirectory, "a folder downloads only as an archive").at(path),
        );
    }
    files::regular(&st, path, "download")?;
    let fd = files::open_at(
        libc::AT_FDCWD,
        &cpath,
        libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOCTTY,
        0,
    )
    .map_err(|e| files::io_error(e, path))?;
    let st = files::stat_fd(fd.as_raw_fd()).map_err(|e| files::io_error(e, path))?;
    files::regular(&st, path, "download")?;
    output
        .send(Piece::Started {
            name: name_text(path),
            size: Some(st.st_size as u64),
            archive: None,
            entries: None,
            input_bytes: None,
        })
        .map_err(|e| output_error(e, path))?;
    let mut file = File::from(fd);
    let mut buffer = vec![0u8; CHUNK_BYTES];
    loop {
        let n = match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(files::io_error(error, path)),
        };
        output
            .write_all(&buffer[..n])
            .map_err(|e| output_error(e, path))?;
    }
    let (size, hash, _) = output.finish().map_err(|e| output_error(e, path))?;
    Ok(Piece::Done {
        size,
        hash,
        skipped: 0,
        changed: 0,
    })
}
fn name_text(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".into())
}

/// One selected entry of an archive: its parent directory and `lstat`.
struct Root {
    path: PathBuf,
    location: files::Location,
    st: libc::stat,
}
/// What the planning walk found.
#[derive(Default)]
struct Plan {
    entries: u64,
    bytes: u64,
}
fn too_many_entries(path: &Path) -> Error {
    Error::new(
        Code::TooLarge,
        format!("the archive would hold more than {MAX_ARCHIVE_ENTRIES} entries"),
    )
    .at(path)
}
fn too_many_bytes(path: &Path) -> Error {
    Error::new(
        Code::TooLarge,
        format!("the archive would read more than {MAX_ARCHIVE_BYTES} bytes"),
    )
    .at(path)
}
fn too_deep(path: &Path) -> Error {
    Error::new(
        Code::TooLarge,
        format!("folders are nested deeper than {} levels", files::MAX_DEPTH),
    )
    .at(path)
}
fn is_denied(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::EACCES | libc::EPERM))
}
fn is_gone(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR))
}
/// A directory's names in byte order, refusing one with more than `room`.
fn sorted_names(dir: RawFd, room: u64, path: &Path) -> Outcome<Vec<std::ffi::CString>> {
    let limit = usize::try_from(room)
        .unwrap_or(usize::MAX)
        .saturating_add(1);
    let (mut names, more) = files::names(dir, limit).map_err(|e| files::io_error(e, path))?;
    if more || names.len() as u64 > room {
        return Err(too_many_entries(path));
    }
    names.sort();
    Ok(names)
}
fn open_dir_at(dir: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    files::open_at(dir, name, files::DIRECTORY | libc::O_NOFOLLOW, 0)
}
/// Count what an archive of a folder would hold, within the bounds.
fn plan_tree(
    dir: RawFd,
    depth: usize,
    dev: libc::dev_t,
    path: &Path,
    plan: &mut Plan,
) -> Outcome<()> {
    if depth > files::MAX_DEPTH {
        return Err(too_deep(path));
    }
    let room = MAX_ARCHIVE_ENTRIES as u64 - plan.entries;
    for name in sorted_names(dir, room, path)? {
        let child = path.join(OsStr::from_bytes(name.to_bytes()));
        let Ok(st) = files::stat_at(dir, &name) else {
            continue;
        };
        plan_entry(dir, &name, &st, depth, dev, &child, plan)?;
    }
    Ok(())
}
fn plan_entry(
    dir: RawFd,
    name: &CStr,
    st: &libc::stat,
    depth: usize,
    dev: libc::dev_t,
    path: &Path,
    plan: &mut Plan,
) -> Outcome<()> {
    let regular = files::is_regular(st);
    if !(regular || files::is_dir(st) || files::is_symlink(st)) {
        return Ok(());
    }
    plan.entries += 1;
    if plan.entries > MAX_ARCHIVE_ENTRIES as u64 {
        return Err(too_many_entries(path));
    }
    if regular {
        plan.bytes += st.st_size as u64;
        if plan.bytes > MAX_ARCHIVE_BYTES {
            return Err(too_many_bytes(path));
        }
    } else if files::is_dir(st) && st.st_dev == dev {
        if let Ok(child) = open_dir_at(dir, name) {
            plan_tree(child.as_raw_fd(), depth + 1, dev, path, plan)?;
        }
    }
    Ok(())
}

/// Files and folders as one gzip-compressed tar archive.
fn tar_gz(paths: &[PathBuf], mut output: Output) -> Outcome<Piece> {
    let mut roots = Vec::with_capacity(paths.len());
    let mut names = std::collections::BTreeSet::new();
    for path in paths {
        let location = files::locate(path)?;
        let st = files::stat_at(location.dir.as_raw_fd(), &location.name)
            .map_err(|e| files::io_error(e, path))?;
        if !names.insert(location.name.clone()) {
            return Err(Error::new(
                Code::InvalidRequest,
                "two selected entries have the same name",
            )
            .at(path));
        }
        if !(files::is_regular(&st) || files::is_dir(&st) || files::is_symlink(&st)) {
            return Err(Error::new(Code::NotAFile, "not a file, folder or symlink").at(path));
        }
        roots.push(Root {
            path: path.clone(),
            location,
            st,
        });
    }
    let mut plan = Plan::default();
    for root in &roots {
        plan_entry(
            root.location.dir.as_raw_fd(),
            &root.location.name,
            &root.st,
            0,
            root.st.st_dev,
            &root.path,
            &mut plan,
        )?;
    }
    let name = match &roots[..] {
        [root] => format!("{}.tar.gz", name_text(&root.path)),
        _ => {
            let parent = roots[0].path.parent();
            if parent.is_some_and(|parent| roots.iter().all(|r| r.path.parent() == Some(parent)))
                && parent.and_then(Path::file_name).is_some()
            {
                format!("{}.tar.gz", name_text(parent.unwrap()))
            } else {
                "download.tar.gz".into()
            }
        }
    };
    let input = Arc::new(AtomicU64::new(0));
    output.input = Some(input.clone());
    let first = &roots[0].path;
    output
        .send(Piece::Started {
            name,
            size: None,
            archive: Some(Archive::TarGz),
            entries: Some(plan.entries),
            input_bytes: Some(plan.bytes),
        })
        .map_err(|e| output_error(e, first))?;
    let mut tar = Tar {
        out: GzEncoder::new(output, Compression::new(LEVEL)),
        input,
        entries: 0,
        bytes: 0,
        skipped: 0,
        changed: 0,
        buffer: vec![0u8; CHUNK_BYTES],
    };
    for root in &roots {
        let name = root.location.name.to_bytes().to_vec();
        tar.entry(
            root.location.dir.as_raw_fd(),
            &root.location.name,
            &root.st,
            &name,
            0,
            root.st.st_dev,
            &root.path,
        )?;
    }
    // The end of archive: two zero blocks.
    tar.out
        .write_all(&[0u8; 2 * BLOCK])
        .map_err(|e| output_error(e, first))?;
    let Tar {
        out,
        skipped,
        changed,
        ..
    } = tar;
    let output = out.finish().map_err(|e| output_error(e, first))?;
    let (size, hash, _) = output.finish().map_err(|e| output_error(e, first))?;
    Ok(Piece::Done {
        size,
        hash,
        skipped,
        changed,
    })
}

struct Tar {
    out: GzEncoder<Output>,
    input: Arc<AtomicU64>,
    entries: u64,
    bytes: u64,
    /// Entries left out: devices, FIFOs, sockets, what could not be opened
    /// for lack of permission, and the contents of mount points.
    skipped: u64,
    /// Files that changed size or modification time while being read.
    changed: u64,
    buffer: Vec<u8>,
}
impl Tar {
    fn write(&mut self, bytes: &[u8], path: &Path) -> Outcome<()> {
        self.out.write_all(bytes).map_err(|e| output_error(e, path))
    }
    fn count(&mut self, path: &Path) -> Outcome<()> {
        self.entries += 1;
        if self.entries > MAX_ARCHIVE_ENTRIES as u64 {
            return Err(too_many_entries(path));
        }
        Ok(())
    }
    fn header(&mut self, header: Header<'_>, path: &Path) -> Outcome<()> {
        let mut blocks = Vec::with_capacity(4 * BLOCK);
        header.encode(&mut blocks);
        self.write(&blocks, path)
    }
    /// Archive one entry as `archived` (a folder with its tree).
    #[allow(clippy::too_many_arguments)]
    fn entry(
        &mut self,
        dir: RawFd,
        name: &CStr,
        st: &libc::stat,
        archived: &[u8],
        depth: usize,
        dev: libc::dev_t,
        path: &Path,
    ) -> Outcome<()> {
        let mode = st.st_mode & 0o777;
        if files::is_dir(st) {
            self.count(path)?;
            let mut folder = archived.to_vec();
            folder.push(b'/');
            self.header(
                Header {
                    name: &folder,
                    kind: b'5',
                    mode,
                    size: 0,
                    mtime: st.st_mtime,
                    link: b"",
                },
                path,
            )?;
            if st.st_dev != dev {
                // A mount point: stored as an empty folder, like
                // `tar --one-file-system`.
                self.skipped += 1;
                return Ok(());
            }
            let child = match open_dir_at(dir, name) {
                Ok(child) => child,
                Err(error) if is_denied(&error) => {
                    self.skipped += 1;
                    return Ok(());
                }
                Err(error) if is_gone(&error) => return Ok(()),
                Err(error) => return Err(files::io_error(error, path)),
            };
            self.tree(child.as_raw_fd(), &folder, depth + 1, dev, path)
        } else if files::is_symlink(st) {
            let Some(target) = read_link(dir, name) else {
                // Removed meanwhile.
                return Ok(());
            };
            self.count(path)?;
            self.header(
                Header {
                    name: archived,
                    kind: b'2',
                    mode,
                    size: 0,
                    mtime: st.st_mtime,
                    link: &target,
                },
                path,
            )
        } else if files::is_regular(st) {
            self.file(dir, name, archived, path)
        } else {
            self.skipped += 1;
            Ok(())
        }
    }
    fn tree(
        &mut self,
        dir: RawFd,
        prefix: &[u8],
        depth: usize,
        dev: libc::dev_t,
        path: &Path,
    ) -> Outcome<()> {
        if depth > files::MAX_DEPTH {
            return Err(too_deep(path));
        }
        let room = MAX_ARCHIVE_ENTRIES as u64 - self.entries;
        for name in sorted_names(dir, room, path)? {
            let child = path.join(OsStr::from_bytes(name.to_bytes()));
            let st = match files::stat_at(dir, &name) {
                Ok(st) => st,
                Err(error) if is_gone(&error) => continue,
                Err(error) if is_denied(&error) => {
                    self.skipped += 1;
                    continue;
                }
                Err(error) => return Err(files::io_error(error, &child)),
            };
            let mut archived = prefix.to_vec();
            archived.extend_from_slice(name.to_bytes());
            self.entry(dir, &name, &st, &archived, depth, dev, &child)?;
        }
        Ok(())
    }
    /// A regular file: its size when opened is the entry's size. A file
    /// that shrinks while read is padded with zero bytes, one that grows is
    /// cut at that size; either counts as `changed`, as does a change of its
    /// modification time.
    fn file(&mut self, dir: RawFd, name: &CStr, archived: &[u8], path: &Path) -> Outcome<()> {
        let fd = match files::open_at(
            dir,
            name,
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_NOCTTY,
            0,
        ) {
            Ok(fd) => fd,
            Err(error) if is_denied(&error) => {
                self.skipped += 1;
                return Ok(());
            }
            Err(error) if is_gone(&error) => return Ok(()),
            // Replaced by a symlink after the listing (ELOOP), or by
            // something that cannot be opened.
            Err(_) => {
                self.skipped += 1;
                return Ok(());
            }
        };
        let st = files::stat_fd(fd.as_raw_fd()).map_err(|e| files::io_error(e, path))?;
        if !files::is_regular(&st) {
            self.skipped += 1;
            return Ok(());
        }
        self.count(path)?;
        let size = st.st_size as u64;
        self.bytes += size;
        if self.bytes > MAX_ARCHIVE_BYTES {
            return Err(too_many_bytes(path));
        }
        self.header(
            Header {
                name: archived,
                kind: b'0',
                mode: st.st_mode & 0o777,
                size,
                mtime: st.st_mtime,
                link: b"",
            },
            path,
        )?;
        let mut file = File::from(fd);
        let mut remaining = size;
        let mut buffer = std::mem::take(&mut self.buffer);
        let result = (|| -> Outcome<()> {
            while remaining > 0 {
                let want = buffer
                    .len()
                    .min(usize::try_from(remaining).unwrap_or(usize::MAX));
                let n = match file.read(&mut buffer[..want]) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(files::io_error(error, path)),
                };
                self.write(&buffer[..n], path)?;
                remaining -= n as u64;
                self.input.fetch_add(n as u64, Ordering::Relaxed);
            }
            Ok(())
        })();
        self.buffer = buffer;
        result?;
        let mut changed = remaining > 0;
        while remaining > 0 {
            let n = remaining.min(self.buffer.len() as u64) as usize;
            let zeros = vec![0u8; n];
            self.write(&zeros, path)?;
            remaining -= n as u64;
        }
        let padding = (BLOCK - (size % BLOCK as u64) as usize) % BLOCK;
        self.write(&[0u8; BLOCK][..padding], path)?;
        if let Ok(now) = files::stat_fd(file.as_raw_fd()) {
            changed |= (now.st_size, now.st_mtime, now.st_mtime_nsec)
                != (st.st_size, st.st_mtime, st.st_mtime_nsec);
        }
        self.changed += u64::from(changed);
        Ok(())
    }
}
/// A symlink's target as bytes, not followed.
fn read_link(dir: RawFd, name: &CStr) -> Option<Vec<u8>> {
    let mut buffer = vec![0u8; files::MAX_PATH_BYTES + 1];
    let length =
        unsafe { libc::readlinkat(dir, name.as_ptr(), buffer.as_mut_ptr().cast(), buffer.len()) };
    if length < 0 {
        return None;
    }
    buffer.truncate(length as usize);
    Some(buffer)
}

/// One tar entry's header.
struct Header<'a> {
    name: &'a [u8],
    /// `0` file, `2` symlink, `5` folder (`x` for a pax header).
    kind: u8,
    mode: u32,
    size: u64,
    mtime: i64,
    link: &'a [u8],
}
impl Header<'_> {
    /// The ustar header block, preceded by a pax extended header (and its
    /// records) when a field does not fit ustar.
    fn encode(&self, out: &mut Vec<u8>) {
        let plain =
            |bytes: &[u8]| bytes.len() <= 100 && bytes.iter().all(|b| (0x20..0x7f).contains(b));
        let mut records = Vec::new();
        if !plain(self.name) {
            pax_record(&mut records, b"path", self.name);
        }
        if !plain(self.link) {
            pax_record(&mut records, b"linkpath", self.link);
        }
        if self.size > OCTAL_MAX {
            pax_record(&mut records, b"size", self.size.to_string().as_bytes());
        }
        let mtime = u64::try_from(self.mtime)
            .ok()
            .filter(|mtime| *mtime <= OCTAL_MAX);
        if mtime.is_none() {
            pax_record(&mut records, b"mtime", self.mtime.to_string().as_bytes());
        }
        if !records.is_empty() {
            let mut name = b"PaxHeader/".to_vec();
            name.extend_from_slice(&fallback(self.name, 100 - name.len()));
            block(
                out,
                &name,
                b'x',
                0o644,
                records.len() as u64,
                mtime.unwrap_or(0),
                b"",
            );
            out.extend_from_slice(&records);
            out.resize(out.len().div_ceil(BLOCK) * BLOCK, 0);
        }
        block(
            out,
            &fallback(self.name, 100),
            self.kind,
            self.mode,
            if self.size > OCTAL_MAX { 0 } else { self.size },
            mtime.unwrap_or(0),
            &fallback(self.link, 100),
        );
    }
}
/// A pax record: `<length> <key>=<value>\n`, the length counting itself.
fn pax_record(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    let rest = 1 + key.len() + 1 + value.len() + 1;
    let mut length = rest + 1;
    while length != rest + length.to_string().len() {
        length = rest + length.to_string().len();
    }
    out.extend_from_slice(length.to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(key);
    out.push(b'=');
    out.extend_from_slice(value);
    out.push(b'\n');
}
/// A ustar field value for a name that needs a pax record: printable ASCII
/// (others as `_`), its last `max` bytes. Readers use the pax record.
fn fallback(name: &[u8], max: usize) -> Vec<u8> {
    let start = name.len().saturating_sub(max);
    name[start..]
        .iter()
        .map(|&b| if (0x20..0x7f).contains(&b) { b } else { b'_' })
        .collect()
}
/// One ustar header block.
fn block(out: &mut Vec<u8>, name: &[u8], kind: u8, mode: u32, size: u64, mtime: u64, link: &[u8]) {
    let mut header = [0u8; BLOCK];
    header[..name.len()].copy_from_slice(name);
    octal(&mut header[100..108], u64::from(mode));
    octal(&mut header[108..116], 0);
    octal(&mut header[116..124], 0);
    octal(&mut header[124..136], size);
    octal(&mut header[136..148], mtime);
    header[156] = kind;
    header[157..157 + link.len()].copy_from_slice(link);
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    // The checksum counts its own field as eight spaces.
    header[148..156].copy_from_slice(b"        ");
    let sum: u32 = header.iter().map(|&b| u32::from(b)).sum();
    header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    out.extend_from_slice(&header);
}
/// A zero-padded octal number filling all but the field's last byte (NUL).
fn octal(field: &mut [u8], value: u64) {
    let digits = field.len() - 1;
    let text = format!("{value:0digits$o}");
    field[..digits].copy_from_slice(&text.as_bytes()[text.len() - digits..]);
    field[digits] = 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pax_records_count_their_own_length() {
        let mut out = Vec::new();
        pax_record(&mut out, b"path", b"a");
        assert_eq!(out, b"9 path=a\n");
        let mut out = Vec::new();
        let value = vec![b'x'; 93];
        pax_record(&mut out, b"path", &value);
        // 1 + 4 + 1 + 93 + 1 = 100 bytes besides the length: 103 in all.
        assert_eq!(out.len(), 103);
        assert!(out.starts_with(b"103 path="));
    }
    #[test]
    fn headers_are_ustar_with_a_valid_checksum() {
        let mut out = Vec::new();
        Header {
            name: b"dir/",
            kind: b'5',
            mode: 0o755,
            size: 0,
            mtime: 1_795_300_000,
            link: b"",
        }
        .encode(&mut out);
        assert_eq!(out.len(), BLOCK);
        assert_eq!(&out[..4], b"dir/");
        assert_eq!(&out[100..108], b"0000755\0");
        assert_eq!(&out[257..263], b"ustar\0");
        assert_eq!(out[156], b'5');
        let stored = u32::from_str_radix(std::str::from_utf8(&out[148..154]).unwrap(), 8).unwrap();
        let mut copy = out.clone();
        copy[148..156].copy_from_slice(b"        ");
        assert_eq!(stored, copy.iter().map(|&b| u32::from(b)).sum::<u32>());
        // A long or non-ASCII name gets a pax header first.
        let mut out = Vec::new();
        Header {
            name: "é".repeat(60).as_bytes(),
            kind: b'0',
            mode: 0o644,
            size: 3,
            mtime: -5,
            link: b"",
        }
        .encode(&mut out);
        assert_eq!(out.len(), 3 * BLOCK);
        assert_eq!(out[156], b'x');
        let records = &out[BLOCK..2 * BLOCK];
        let text = String::from_utf8_lossy(records);
        assert!(text.contains(&format!("path={}\n", "é".repeat(60))));
        assert!(text.contains("mtime=-5\n"));
        assert_eq!(out[2 * BLOCK + 156], b'0');
    }
    #[test]
    fn octal_fields_are_zero_padded() {
        let mut field = [0xffu8; 12];
        octal(&mut field, 5);
        assert_eq!(&field, b"00000000005\0");
        octal(&mut field, OCTAL_MAX);
        assert_eq!(&field, b"77777777777\0");
    }
}

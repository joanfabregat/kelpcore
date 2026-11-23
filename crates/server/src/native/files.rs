//! File operations over the pane stream (capability `files`, ADR 0003
//! section 3): list, stat, read, write, create a folder, rename, move to the
//! freedesktop trash and delete, on the host where the server and its shells
//! run. They serve the file browsers and editors of stream clients.
//!
//! These operations grant nothing a stream connection does not already have:
//! any connection may type into a shell running as the server's user. They
//! run with the server's credentials and never elevate. What they add is
//! care against surprises, not a confinement:
//!
//! - Paths are absolute (or `~`-relative to the server user's `HOME`),
//!   lexically normalised, and may not contain `..`.
//! - Every change (write, mkdir, rename, trash, delete) acts on the final
//!   component through its parent's directory descriptor and never follows a
//!   symlink there: writing to a symlink is refused, deleting or trashing one
//!   removes the link, never its target, and recursive deletion opens every
//!   directory with `O_NOFOLLOW`. Reads, stats and listings follow symlinks
//!   in intermediate components like a shell does; `file_stat` and listing
//!   entries describe a symlink itself (`lstat`) and report its target.
//! - Writes are atomic replacements: a temporary file in the same directory,
//!   synced, then renamed over the target, keeping the target's permission
//!   bits. A `base_hash` turns the write into a compare-and-swap on the
//!   content's SHA-256.
//! - Sizes, entry counts, paths per request and concurrent operations are
//!   bounded ([`LIMITS`], advertised in Welcome `file_limits`).
//!
//! Operations run on worker threads, at most [`MAX_IN_FLIGHT`] per
//! connection and [`MAX_SERVER_IN_FLIGHT`] per server, so a slow disk never
//! stalls pane output.
//!
//! The descriptor-relative design, the text/binary classification and the
//! atomic replacement are adapted from KelpHub's workspace file browser
//! (`server/src/management/files.rs`), moved here with the owner's approval
//! (decision 185b).
use super::stream::{Event, Request};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::{
    ffi::{CStr, CString, OsStr},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Component, Path, PathBuf},
};

/// Largest file `file_read` returns. Its base64 form (about 10.7 MiB) fits
/// one 16 MiB record.
pub const MAX_READ_BYTES: u64 = 8 * 1024 * 1024;
/// Largest content `file_write` accepts, after decoding.
pub const MAX_WRITE_BYTES: u64 = 8 * 1024 * 1024;
/// Most entries one `file_list` returns (sorted, then truncated).
pub const MAX_LIST_ENTRIES: usize = 5000;
/// Most directory entries one listing examines.
const MAX_LIST_SCAN: usize = 200_000;
/// Most paths in one `file_trash` or `file_delete`.
pub const MAX_PATHS: usize = 256;
/// Concurrent file operations per connection.
pub const MAX_IN_FLIGHT: usize = 4;
/// Concurrent file operations per server.
pub const MAX_SERVER_IN_FLIGHT: usize = 16;
/// Longest accepted path, in bytes.
pub const MAX_PATH_BYTES: usize = 4096;
/// Deepest directory nesting `file_delete` removes.
pub const MAX_DEPTH: usize = 64;
/// Longest symlink target reported.
const MAX_TARGET_BYTES: usize = 4096;
/// Largest serialized reply, below the 16 MiB record with room to spare.
const REPLY_BUDGET: usize = 15 * 1024 * 1024;
/// Largest JSON-escaped text content; beyond it content is sent as base64.
const TEXT_BUDGET: usize = 12 * 1024 * 1024;
/// Longest file name on Linux file systems.
const NAME_MAX: usize = 255;
const TRASHINFO: &str = ".trashinfo";

/// File bounds advertised in Welcome `file_limits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_read_bytes: u64,
    pub max_write_bytes: u64,
    pub max_list_entries: usize,
    pub max_paths: usize,
    pub max_in_flight: usize,
    pub max_path_bytes: usize,
    pub max_depth: usize,
}
pub const LIMITS: Limits = Limits {
    max_read_bytes: MAX_READ_BYTES,
    max_write_bytes: MAX_WRITE_BYTES,
    max_list_entries: MAX_LIST_ENTRIES,
    max_paths: MAX_PATHS,
    max_in_flight: MAX_IN_FLIGHT,
    max_path_bytes: MAX_PATH_BYTES,
    max_depth: MAX_DEPTH,
};

/// Server configuration of file operations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settings {
    /// The trash directory (holding `files/` and `info/`). `None` is the
    /// freedesktop home trash, `$XDG_DATA_HOME/Trash` or
    /// `~/.local/share/Trash`.
    pub trash_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    File,
    Dir,
    Symlink,
    /// A FIFO, socket or device.
    Other,
}
/// One file system entry, as `lstat` sees it (a symlink is never followed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub name: String,
    pub kind: Kind,
    /// Bytes (the link length for a symlink).
    pub size: u64,
    /// Last modification, Unix seconds.
    pub modified: i64,
    /// Permission bits, including set-id and sticky bits (`st_mode & 0o7777`).
    /// The entry is executable when `mode & 0o111` is not zero.
    pub mode: u32,
    /// A symlink's target as text, not followed; invalid UTF-8 is replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}
/// How `content` is carried in `file_read` replies and `file_write` requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    /// The content is the text itself.
    #[default]
    Utf8,
    /// Standard padded base64 of the bytes.
    Base64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    /// Not absolute, not normalisable, too long, or with no final name.
    InvalidPath,
    /// A request field combination or count that is not accepted.
    InvalidRequest,
    /// `file_write` content that is not valid base64.
    InvalidContent,
    NotFound,
    PermissionDenied,
    NotADirectory,
    IsADirectory,
    /// Not a regular file (a FIFO, socket or device).
    NotAFile,
    /// The final component is a symlink, which this operation never follows.
    Symlink,
    Exists,
    NotEmpty,
    /// `base_hash` differs from the current content (`current_hash`).
    Conflict,
    /// Over a size, entry or depth bound (`size` carries a file's size).
    TooLarge,
    /// Different file systems: rename cannot move it, or the trash is
    /// elsewhere.
    CrossDevice,
    ReadOnly,
    NoSpace,
    /// The connection or server already runs the most file operations.
    Busy,
    /// Any other I/O error; `message` has the system's description.
    Io,
}
/// A structured failure of a file operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub code: Code,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// `conflict`: the hash of the content now on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_hash: Option<String>,
    /// `too_large`: the file's size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}
impl Error {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            path: None,
            current_hash: None,
            size: None,
        }
    }
    fn at(mut self, path: &Path) -> Self {
        self.path = Some(path.to_string_lossy().into_owned());
        self
    }
}
type Outcome<T> = std::result::Result<T, Error>;
/// A system error, classified.
fn io_error(error: io::Error, path: &Path) -> Error {
    let code = match error.raw_os_error() {
        Some(libc::ENOENT) => Code::NotFound,
        Some(libc::EACCES | libc::EPERM) => Code::PermissionDenied,
        Some(libc::ENOTDIR) => Code::NotADirectory,
        Some(libc::EISDIR) => Code::IsADirectory,
        // Opening a socket, or a device without a driver.
        Some(libc::ENXIO | libc::ENODEV) => Code::NotAFile,
        Some(libc::ELOOP) => Code::Symlink,
        Some(libc::EEXIST) => Code::Exists,
        Some(libc::ENOTEMPTY) => Code::NotEmpty,
        Some(libc::EXDEV) => Code::CrossDevice,
        Some(libc::EROFS) => Code::ReadOnly,
        Some(libc::ENOSPC | libc::EDQUOT) => Code::NoSpace,
        Some(libc::EFBIG) => Code::TooLarge,
        Some(libc::ENAMETOOLONG | libc::EINVAL) => Code::InvalidPath,
        _ => Code::Io,
    };
    Error::new(code, error.to_string()).at(path)
}
/// A removed path of `file_trash` or `file_delete`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Removed {
    pub path: String,
    /// `file_trash`: where the entry now is (`<trash>/files/<name>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trashed_as: Option<String>,
}
/// A path of `file_trash` or `file_delete` that was not removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub path: String,
    pub error: Error,
}

/// The server user's home directory, from `HOME`.
pub fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}
/// Lexically normalise a request path: `~` and `~/…` start at `home`;
/// otherwise it must be absolute. Empty and `.` components are dropped; `..`,
/// `~user`, NUL and paths over `MAX_PATH_BYTES` are refused.
pub fn normalize(path: &str, home: Option<&Path>) -> Outcome<PathBuf> {
    let invalid = |message: &str| Error::new(Code::InvalidPath, message);
    if path.is_empty() || path.len() > MAX_PATH_BYTES || path.contains('\0') {
        return Err(invalid("path must be 1 to 4096 bytes without NUL"));
    }
    let (mut result, rest) = if path == "~" || path.starts_with("~/") {
        let home = home.ok_or_else(|| invalid("the server user has no absolute HOME"))?;
        (normalize_home(home)?, &path[1..])
    } else if path.starts_with('~') {
        return Err(invalid("only ~ and ~/ paths name a home directory"));
    } else if path.starts_with('/') {
        (PathBuf::from("/"), path)
    } else {
        return Err(invalid("path must be absolute or start with ~/"));
    };
    for part in rest.split('/') {
        match part {
            "" | "." => {}
            ".." => return Err(invalid("path must not contain ..")),
            name => result.push(name),
        }
    }
    if result.as_os_str().len() > MAX_PATH_BYTES {
        return Err(invalid("path is too long"));
    }
    Ok(result)
}
fn normalize_home(home: &Path) -> Outcome<PathBuf> {
    let mut result = PathBuf::from("/");
    for component in home.components() {
        match component {
            Component::Normal(name) => result.push(name),
            Component::RootDir | Component::CurDir => {}
            _ => {
                return Err(Error::new(
                    Code::InvalidPath,
                    "the server user's HOME is not a normalised path",
                ));
            }
        }
    }
    Ok(result)
}
fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}
fn cstring(bytes: &[u8]) -> io::Result<CString> {
    CString::new(bytes).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
}
fn open_at(dir: RawFd, name: &CStr, flags: libc::c_int, mode: libc::c_uint) -> io::Result<OwnedFd> {
    let fd = check(unsafe { libc::openat(dir, name.as_ptr(), flags | libc::O_CLOEXEC, mode) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}
const DIRECTORY: libc::c_int = libc::O_RDONLY | libc::O_DIRECTORY;
fn stat_at(dir: RawFd, name: &CStr) -> io::Result<libc::stat> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    check(unsafe { libc::fstatat(dir, name.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) })?;
    Ok(st)
}
fn stat_fd(fd: RawFd) -> io::Result<libc::stat> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    check(unsafe { libc::fstat(fd, &mut st) })?;
    Ok(st)
}
fn is_dir(st: &libc::stat) -> bool {
    st.st_mode & libc::S_IFMT == libc::S_IFDIR
}
fn is_regular(st: &libc::stat) -> bool {
    st.st_mode & libc::S_IFMT == libc::S_IFREG
}
fn read_link_at(dir: RawFd, name: &CStr) -> Option<String> {
    let mut buffer = vec![0u8; MAX_TARGET_BYTES];
    let length =
        unsafe { libc::readlinkat(dir, name.as_ptr(), buffer.as_mut_ptr().cast(), buffer.len()) };
    if length < 0 {
        return None;
    }
    buffer.truncate(length as usize);
    Some(String::from_utf8_lossy(&buffer).into_owned())
}
fn entry(name: String, st: &libc::stat, dir: RawFd, cname: &CStr) -> Entry {
    let kind = match st.st_mode & libc::S_IFMT {
        libc::S_IFREG => Kind::File,
        libc::S_IFDIR => Kind::Dir,
        libc::S_IFLNK => Kind::Symlink,
        _ => Kind::Other,
    };
    Entry {
        name,
        kind,
        size: st.st_size as u64,
        modified: st.st_mtime,
        mode: st.st_mode & 0o7777,
        target: (kind == Kind::Symlink)
            .then(|| read_link_at(dir, cname))
            .flatten(),
    }
}
/// A directory's entry names (without `.` and `..`), at most `limit` of
/// them; `true` when the directory had more.
fn names(dir: RawFd, limit: usize) -> io::Result<(Vec<CString>, bool)> {
    let copy = check(unsafe { libc::fcntl(dir, libc::F_DUPFD_CLOEXEC, 0) })?;
    let stream = unsafe { libc::fdopendir(copy) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        unsafe { libc::close(copy) };
        return Err(error);
    }
    unsafe { libc::rewinddir(stream) };
    let mut names = Vec::new();
    let mut more = false;
    loop {
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        if names.len() == limit {
            more = true;
            break;
        }
        names.push(name.to_owned());
    }
    unsafe { libc::closedir(stream) };
    Ok((names, more))
}
fn random_hex() -> io::Result<String> {
    let mut bytes = [0u8; 8];
    if unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) } != bytes.len() as isize
    {
        return Err(io::Error::last_os_error());
    }
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// The parent directory of a normalised path, opened (following symlinks in
/// the parent's own path, like a shell), and the final name.
struct Location {
    dir: OwnedFd,
    name: CString,
}
fn locate(path: &Path) -> Outcome<Location> {
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(Error::new(Code::InvalidPath, "the root directory has no name").at(path));
    };
    let parent_c = cstring(parent.as_os_str().as_bytes()).map_err(|e| io_error(e, path))?;
    let dir = open_at(libc::AT_FDCWD, &parent_c, DIRECTORY, 0).map_err(|e| io_error(e, path))?;
    Ok(Location {
        dir,
        name: cstring(name.as_bytes()).map_err(|e| io_error(e, path))?,
    })
}
/// Rename without replacing an existing target. A file system without
/// `RENAME_NOREPLACE` gets a check, then a plain rename.
fn rename_noreplace(from_dir: RawFd, from: &CStr, to_dir: RawFd, to: &CStr) -> io::Result<()> {
    let renamed = unsafe {
        libc::renameat2(
            from_dir,
            from.as_ptr(),
            to_dir,
            to.as_ptr(),
            libc::RENAME_NOREPLACE as _,
        )
    };
    if renamed == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOSYS)) {
        return Err(error);
    }
    match stat_at(to_dir, to) {
        Ok(_) => return Err(io::Error::from_raw_os_error(libc::EEXIST)),
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {}
        Err(error) => return Err(error),
    }
    check(unsafe { libc::renameat(from_dir, from.as_ptr(), to_dir, to.as_ptr()) })?;
    Ok(())
}
fn sync_dir(dir: RawFd) {
    // Durability of the directory entry; the operation already happened.
    unsafe { libc::fsync(dir) };
}

/// The bytes after classification: `binary` when they hold NUL or are not
/// UTF-8. Text goes as UTF-8 unless its JSON escaping would exceed the reply
/// budget (many control characters); binary always goes as base64.
fn content(bytes: Vec<u8>) -> (bool, Encoding, String) {
    if bytes.contains(&0) {
        return (true, Encoding::Base64, STANDARD.encode(&bytes));
    }
    match String::from_utf8(bytes) {
        Ok(text) if escaped_len(&text) <= TEXT_BUDGET => (false, Encoding::Utf8, text),
        Ok(text) => (false, Encoding::Base64, STANDARD.encode(text.as_bytes())),
        Err(error) => (true, Encoding::Base64, STANDARD.encode(error.as_bytes())),
    }
}
/// The length of `text` as a JSON string body.
fn escaped_len(text: &str) -> usize {
    text.chars()
        .map(|c| match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            c if (c as u32) < 0x20 => 6,
            c => c.len_utf8(),
        })
        .sum()
}
/// Stream a file through SHA-256.
fn hash_file(file: &mut File) -> io::Result<String> {
    let mut hasher = super::sha256::Sha256::default();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => hasher.update(&buffer[..n]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(hasher.hex())
}

fn list(path: &Path) -> Outcome<(Vec<Entry>, bool, usize)> {
    let cpath = cstring(path.as_os_str().as_bytes()).map_err(|e| io_error(e, path))?;
    let dir = open_at(libc::AT_FDCWD, &cpath, DIRECTORY, 0).map_err(|e| io_error(e, path))?;
    let (found, mut truncated) =
        names(dir.as_raw_fd(), MAX_LIST_SCAN).map_err(|e| io_error(e, path))?;
    let mut entries = Vec::new();
    let mut skipped = 0;
    for name in found {
        // A name that is not UTF-8 cannot be addressed by a JSON path.
        let Ok(text) = name.to_str().map(str::to_owned) else {
            skipped += 1;
            continue;
        };
        // An entry removed while listing is simply absent.
        let Ok(st) = stat_at(dir.as_raw_fd(), &name) else {
            continue;
        };
        entries.push(entry(text, &st, dir.as_raw_fd(), &name));
    }
    // Directories first, then everything else, each in byte order of the name.
    entries.sort_by(|a, b| {
        (a.kind != Kind::Dir)
            .cmp(&(b.kind != Kind::Dir))
            .then_with(|| a.name.cmp(&b.name))
    });
    if entries.len() > MAX_LIST_ENTRIES {
        entries.truncate(MAX_LIST_ENTRIES);
        truncated = true;
    }
    // Long names and targets with many escapes still fit one record.
    let mut budget = 0usize;
    for (index, item) in entries.iter().enumerate() {
        budget += serde_json::to_vec(item).map_or(usize::MAX / 2, |bytes| bytes.len() + 1);
        if budget > REPLY_BUDGET - 64 * 1024 {
            entries.truncate(index);
            truncated = true;
            break;
        }
    }
    Ok((entries, truncated, skipped))
}

fn stat(path: &Path) -> Outcome<Entry> {
    let cpath = cstring(path.as_os_str().as_bytes()).map_err(|e| io_error(e, path))?;
    let st = stat_at(libc::AT_FDCWD, &cpath).map_err(|e| io_error(e, path))?;
    let name = path.file_name().map_or_else(
        || "/".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    );
    Ok(entry(name, &st, libc::AT_FDCWD, &cpath))
}

struct Content {
    size: u64,
    modified: i64,
    mode: u32,
    hash: String,
    binary: bool,
    encoding: Encoding,
    content: String,
}
fn read(path: &Path, max_bytes: Option<u64>) -> Outcome<Content> {
    let limit = max_bytes.unwrap_or(MAX_READ_BYTES).min(MAX_READ_BYTES);
    let cpath = cstring(path.as_os_str().as_bytes()).map_err(|e| io_error(e, path))?;
    // O_NONBLOCK keeps a FIFO from blocking before it is refused below.
    let fd = open_at(
        libc::AT_FDCWD,
        &cpath,
        libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOCTTY,
        0,
    )
    .map_err(|e| io_error(e, path))?;
    let st = stat_fd(fd.as_raw_fd()).map_err(|e| io_error(e, path))?;
    if is_dir(&st) {
        return Err(Error::new(Code::IsADirectory, "a directory has no content").at(path));
    }
    if !is_regular(&st) {
        return Err(Error::new(Code::NotAFile, "not a regular file").at(path));
    }
    let too_large = |size: u64| {
        let mut error = Error::new(
            Code::TooLarge,
            format!("the file is larger than {limit} bytes"),
        )
        .at(path);
        error.size = Some(size);
        error
    };
    if st.st_size as u64 > limit {
        return Err(too_large(st.st_size as u64));
    }
    let mut bytes = Vec::with_capacity(st.st_size as usize);
    File::from(fd)
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| io_error(e, path))?;
    if bytes.len() as u64 > limit {
        // It grew while being read.
        return Err(too_large(bytes.len() as u64));
    }
    let hash = super::sha256::hex(&bytes);
    let size = bytes.len() as u64;
    let (binary, encoding, content) = content(bytes);
    Ok(Content {
        size,
        modified: st.st_mtime,
        mode: st.st_mode & 0o7777,
        hash,
        binary,
        encoding,
        content,
    })
}

struct Written {
    hash: String,
    size: u64,
    modified: i64,
}
fn decode(content: String, encoding: Encoding) -> Outcome<Vec<u8>> {
    let too_large = || {
        Error::new(
            Code::TooLarge,
            format!("content is limited to {MAX_WRITE_BYTES} bytes"),
        )
    };
    let bytes = match encoding {
        Encoding::Utf8 => content.into_bytes(),
        Encoding::Base64 => {
            if content.len() as u64 > MAX_WRITE_BYTES.div_ceil(3) * 4 {
                return Err(too_large());
            }
            STANDARD
                .decode(content.as_bytes())
                .map_err(|_| Error::new(Code::InvalidContent, "content is not valid base64"))?
        }
    };
    if bytes.len() as u64 > MAX_WRITE_BYTES {
        return Err(too_large());
    }
    Ok(bytes)
}
fn write(path: &Path, bytes: &[u8], base_hash: Option<&str>, create: bool) -> Outcome<Written> {
    if create && base_hash.is_some() {
        return Err(Error::new(
            Code::InvalidRequest,
            "base_hash names existing content; it cannot be combined with create",
        ));
    }
    let fail = |e: io::Error| io_error(e, path);
    let location = locate(path)?;
    let dir = location.dir.as_raw_fd();
    let name = &location.name;
    // The file being replaced: never through a symlink, only a regular file
    // of this user (a replacement would change its owner).
    let existing = if create {
        None
    } else {
        let fd = open_at(
            dir,
            name,
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_NOCTTY,
            0,
        )
        .map_err(|e| {
            if e.raw_os_error() == Some(libc::ELOOP) {
                Error::new(Code::Symlink, "refusing to write through a symlink").at(path)
            } else {
                fail(e)
            }
        })?;
        let st = stat_fd(fd.as_raw_fd()).map_err(fail)?;
        if is_dir(&st) {
            return Err(Error::new(Code::IsADirectory, "cannot write a directory").at(path));
        }
        if !is_regular(&st) {
            return Err(Error::new(Code::NotAFile, "not a regular file").at(path));
        }
        if st.st_uid != unsafe { libc::geteuid() } {
            return Err(
                Error::new(Code::PermissionDenied, "the file belongs to another user").at(path),
            );
        }
        if let Some(base) = base_hash {
            let current = hash_file(&mut File::from(fd)).map_err(fail)?;
            if !current.eq_ignore_ascii_case(base) {
                let mut error =
                    Error::new(Code::Conflict, "the file changed since it was read").at(path);
                error.current_hash = Some(current);
                return Err(error);
            }
        }
        Some(st)
    };
    let temporary = cstring(format!(".kelpserver-{}.tmp", random_hex().map_err(fail)?).as_bytes())
        .map_err(fail)?;
    // A new file gets 0666 less the umask, like a shell's; a replacement
    // starts private and takes the existing permission bits (without set-id).
    let fd = open_at(
        dir,
        &temporary,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
        if create { 0o666 } else { 0o600 },
    )
    .map_err(fail)?;
    let result = (|| -> Outcome<Written> {
        let mut file = File::from(fd);
        file.write_all(bytes).map_err(fail)?;
        if let Some(st) = &existing {
            check(unsafe { libc::fchmod(file.as_raw_fd(), st.st_mode & 0o777) }).map_err(fail)?;
        }
        file.sync_all().map_err(fail)?;
        let written = stat_fd(file.as_raw_fd()).map_err(fail)?;
        match &existing {
            None => rename_noreplace(dir, &temporary, dir, name).map_err(fail)?,
            Some(st) => {
                // Refuse if the name now points elsewhere (replaced meanwhile).
                let now = stat_at(dir, name).map_err(fail)?;
                if now.st_dev != st.st_dev || now.st_ino != st.st_ino {
                    return Err(Error::new(
                        Code::Conflict,
                        "the file was replaced while it was being saved",
                    )
                    .at(path));
                }
                check(unsafe { libc::renameat(dir, temporary.as_ptr(), dir, name.as_ptr()) })
                    .map_err(fail)?;
            }
        }
        Ok(Written {
            hash: super::sha256::hex(bytes),
            size: bytes.len() as u64,
            modified: written.st_mtime,
        })
    })();
    if result.is_err() {
        unsafe { libc::unlinkat(dir, temporary.as_ptr(), 0) };
    } else {
        sync_dir(dir);
    }
    result
}

fn mkdir(path: &Path) -> Outcome<()> {
    let location = locate(path)?;
    check(unsafe { libc::mkdirat(location.dir.as_raw_fd(), location.name.as_ptr(), 0o777) })
        .map_err(|e| io_error(e, path))?;
    sync_dir(location.dir.as_raw_fd());
    Ok(())
}

fn rename(from: &Path, to: &Path, overwrite: bool) -> Outcome<()> {
    let source = locate(from)?;
    let target = locate(to)?;
    // The source must exist (a symlink is moved as a link).
    stat_at(source.dir.as_raw_fd(), &source.name).map_err(|e| io_error(e, from))?;
    let result = if overwrite {
        check(unsafe {
            libc::renameat(
                source.dir.as_raw_fd(),
                source.name.as_ptr(),
                target.dir.as_raw_fd(),
                target.name.as_ptr(),
            )
        })
        .map(drop)
    } else {
        rename_noreplace(
            source.dir.as_raw_fd(),
            &source.name,
            target.dir.as_raw_fd(),
            &target.name,
        )
    };
    result.map_err(|error| {
        let path = if error.raw_os_error() == Some(libc::EEXIST) {
            to
        } else {
            from
        };
        io_error(error, path)
    })?;
    sync_dir(target.dir.as_raw_fd());
    sync_dir(source.dir.as_raw_fd());
    Ok(())
}

/// Remove a directory's contents without following symlinks, in bounded
/// batches of names.
fn clear(dir: RawFd, depth: usize, path: &Path) -> Outcome<()> {
    if depth >= MAX_DEPTH {
        return Err(Error::new(
            Code::TooLarge,
            format!("directories are nested deeper than {MAX_DEPTH} levels"),
        )
        .at(path));
    }
    loop {
        let (batch, _) = names(dir, 4096).map_err(|e| io_error(e, path))?;
        if batch.is_empty() {
            return Ok(());
        }
        for name in batch {
            let child = path.join(OsStr::from_bytes(name.to_bytes()));
            let st = match stat_at(dir, &name) {
                Ok(st) => st,
                Err(error) if error.raw_os_error() == Some(libc::ENOENT) => continue,
                Err(error) => return Err(io_error(error, &child)),
            };
            remove_entry(dir, &name, &st, depth, &child)?;
        }
    }
}
fn remove_entry(
    dir: RawFd,
    name: &CStr,
    st: &libc::stat,
    depth: usize,
    path: &Path,
) -> Outcome<()> {
    let removed = if is_dir(st) {
        let child =
            open_at(dir, name, DIRECTORY | libc::O_NOFOLLOW, 0).map_err(|e| io_error(e, path))?;
        clear(child.as_raw_fd(), depth + 1, path)?;
        drop(child);
        unsafe { libc::unlinkat(dir, name.as_ptr(), libc::AT_REMOVEDIR) }
    } else {
        unsafe { libc::unlinkat(dir, name.as_ptr(), 0) }
    };
    match check(removed) {
        Ok(_) => Ok(()),
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(()),
        Err(error) => Err(io_error(error, path)),
    }
}
fn delete(path: &Path) -> Outcome<()> {
    let location = locate(path)?;
    let dir = location.dir.as_raw_fd();
    let st = stat_at(dir, &location.name).map_err(|e| io_error(e, path))?;
    remove_entry(dir, &location.name, &st, 0, path)?;
    sync_dir(dir);
    Ok(())
}

/// The freedesktop home trash: `$XDG_DATA_HOME/Trash`, or
/// `~/.local/share/Trash` when `XDG_DATA_HOME` is unset or not absolute.
pub fn default_trash() -> Option<PathBuf> {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| home().map(|home| home.join(".local/share")))
        .map(|data| data.join("Trash"))
}
/// Percent-encode an absolute path for a `.trashinfo` `Path=` key, as in a
/// file URL: every byte except unreserved characters and `/`.
pub fn encode_trash_path(path: &[u8]) -> String {
    let mut encoded = String::with_capacity(path.len());
    for &byte in path {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}
/// The local time as `YYYY-MM-DDThh:mm:ss`, the `DeletionDate` format.
fn deletion_date(time: libc::time_t) -> String {
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    if unsafe { libc::localtime_r(&time, &mut tm) }.is_null() {
        // Without a local time zone, UTC is the most useful fallback.
        unsafe { libc::gmtime_r(&time, &mut tm) };
    }
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    )
}
/// The `.trashinfo` contents for an absolute path.
pub fn trash_info(path: &Path, deletion_date: &str) -> String {
    format!(
        "[Trash Info]\nPath={}\nDeletionDate={deletion_date}\n",
        encode_trash_path(path.as_os_str().as_bytes())
    )
}
/// The trash name of the `n`th candidate (1 is the name itself; then
/// `stem.2.ext`, `stem.3.ext`, …), short enough for its `.trashinfo` name.
pub fn trash_name(name: &str, n: usize) -> String {
    let max = NAME_MAX - TRASHINFO.len();
    if n == 1 && name.len() <= max {
        return name.to_owned();
    }
    let (stem, extension) = match name.rfind('.') {
        Some(index) if index > 0 && name.len() - index <= 32 => name.split_at(index),
        _ => (name, ""),
    };
    let suffix = format!(".{n}{extension}");
    let mut keep = stem.len().min(max.saturating_sub(suffix.len()));
    while !stem.is_char_boundary(keep) {
        keep -= 1;
    }
    format!("{}{suffix}", &stem[..keep])
}
struct Trash {
    root: PathBuf,
    files: OwnedFd,
    info: OwnedFd,
    dev: libc::dev_t,
}
fn private_dir_at(parent: RawFd, name: &CStr, path: &Path) -> Outcome<OwnedFd> {
    if unsafe { libc::mkdirat(parent, name.as_ptr(), 0o700) } != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EEXIST) {
            return Err(io_error(error, path));
        }
    }
    let fd =
        open_at(parent, name, DIRECTORY | libc::O_NOFOLLOW, 0).map_err(|e| io_error(e, path))?;
    let st = stat_fd(fd.as_raw_fd()).map_err(|e| io_error(e, path))?;
    if st.st_uid != unsafe { libc::geteuid() } {
        return Err(Error::new(
            Code::PermissionDenied,
            "the trash directory belongs to another user",
        )
        .at(path));
    }
    Ok(fd)
}
fn open_trash(settings: &Settings) -> Outcome<Trash> {
    let root = settings
        .trash_dir
        .clone()
        .or_else(default_trash)
        .ok_or_else(|| {
            Error::new(
                Code::InvalidPath,
                "neither XDG_DATA_HOME nor HOME is an absolute path",
            )
        })?;
    let parent = root.parent().unwrap_or(Path::new("/"));
    std::fs::create_dir_all(parent).map_err(|e| io_error(e, parent))?;
    let parent_c = cstring(parent.as_os_str().as_bytes()).map_err(|e| io_error(e, parent))?;
    let parent_fd =
        open_at(libc::AT_FDCWD, &parent_c, DIRECTORY, 0).map_err(|e| io_error(e, parent))?;
    let name =
        cstring(root.file_name().unwrap_or_default().as_bytes()).map_err(|e| io_error(e, &root))?;
    let trash = private_dir_at(parent_fd.as_raw_fd(), &name, &root)?;
    let files = private_dir_at(trash.as_raw_fd(), c"files", &root.join("files"))?;
    let info = private_dir_at(trash.as_raw_fd(), c"info", &root.join("info"))?;
    let dev = stat_fd(files.as_raw_fd())
        .map_err(|e| io_error(e, &root))?
        .st_dev;
    Ok(Trash {
        root,
        files,
        info,
        dev,
    })
}
/// Move one entry to the trash: reserve a unique `info/<name>.trashinfo`
/// with `O_EXCL`, write and sync it, then rename the entry to
/// `files/<name>` without replacing anything. A failed move removes the
/// info file again.
fn trash_one(trash: &Trash, path: &Path) -> Outcome<PathBuf> {
    let location = locate(path)?;
    let dir = location.dir.as_raw_fd();
    stat_at(dir, &location.name).map_err(|e| io_error(e, path))?;
    let st = stat_fd(dir).map_err(|e| io_error(e, path))?;
    if st.st_dev != trash.dev {
        return Err(Error::new(
            Code::CrossDevice,
            "the trash is on another file system; delete the entry instead",
        )
        .at(path));
    }
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let info = trash_info(path, &deletion_date(crate::now() as libc::time_t));
    for n in 1..=1000 {
        let candidate = trash_name(&name, n);
        let info_name =
            cstring(format!("{candidate}{TRASHINFO}").as_bytes()).map_err(|e| io_error(e, path))?;
        let file = match open_at(
            trash.info.as_raw_fd(),
            &info_name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
            0o600,
        ) {
            Ok(fd) => File::from(fd),
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
            Err(error) => return Err(io_error(error, &trash.root.join("info"))),
        };
        let unreserve = || unsafe {
            libc::unlinkat(trash.info.as_raw_fd(), info_name.as_ptr(), 0);
        };
        let mut file = file;
        if let Err(error) = file
            .write_all(info.as_bytes())
            .and_then(|()| file.sync_all())
        {
            unreserve();
            return Err(io_error(error, &trash.root.join("info")));
        }
        drop(file);
        let target = cstring(candidate.as_bytes()).map_err(|e| io_error(e, path))?;
        match rename_noreplace(dir, &location.name, trash.files.as_raw_fd(), &target) {
            Ok(()) => {
                sync_dir(dir);
                return Ok(trash.root.join("files").join(candidate));
            }
            // An orphaned payload holds the name; try the next one.
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => unreserve(),
            Err(error) => {
                unreserve();
                return Err(io_error(error, path));
            }
        }
    }
    Err(Error::new(Code::Exists, "no free name in the trash for this entry").at(path))
}

/// The `file_error` reply for a failed request.
pub fn failure(request: String, error: Error) -> Event {
    Event::FileError { request, error }
}
fn removal(
    request: String,
    paths: &[String],
    home: Option<&Path>,
    mut remove: impl FnMut(&Path) -> Outcome<Option<PathBuf>>,
) -> Event {
    if paths.is_empty() || paths.len() > MAX_PATHS {
        return failure(
            request,
            Error::new(Code::InvalidRequest, format!("name 1 to {MAX_PATHS} paths")),
        );
    }
    let mut normalized = Vec::with_capacity(paths.len());
    for path in paths {
        match normalize(path, home) {
            Ok(path) => normalized.push(path),
            Err(mut error) => {
                error.path = Some(path.clone());
                return failure(request, error);
            }
        }
    }
    let mut removed = Vec::new();
    let mut failed = Vec::new();
    for path in normalized {
        match remove(&path) {
            Ok(trashed) => removed.push(Removed {
                path: path_text(&path),
                trashed_as: trashed.as_deref().map(path_text),
            }),
            Err(error) => failed.push(Failure {
                path: path_text(&path),
                error,
            }),
        }
    }
    Event::FileRemoved {
        request,
        removed,
        failed,
    }
}
/// Run one file request (blocking; called on a worker thread). Every outcome
/// is a reply event for the requester whose serialized form fits one record.
pub fn execute(request: Request, settings: &Settings) -> Event {
    let home = home();
    let home = home.as_deref();
    let correlation = request.correlation().unwrap_or_default().to_owned();
    let event = match run(request, settings, home) {
        Ok(event) => event,
        Err(error) => failure(correlation.clone(), error),
    };
    let size = serde_json::to_vec(&event).map_or(usize::MAX, |bytes| bytes.len());
    if size > REPLY_BUDGET {
        return failure(
            correlation,
            Error::new(Code::TooLarge, "the reply would exceed the record size"),
        );
    }
    event
}
fn run(request: Request, settings: &Settings, home: Option<&Path>) -> Outcome<Event> {
    fn at(path: &str) -> impl FnOnce(Error) -> Error + '_ {
        move |mut error| {
            error.path = Some(path.to_owned());
            error
        }
    }
    Ok(match request {
        Request::FileList { request, path } => {
            let path = normalize(&path, home).map_err(at(&path))?;
            let (entries, truncated, skipped) = list(&path)?;
            Event::FileList {
                request,
                path: path_text(&path),
                entries,
                truncated,
                skipped,
            }
        }
        Request::FileStat { request, path } => {
            let path = normalize(&path, home).map_err(at(&path))?;
            let entry = stat(&path)?;
            Event::FileStat {
                request,
                path: path_text(&path),
                entry,
            }
        }
        Request::FileRead {
            request,
            path,
            max_bytes,
        } => {
            let path = normalize(&path, home).map_err(at(&path))?;
            let content = read(&path, max_bytes)?;
            Event::FileContent {
                request,
                path: path_text(&path),
                size: content.size,
                modified: content.modified,
                mode: content.mode,
                hash: content.hash,
                binary: content.binary,
                encoding: content.encoding,
                content: content.content,
            }
        }
        Request::FileWrite {
            request,
            path,
            content,
            encoding,
            base_hash,
            create,
        } => {
            let path = normalize(&path, home).map_err(at(&path))?;
            let bytes = decode(content, encoding).map_err(|e| e.at(&path))?;
            let written = write(&path, &bytes, base_hash.as_deref(), create)?;
            Event::FileWritten {
                request,
                path: path_text(&path),
                hash: written.hash,
                size: written.size,
                modified: written.modified,
                created: create,
            }
        }
        Request::FileMkdir { request, path } => {
            let path = normalize(&path, home).map_err(at(&path))?;
            mkdir(&path)?;
            Event::Done { request }
        }
        Request::FileRename {
            request,
            from,
            to,
            overwrite,
        } => {
            let from = normalize(&from, home).map_err(at(&from))?;
            let to = normalize(&to, home).map_err(at(&to))?;
            rename(&from, &to, overwrite)?;
            Event::Done { request }
        }
        Request::FileTrash { request, paths } => {
            let trash = match open_trash(settings) {
                Ok(trash) => trash,
                Err(error) => return Ok(failure(request, error)),
            };
            removal(request, &paths, home, |path| {
                trash_one(&trash, path).map(Some)
            })
        }
        Request::FileDelete { request, paths } => {
            removal(request, &paths, home, |path| delete(path).map(|()| None))
        }
        _ => {
            return Err(Error::new(Code::InvalidRequest, "not a file operation"));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paths_are_normalized() {
        let home = Path::new("/home/me");
        let ok = |path: &str| normalize(path, Some(home)).unwrap();
        assert_eq!(ok("/"), Path::new("/"));
        assert_eq!(ok("//"), Path::new("/"));
        assert_eq!(ok("/a//b/./c/"), Path::new("/a/b/c"));
        assert_eq!(ok("~"), Path::new("/home/me"));
        assert_eq!(ok("~/"), Path::new("/home/me"));
        assert_eq!(ok("~/notes/./a.txt"), Path::new("/home/me/notes/a.txt"));
        assert_eq!(
            normalize("~/x", Some(Path::new("/home//me/"))).unwrap(),
            Path::new("/home/me/x")
        );
        let code = |path: &str, home: Option<&Path>| normalize(path, home).unwrap_err().code;
        for bad in [
            "", "a/b", "./a", "/a/../b", "/..", "~other/x", "~x", "/a\0b",
        ] {
            assert_eq!(code(bad, Some(home)), Code::InvalidPath, "{bad:?}");
        }
        assert_eq!(code("~/x", None), Code::InvalidPath);
        assert_eq!(
            code(&format!("/{}", "a".repeat(4096)), None),
            Code::InvalidPath
        );
        assert!(normalize(&format!("/{}", "a".repeat(4095)), None).is_ok());
    }
    #[test]
    fn trash_info_is_url_encoded() {
        assert_eq!(
            encode_trash_path("/home/me/a b/é%#?.txt".as_bytes()),
            "/home/me/a%20b/%C3%A9%25%23%3F.txt"
        );
        assert_eq!(encode_trash_path(b"/x/A-z_0.9~"), "/x/A-z_0.9~");
        assert_eq!(
            trash_info(Path::new("/tmp/my file"), "2026-11-23T10:04:05"),
            "[Trash Info]\nPath=/tmp/my%20file\nDeletionDate=2026-11-23T10:04:05\n"
        );
        let date = deletion_date(1_795_300_000);
        assert_eq!(date.len(), 19);
        assert_eq!(&date[4..5], "-");
        assert_eq!(&date[10..11], "T");
        assert_eq!(&date[13..14], ":");
    }
    #[test]
    fn trash_names_are_numbered_and_bounded() {
        assert_eq!(trash_name("a.txt", 1), "a.txt");
        assert_eq!(trash_name("a.txt", 2), "a.2.txt");
        assert_eq!(trash_name("archive", 3), "archive.3");
        assert_eq!(trash_name(".bashrc", 2), ".bashrc.2");
        let long = format!("{}.jpeg", "é".repeat(127));
        assert!(long.len() > NAME_MAX - TRASHINFO.len());
        for n in [1, 2, 1000] {
            let name = trash_name(&long, n);
            assert!(name.len() <= NAME_MAX - TRASHINFO.len(), "{n}");
            assert!(name.ends_with(&format!(".{n}.jpeg")));
        }
    }
    #[test]
    fn content_is_classified() {
        assert_eq!(
            content(b"hello\n".to_vec()),
            (false, Encoding::Utf8, "hello\n".into())
        );
        assert_eq!(
            content(vec![0, 1, 2]),
            (true, Encoding::Base64, "AAEC".into())
        );
        assert_eq!(
            content(vec![0xff, 0xfe]),
            (true, Encoding::Base64, "//4=".into())
        );
        assert_eq!(escaped_len("a\"\n\u{1}é"), 1 + 2 + 2 + 6 + 2);
        // Text whose escaping would not fit one record goes as base64.
        let controls = "\u{1}".repeat(TEXT_BUDGET / 6 + 1);
        let (binary, encoding, _) = content(controls.into_bytes());
        assert_eq!((binary, encoding), (false, Encoding::Base64));
    }
    #[test]
    fn wire_names_match_the_contract() {
        let name = |value: serde_json::Value| value.as_str().unwrap().to_owned();
        assert_eq!(
            name(serde_json::json!(Code::NotADirectory)),
            "not_a_directory"
        );
        assert_eq!(
            name(serde_json::json!(Code::IsADirectory)),
            "is_a_directory"
        );
        assert_eq!(name(serde_json::json!(Code::CrossDevice)), "cross_device");
        assert_eq!(name(serde_json::json!(Encoding::Utf8)), "utf8");
        assert_eq!(name(serde_json::json!(Kind::Symlink)), "symlink");
        let request: Request = serde_json::from_str(
            r#"{"type":"file_write","request":"f4","path":"~/a","content":"x","base_hash":"e3"}"#,
        )
        .unwrap();
        match request {
            Request::FileWrite {
                encoding,
                create,
                base_hash,
                ..
            } => assert_eq!(
                (encoding, create, base_hash.as_deref()),
                (Encoding::Utf8, false, Some("e3"))
            ),
            other => panic!("{other:?}"),
        }
        // Unknown fields are refused, as for every stream request.
        assert!(
            serde_json::from_str::<Request>(
                r#"{"type":"file_stat","request":"f","path":"/","x":1}"#
            )
            .is_err()
        );
        let event = failure(
            "f".into(),
            Error::new(Code::Conflict, "changed").at(Path::new("/a")),
        );
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            serde_json::json!({"type":"file_error","request":"f","error":{"code":"conflict","message":"changed","path":"/a"}})
        );
    }
}

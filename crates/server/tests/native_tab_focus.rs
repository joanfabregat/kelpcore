//! Tab order after unparking and focus after closing or parking the current tab.
use kelpserver::native::{protocol::*, workspace::Workspace};
use std::{
    fs,
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

fn workspace(count: u64) -> Workspace {
    let mut ws = Workspace::new(160, 50).unwrap();
    ws.panes = (1..=count)
        .map(|id| PaneInfo {
            id,
            number: id as usize,
            palette: id as u8,
            pid: id as u32,
            title: format!("synthetic-{id}"),
            ..Default::default()
        })
        .collect();
    ws.active = 1;
    ws.reading = ReadingMode::Tabs(1);
    ws.reflow();
    ws
}
fn visible(ws: &Workspace) -> Vec<PaneId> {
    ws.panes
        .iter()
        .filter(|pane| !pane.parked)
        .map(|pane| pane.id)
        .collect()
}

#[test]
fn unparking_a_tab_puts_it_last() {
    let mut ws = workspace(4);
    ws.apply(&Action::Park(2)).unwrap();
    assert_eq!(visible(&ws), [1, 3, 4]);
    ws.apply(&Action::Restore(2)).unwrap();
    assert_eq!(visible(&ws), [1, 3, 4, 2]);
    assert_eq!(ws.active, 2);
    assert_eq!(ws.reading, ReadingMode::Tabs(2));
    let numbers: Vec<_> = ws.panes.iter().map(|p| (p.id, p.number)).collect();
    assert_eq!(numbers, [(1, 1), (3, 2), (4, 3), (2, 4)]);
    // Restoring a tab that is not parked does not reorder it.
    ws.apply(&Action::Restore(3)).unwrap();
    assert_eq!(visible(&ws), [1, 3, 4, 2]);
}

#[test]
fn parking_the_current_tab_focuses_the_previous_one() {
    let mut ws = workspace(4);
    for id in [3, 2, 4] {
        ws.apply(&Action::Focus(id)).unwrap();
    }
    ws.apply(&Action::Park(4)).unwrap();
    assert_eq!(ws.active, 2);
    assert_eq!(ws.reading, ReadingMode::Tabs(2));
    ws.apply(&Action::Park(2)).unwrap();
    assert_eq!(ws.active, 3);
}

#[test]
fn parking_skips_a_previous_tab_that_is_parked_or_gone() {
    let mut ws = workspace(5);
    for id in [3, 2, 5, 4] {
        ws.apply(&Action::Focus(id)).unwrap();
    }
    // The previous tab (5) is parked, and the one before it (2) has gone.
    ws.apply(&Action::Park(5)).unwrap();
    assert_eq!(ws.active, 4);
    ws.panes.retain(|pane| pane.id != 2);
    ws.apply(&Action::Park(4)).unwrap();
    assert_eq!(ws.active, 3);
}

#[test]
fn parking_without_focus_history_keeps_the_first_tab_fallback() {
    let mut ws = workspace(3);
    // Focus moved without any recorded history: only the current tab is known.
    ws.active = 3;
    ws.reading = ReadingMode::Tabs(3);
    ws.apply(&Action::Park(3)).unwrap();
    assert_eq!(ws.active, 1);
    assert_eq!(ws.reading, ReadingMode::Tabs(1));
}

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Daemon {
    root: PathBuf,
    socket: PathBuf,
    child: Child,
}
impl Daemon {
    fn start() -> Self {
        let root = std::env::temp_dir().join(format!(
            "kelpserver-tab-focus-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = root.join("control.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_kelpserver"))
            .args(["server", "--socket"])
            .arg(&socket)
            .arg("--state-dir")
            .arg(root.join("state"))
            .args(["--shell", "/bin/sh", "--cwd"])
            .arg(&root)
            .args(["--size", "160x50"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut daemon = Self {
            root,
            socket,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !daemon.socket.exists() {
            assert!(daemon.child.try_wait().unwrap().is_none(), "server exited");
            assert!(Instant::now() < deadline, "server startup timeout");
            thread::sleep(Duration::from_millis(10));
        }
        daemon
    }
    fn attach(&self) -> UnixStream {
        let mut stream = UnixStream::connect(&self.socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write_message(
            &mut stream,
            &ClientMessage::Attach {
                version: VERSION,
                cols: 160,
                rows: 50,
                takeover: false,
            },
        )
        .unwrap();
        stream
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        if let Ok(mut stream) = UnixStream::connect(&self.socket) {
            let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
            let _ = write_message(&mut stream, &ClientMessage::Command(Action::Shutdown));
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Waits for a full frame or patch matching `predicate`, tracking patches.
fn wait(
    stream: &mut UnixStream,
    frame: &mut Option<(Frame, u64)>,
    predicate: impl Fn(&Frame) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        assert!(Instant::now() < deadline, "frame deadline exceeded");
        match read_message(stream).unwrap().expect("disconnected") {
            ServerMessage::Frame(next) => *frame = Some((next, 1)),
            ServerMessage::Patch(patch) => {
                let (base, revision) = frame.as_ref().expect("patch before frame");
                let next = kelpserver::native::delta::apply(base, *revision, &patch).unwrap();
                *frame = Some((next, patch.revision));
            }
            ServerMessage::Error(error) => panic!("server error: {error}"),
            _ => continue,
        }
        if frame.as_ref().is_some_and(|(f, _)| predicate(f)) {
            return;
        }
    }
}
fn close(stream: &mut UnixStream, frame: &mut Option<(Frame, u64)>, id: PaneId) {
    write_message(stream, &ClientMessage::Action(Action::ClosePaneRequest(id))).unwrap();
    write_message(stream, &ClientMessage::Action(Action::ClosePaneConfirm(id))).unwrap();
    wait(stream, frame, |f| {
        f.panes.iter().all(|view| view.pane.id != id)
    });
}

#[test]
fn closing_the_current_tab_focuses_the_previous_open_one() {
    let daemon = Daemon::start();
    let mut stream = daemon.attach();
    let mut frame = None;
    wait(&mut stream, &mut frame, |f| {
        f.controller && f.panes.len() == 1
    });
    for _ in 0..4 {
        write_message(&mut stream, &ClientMessage::Action(Action::NewTab)).unwrap();
    }
    wait(&mut stream, &mut frame, |f| f.panes.len() == 5);
    let ids: Vec<PaneId> = frame
        .as_ref()
        .unwrap()
        .0
        .panes
        .iter()
        .map(|v| v.pane.id)
        .collect();
    let [_, b, c, d, e] = ids[..] else {
        panic!("expected five tabs: {ids:?}");
    };
    for id in [d, c, b, e] {
        write_message(&mut stream, &ClientMessage::Action(Action::Focus(id))).unwrap();
        wait(&mut stream, &mut frame, |f| f.active == id);
    }
    // Closing the current tab returns to the previous one, not the first.
    close(&mut stream, &mut frame, e);
    assert_eq!(frame.as_ref().unwrap().0.active, b);
    // The previous tab of the current one is closed in the background;
    // closing the current tab then skips it for the next most recent.
    close(&mut stream, &mut frame, c);
    assert_eq!(frame.as_ref().unwrap().0.active, b);
    close(&mut stream, &mut frame, b);
    assert_eq!(frame.as_ref().unwrap().0.active, d);
}

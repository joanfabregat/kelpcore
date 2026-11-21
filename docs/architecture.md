# Architecture

> Server-relevant sections of [KelpMux docs/architecture.md](https://github.com/joanfabregat/kelpmux/blob/eb31da5/docs/architecture.md) at `eb31da5`, copied when KelpServer moved to its own repository, with later server changes from KelpMux `cfe28de` (pane environment, startup-dialog activity). Frontend, TUI layout and legacy tmux material stays in KelpMux. "Kelpmux" below refers to the combined product at the time of writing; the behavior described is implemented by KelpServer.

KelpServer is the persistent server of native Kelp workspaces. It owns PTYs, interactive shells, terminal emulation, workspace geometry, agent metadata and client authorization. Clients (the KelpMux TUI, browser clients through `bridge`/`web-bridge`, and KelpHub) own presentation and input framing.

## Owned server and terminal engine

Each pane has a stable numeric ID, child process identity, PTY master and Alacritty terminal engine. The server drains PTY output into the engine, queues responses to terminal queries back to the application, and composes snapshots into client frames. ANSI styles and application colors, Unicode cell widths, cursor state, primary/alternate screens, history, selection and input modes belong to the engine. The engine handles fragmented terminal output; the compositor sanitizes labels and preserves wide-cell boundaries when clipping views.

PTY lifetime belongs to the server. Detaching or losing a frontend leaves its shells and agents running, and reconnecting sees the same pane IDs and process IDs. Explicit shutdown terminates owned terminals. The metadata journal does not reconstruct PTYs after shutdown, a crash or reboot.

The server polls nonblocking sockets and PTYs, bounds messages and per-client queues, and disconnects malformed or slow clients. A separate metadata worker avoids holding the terminal loop behind process scanning or a journal lock. Its bounded request/result channels let the terminal loop skip work rather than wait for discovery. Rendering uses periodic ticks to animate working markers and sends composed frames when state changes.

## Private protocol and control

A versioned length-prefixed JSON protocol runs over a same-user Unix socket. Dedicated directories and socket files are private; instance locks, owner checks, peer credentials and symlink rejection guard the endpoint. Frame messages include pane rectangles, terminal input modes, reading state, active pane, controller status and help state. Raw input/paste payloads are bounded to 64 KiB and protocol frames have a separate size bound.

Exactly one attached client controls interactive input and backend dimensions. Other clients observe and cannot mutate state through the interactive action/input channel. Takeover is explicit, including after the controller disconnects, and resizes the workspace to the new controller. Observer rendering clips or pads the same backend frame; observer resize messages do not resize PTYs. A separate same-user `ctl` command channel permits explicit administration without acquiring the interactive controller role.

Independent clients use the separate pane stream (version 1) over the same socket or `kelpserver bridge` over SSH, with per-pane leases and sizes; see [the stream contract](../crates/server/docs/native-client-protocol.md).

## Workspace geometry and process safety

The pure workspace layer computes balanced geometry from unparked terminal count and controller dimensions. Rows divide the full width evenly, including partial rows, and rebalance as terminals or dimensions change. One row per pane is reserved for its header and the last terminal row for a shared footer; vertical separators use solid quarter-cell strokes. Pane identity and palette stay with the process while swaps change logical order and displayed numbers.

New Tab prepares a PTY before committing the new terminal. Changing the grid view preserves the inventory of processes. Manual native layout and boundary-resize requests are rejected. Park/Restore modifies visibility and preserves IDs, PTYs, output and status; observers require control before changing it. Parking is not process suspension. Exact startup tab counts replace saved geometry, with legacy grid settings accepted as migration input. Shell exit removes its terminal and rebalances; explicit close requires confirmation. Last-shell exit detaches clients normally when there is no scratch terminal to preserve.

Screen resizes preserve every process, including foreground, background and stopped jobs. The separate legacy tmux helper retains its original occupancy guard; it does not control native workspaces.

Panes are new top-level terminals, so their shells do not inherit the session markers of whoever started the server. KelpServer removes `CLAUDECODE`, every `CLAUDE_CODE_*` variable (for example `CLAUDE_CODE_CHILD_SESSION`, which makes Claude Code treat a new session as a child and turn off transcript saving, and `CLAUDE_CODE_MESSAGING_SOCKET`), `CLAUDE_PID`, `CLAUDE_EFFORT`, `CODEX_SESSION_ID`, `CODEX_THREAD_ID`, `CODEX_TOOL_CALL_ID`, `CODEX_CI`, `TMUX`, `TMUX_PANE`, `STY`, `SSH_CONNECTION`, `SSH_CLIENT`, `SSH_TTY` and `NO_COLOR`. User configuration is kept, including API keys, `CLAUDE_CONFIG_DIR` and `CODEX_HOME`, so a server started from inside an agent or tmux session still gives its panes a clean, configured environment. `start` also launches the background server process itself without the agent and multiplexer markers.

## Discovery and journal

Native discovery reuses the process and transcript evidence adapters for Claude/Codex. The outermost recognizable interactive agent under a pane shell supplies metadata; headless tasks and shared app/MCP servers are excluded. PID plus process birth ticks guards against reuse. Session ID, title and lifecycle are evidence-derived, and missing or unbound evidence stays unknown. Some Codex shared-daemon TUIs therefore have unknown pane-bound session identity.

Native metadata discovery operates without requiring hooks. The optional `native-event` receiver accepts bounded JSON on standard input and resolves the pane from an explicit ID or the server-provided `KELPMUX_PANE` environment variable. It validates the registered agent kind, live process birth identity and session conflicts before updating metadata. Events arriving before discovery registers the agent fail promptly. Nonblocking journal access prevents a hook from waiting indefinitely for a held lock; the worker picks up accepted metadata on its next scan, normally within two seconds. Shared-daemon session ownership still requires verifiable pane-bound evidence.

The worker and native hook receiver use the registry and JSONL lifecycle journal under `STATE_DIR/native`, with private files, atomic registry replacement, locking and seven-day retention. `native-event --state-dir` takes the server state root; the existing journal/name commands take the nested `STATE_DIR/native` registry path. Current live agents remain registered beyond seven days. No prompt, response, tool argument, credential or transcript is stored in the journal. Hook setup is explicit and existing legacy tmux hook command examples require replacement before native use.

## Workspace lifecycle

A named workspace selects its private state directory and socket. `start` creates an explicit persistent instance, `attach` connects without replacing the server, `status` inspects it, and `stop` requests shutdown of that instance. Existing foreground `server` and explicit socket commands remain independent. Startup validates names and live endpoints, avoids duplicate instances, and reports failures rather than silently replacing another workspace. `ssh-entry` is a login entry point that starts or attaches the selected workspace; SSH authentication and terminal transport remain the client's responsibility. `attach` and `ssh-entry` are KelpMux commands; `kelpserver` provides `start`, `status`, `stop`, the foreground `server` and the `bridge`.

## Phase-two boundaries

Activity history persists bounded lifecycle metadata separately from terminal history. Discovery produces deduplicated transitions; initial state does not count as a fresh completion, and an agent first seen waiting for input before it has a session (a startup dialog such as Claude Code's folder trust prompt) does not alert until it asks again later. Acknowledgements are persisted, and sound is opt-in in the frontend. Recovery collection uses a bounded background job with cancellation and nonblocking journal access, keeping the terminal I/O loop responsive.

Scratch is a separate persistent PTY whose visibility overlays the base view. It is excluded from the automatic grid and parking. Closing the last main terminal creates a replacement main shell; a main-shell exit also retains a main shell when scratch exists, preserving its process. Templates prepare new PTYs with validated names and directories; they never reconfigure an existing live instance.

Terminal history, search and selection remain in memory. Alternate-screen entry retains a bounded primary-history snapshot for search; revealing that history waits until the application returns to its primary screen. No search text or terminal contents enter the recovery journal. View modes and widths are remembered by column profile. Parking survives frontend reconnects while the owning daemon remains alive.

Composed frame deltas carry checked base/revision numbers and validated changed-cell indices. Full snapshots reset the baseline; backpressured queue replacement uses a snapshot rather than a patch whose base could be missing. Content-free diagnostic counters and a synthetic payload benchmark expose these mechanics without inspecting agent text. Independent native clients will require per-pane subscriptions beyond the current composed-frame protocol; these now exist as the independent pane stream.

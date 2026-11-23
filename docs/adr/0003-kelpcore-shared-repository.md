# ADR 0003: KelpCore holds the server and every client-shared component

- Status: Accepted by Joan 2026-11-22 (America/Los_Angeles); phase A (client core and browser layer moved) implemented 2026-11-22; binary screens (section 2): client adoption (client core decoding, browser opt-in) implemented 2026-11-22.
- Numbering continues KelpServer's ADRs ([0001](../../crates/server/docs/adr/0001-native-client-protocol-and-device-capabilities.md), [0002](../../crates/server/docs/adr/0002-generic-session-server.md)), which stay where they are.
- Related: KelpMux [ADR 0003 (web client)](https://github.com/joanfabregat/kelpmux/blob/main/docs/adr/0003-web-client-in-kelpmux.md), KelpHub ADR 0007 (multi-tenant, Kubernetes-only; private repository).

## Context

The Kelp project had three repositories: KelpServer (the session server and the `kelpclient-protocol` wire types), KelpMux (TUI, web client, the `kelpclient` Rust/WASM client core) and KelpHub (private, paused, being rebuilt as a multi-tenant Kubernetes product). KelpMux and KelpHub pin KelpServer by Git revision.

Code shared by the two applications beyond the server was shared by copying. On 2026-11-22 KelpHub's web client held about 4,000 lines copied from KelpMux's `web/src/terminal/`: `renderer.ts`, `p256.ts`, `keyboard.ts`, `mouse.ts` and the in-browser SSH (`ssh/`) were byte-identical, `connection.ts` differed by two lines, and `Terminal.tsx` and `session-worker.ts` had drifted by about 300 lines each. KelpMux is now getting a file browser and editor that exist only in KelpHub, and toasts and notification rules that KelpHub will need too.

Separately, the stream protocol sends every terminal cell as a JSON object of about 96 bytes. A full-window snapshot (about 300×80 cells) is about 2.3 MB, sent whenever a client subscribes to a pane, the pane's size changes or a client falls behind. In the browser that costs about 250 ms when switching from the grid to one terminal or between tabs, against under 100 ms in the TUI, which reads the same JSON over a local socket.

## Decision

### 1. One shared repository, KelpCore

The KelpServer repository becomes **KelpCore** (`joanfabregat/kelpcore`, public, Apache-2.0). Its history was pushed unchanged on 2026-11-22; `joanfabregat/kelpserver` is archived once KelpMux depends on KelpCore. KelpCore holds the server and everything KelpMux and KelpHub share; KelpMux and KelpHub keep only their own application.

**Boundary rule:** a component belongs in KelpCore when both applications use it, or when it defines how clients talk to the server. Application layout, navigation and product features stay in each application. KelpServer's boundary rule (ADR 0002) still applies to the server package itself: the server has no UI.

| Package | Location | Content |
| --- | --- | --- |
| `kelpserver` | `crates/server` | The session server, unchanged in scope (ADR 0002), plus the server-side operations added below. Executable still named `kelpserver`. |
| `kelpclient-protocol` | `crates/protocol` | Wire types, framing and the binary screen codec (section 2). |
| `kelpclient` | `crates/client` | The Rust client core used natively by the TUI and compiled to WASM for browsers (screen model, selection, history layout, decoding). Moved from KelpMux. |
| `@kelpcore/web` (npm, not published) | `web/` | The browser layer both web clients use: the WebGL/Canvas renderer, the in-browser SSH, the shared worker and session, keyboard, mouse, wheel and touch keys, and the terminal component. Moved from KelpMux's `web/src/terminal/`. Later: the file browser and editor UI, toasts, notification rules. |

Package and executable names are kept, so consumers change only the repository URL of their pins.

**Consumption.** Rust crates are pinned by exact Git revision, as today. The web package is an npm Git dependency pinned to the same commit (`github:joanfabregat/kelpcore#<sha>` from a root `package.json` that exposes `web/`); it ships TypeScript sources that the application's Vite build compiles, so there is no separate build or registry. An application pins one KelpCore revision for all packages.

**Order of work for a cross-cutting change:** KelpCore first (server, protocol, client core and web layer in one pull request when they change together), then the applications bump their pin.

KelpMux moves first. KelpHub switches to KelpCore when it resumes; until then it keeps building against its current pins (an archived repository stays fetchable).

### 2. A binary screen format in the stream protocol

Screens (snapshots and patches) get a compact binary encoding. Control messages (pane lists, leases, notifications, requests and replies) stay JSON: they are small and change shape often.

- **Snapshot:** the pane's size and cursor, a table of the distinct cell styles used (foreground, background, attributes, underline colour, hyperlink), then each row as runs of `style index + UTF-8 text`, with trailing default blanks omitted. Wide characters keep their width information.
- **Patch:** the rows changed since the client's last revision, each re-encoded whole, plus cursor and modes. A row is a few dozen bytes, so this is simpler than per-cell changes and not larger in practice.
- **Framing:** the existing 4-byte length prefix and 16 MiB limit stay. A binary record is distinguished from a JSON record by a framing marker defined in `kelpclient-protocol` and is only ever sent to a connection that asked for it: the server advertises the `binary-screens` capability in `Welcome`, and the client then sends a `binary_screens` request (as with `session_events`). A field in `Hello` would not do, because older servers reject unknown `Hello` fields. Other connections keep receiving JSON, so the server, KelpMux and KelpHub can migrate independently.
- **One codec, in Rust,** in `kelpclient-protocol`: the server encodes, the TUI decodes natively and browsers decode through the `kelpclient` WASM core. Bridges forward binary records without parsing them.
- **In the browser,** the WASM core decodes into typed arrays, and the shared worker transfers (not copies) them to each window.

Expected size of a 300×80 snapshot: tens of kilobytes instead of 2.3 MB. This is an estimate to be measured by the implementation.

Raw PTY output was considered and rejected as the screen transport: clients attaching mid-session need a snapshot anyway, a busy pane would send every byte instead of at most 30 diffs a second, and every client would have to run an identical terminal emulator.

### 3. Server operations shared by every client

New session-API operations live in `kelpserver` because they act on the machine where the shells run (the user's host for KelpMux, the workspace pod for KelpHub) and every client reaches the server the same way. Bridges relay them unchanged, as they already relay uploads. Each is an optional capability.

- **Files:** list, stat, read, write (atomic replace with a content-hash conflict check), create folder, rename and move, move to trash (freedesktop trash in `~/.local/share/Trash`) and immediate delete. The design starts from KelpHub's `files.rs` (descriptor-relative opens, no symlink following, atomic writes, hard limits on sizes and entry counts), moved into KelpCore with Joan's approval (decision 185b). File operations grant no access beyond the shell the same authenticated user already has; the security boundary remains the authenticated stream connection. Uploads keep using the existing `upload_*` requests.
- **Notifications:** a bell and the agent notification it accompanies are merged by the server, so every client shows one event (decision 205a).

## Consequences

- KelpMux and KelpHub each pin one KelpCore revision instead of KelpServer plus copied code.
- A change crossing the protocol, client core and browser layer lands in one KelpCore pull request, checked by one CI.
- KelpCore's CI gains a Node job for `web/` next to the Rust jobs.
- Server releases and shared-library changes share one history and version line; the server keeps its own crate version.
- Documentation naming KelpServer as a repository (the Kelp `CLAUDE.md`, KelpMux docs and ADR 0003, KelpHub docs) is updated to KelpCore as the migration lands. The product and executable name KelpServer stays.

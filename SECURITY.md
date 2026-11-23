# Security policy

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/joanfabregat/kelpserver/security/advisories/new). Do not disclose suspected vulnerabilities, exploit details or credentials in public issues, discussions or pull requests. If private reporting is unavailable, contact the repository owner through an existing private channel to arrange a report; do not publish the details as a fallback.

Include affected versions or commits, prerequisites, impact and a minimal reproduction. Redact credentials and user data. Never attack a deployed instance, exhaust resources or access another user's workspace to demonstrate a problem without specific authorization.

## Supported code

Security reports about the default branch are welcome. This repository has no published release of its own yet; KelpServer builds shipped in existing KelpMux releases remain covered through [KelpMux's security policy](https://github.com/joanfabregat/kelpmux/security/policy). Older releases do not have a promised long-term support window. Reports that concern a client (the KelpMux TUI, web client or KelpClient) belong to that client's repository; KelpHub maintains its web and management code separately. When unsure, report here.

## Boundaries

KelpServer runs every terminal with the authority of the Linux user who owns it. Its local socket and state are private to that user, and peer credentials are checked, but this is not isolation against hostile processes running as the same user. A terminal client can exercise that user's authority; SSH authentication does not make executing untrusted terminal commands safe.

KelpServer contains no browser relay or static asset server; the browser relay belongs to KelpMux (`kelpmux web-relay`) and is covered by its security policy. A browser reaches a workspace only through an SSH-authenticated bridge command (`kelpserver bridge` or a client's own bridge), with the authority of that Linux user.

## File operations

The `files` stream capability (list, stat, read, write, create a folder, rename, move to the trash, delete) acts on the host where the shells run, with the server user's credentials. It grants nothing a stream connection does not already have, since any connection may type into a shell as that user; the security boundary remains the authenticated stream connection (the private, peer-checked socket, or SSH for a bridge). It never elevates privileges and confines nothing to a directory. In scope for reports: a file operation that acts through a symlink at the final component of a change (writes, renames, trash, delete), follows a symlink while deleting a tree, writes non-atomically, escapes its documented bounds (sizes, entry counts, paths per request, concurrent operations), blocks the server's event loop, or reaches anything the server otherwise refuses. Out of scope: that a connection can read or delete the server user's files at all, and races with other processes of the same user, which the same-user boundary above already excludes. Front ends that relay several users' requests to one server must apply their own policy; see [the files contract](crates/server/docs/native-client-protocol.md#files-files).

# Security policy

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/joanfabregat/kelpserver/security/advisories/new). Do not disclose suspected vulnerabilities, exploit details or credentials in public issues, discussions or pull requests. If private reporting is unavailable, contact the repository owner through an existing private channel to arrange a report; do not publish the details as a fallback.

Include affected versions or commits, prerequisites, impact and a minimal reproduction. Redact credentials and user data. Never attack a deployed instance, exhaust resources or access another user's workspace to demonstrate a problem without specific authorization.

## Supported code

Security reports about the default branch are welcome. This repository has no published release of its own yet; KelpServer builds shipped in existing KelpMux releases remain covered through [KelpMux's security policy](https://github.com/joanfabregat/kelpmux/security/policy). Older releases do not have a promised long-term support window. Reports that concern a client (the KelpMux TUI, web client or KelpClient) belong to that client's repository; KelpHub maintains its web and management code separately. When unsure, report here.

## Boundaries

KelpServer runs every terminal with the authority of the Linux user who owns it. Its local socket and state are private to that user, and peer credentials are checked, but this is not isolation against hostile processes running as the same user. A terminal client can exercise that user's authority; SSH authentication does not make executing untrusted terminal commands safe.

The generic web relay serves caller-supplied static assets and forwards WebSocket bytes to one configured SSH destination after checking its host key. It never terminates SSH, holds no private key and has no path to a workspace socket. The origin that serves browser code remains trusted frontend delivery code. Keep the relay, user-owned services and credentials separated.

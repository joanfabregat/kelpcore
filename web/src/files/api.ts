// A typed client for KelpServer's `files` stream requests (file browsers and
// editors acting on the host where the shells run; see KelpCore's
// native-client-protocol.md, "Files"). Each call sends one correlated
// `file_*` request and resolves with its reply, or rejects with a
// `FileError` carrying the server's error code. Replies may arrive in any
// order and between other events; they are matched by request identifier.
// At most the server's `max_in_flight` requests run at once: further calls
// wait in a queue, and a `busy` refusal (other windows sharing the
// connection, or the server's own bound) is retried a few times.
// The application supplies the transport, as for uploads; this module
// imports nothing from it, nor from React or the DOM.

/** Welcome `file_limits`, read from the server rather than hardcoded. */
export type FileLimits = {
  max_read_bytes: number;
  max_write_bytes: number;
  max_list_entries: number;
  max_paths: number;
  max_in_flight: number;
  max_path_bytes: number;
  max_depth: number;
};
export type FileKind = "file" | "dir" | "symlink" | "other";
/** One entry as `lstat` sees it: a symlink is described, not followed. */
export type FileEntry = {
  name: string;
  kind: FileKind;
  /** Bytes (a symlink's link length). */
  size: number;
  /** Unix seconds. */
  modified: number;
  /** Permission bits, set-id and sticky included. */
  mode: number;
  /** A symlink's target, as text. */
  target?: string;
};
export type FileListing = {
  /** The listed directory, normalised and absolute (`~` expanded). */
  path: string;
  /** Directories first, then the other entries, each in byte order. */
  entries: FileEntry[];
  /** Entries were left out by the server's bounds. */
  truncated: boolean;
  /** Names that are not UTF-8, which cannot be addressed. */
  skipped: number;
};
export type FileContent = {
  path: string;
  size: number;
  modified: number;
  mode: number;
  /** SHA-256 of the content: the `baseHash` of a later write. */
  hash: string;
  /** The bytes hold NUL or are not UTF-8. */
  binary: boolean;
  /** The text, for a file that is not binary. */
  text?: string;
  /** The bytes, for a binary file. */
  bytes?: Uint8Array;
};
export type FileWritten = {
  path: string;
  hash: string;
  size: number;
  modified: number;
  created: boolean;
};
export type FileErrorCode =
  | "invalid_path"
  | "invalid_request"
  | "invalid_content"
  | "not_found"
  | "permission_denied"
  | "not_a_directory"
  | "is_a_directory"
  | "not_a_file"
  | "symlink"
  | "exists"
  | "not_empty"
  | "conflict"
  | "too_large"
  | "cross_device"
  | "read_only"
  | "no_space"
  | "busy"
  | "io"
  /** Client side: the connection closed or the reply never came. */
  | "closed"
  | "timeout";
/** The server's error body (`file_error.error`). */
export type FileErrorBody = {
  code: string;
  message: string;
  path?: string;
  current_hash?: string;
  size?: number;
};
export type FileRemoval = {
  removed: { path: string; trashed_as?: string }[];
  failed: { path: string; error: FileError }[];
};
/** Sends stream requests and delivers this connection's file events. */
export type FilesTransport = {
  send(type: string, params: any): void;
  listen(handler: (event: any) => void): () => void;
};

/** A failed or refused file request. `code` is the server's code, or
 * `closed` / `timeout` when no reply could be had. */
export class FileError extends Error {
  code: FileErrorCode | string;
  path?: string;
  /** `conflict`: the hash of the content now on disk, when it was hashed. */
  currentHash?: string;
  /** `too_large`: the file's size. */
  size?: number;
  constructor(body: FileErrorBody) {
    super(body.message || describeCode(body.code));
    this.name = "FileError";
    this.code = body.code;
    if (body.path) this.path = body.path;
    if (body.current_hash) this.currentHash = body.current_hash;
    if (typeof body.size === "number") this.size = body.size;
  }
}
const descriptions: Record<string, string> = {
  invalid_path: "The path is not valid",
  invalid_request: "The request was refused",
  invalid_content: "The content could not be decoded",
  not_found: "No such file or folder",
  permission_denied: "Permission denied",
  not_a_directory: "Not a folder",
  is_a_directory: "This is a folder",
  not_a_file: "Not a regular file",
  symlink: "This is a symbolic link",
  exists: "An entry with this name already exists",
  not_empty: "The folder is not empty",
  conflict: "The file changed on disk",
  too_large: "Too large",
  cross_device: "On another file system",
  read_only: "Read-only file system",
  no_space: "No space left on the device",
  busy: "The server is busy with other file operations",
  io: "Input/output error",
  closed: "The connection is closed",
  timeout: "No reply from the server; the outcome is unknown",
};
/** A short description of an error code, for a message without one. */
export function describeCode(code: string): string {
  return descriptions[code] || code;
}

const limitKeys = [
  "max_read_bytes",
  "max_write_bytes",
  "max_list_entries",
  "max_paths",
  "max_in_flight",
  "max_path_bytes",
  "max_depth",
] as const;

/** The Welcome's `file_limits`, or null when absent or malformed. */
export function readFileLimits(value: any): FileLimits | null {
  if (!value || typeof value !== "object") return null;
  const limits: any = {};
  for (const key of limitKeys) {
    const n = value[key];
    if (!Number.isSafeInteger(n) || n < 1) return null;
    limits[key] = n;
  }
  return limits as FileLimits;
}

/** Request identifiers of this module start with this prefix. */
export const FILE_REQUEST_PREFIX = "file-";
/** Whether a stream event belongs to a file request: a `file_*` reply, or
 * the `done` / plain `error` answering one. */
export function isFileEvent(e: any): boolean {
  if (!e || typeof e.type !== "string") return false;
  if (e.type.startsWith("file_")) return true;
  return (
    (e.type === "done" || e.type === "error") &&
    typeof e.request === "string" &&
    e.request.startsWith(FILE_REQUEST_PREFIX)
  );
}

/** Standard padded base64 to bytes. */
export function base64ToBytes(text: string): Uint8Array {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}
function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000)
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(binary);
}

/** The reply event type that completes each request type. */
const replies: Record<string, string> = {
  file_list: "file_list",
  file_stat: "file_stat",
  file_read: "file_content",
  file_write: "file_written",
  file_mkdir: "done",
  file_rename: "done",
  file_trash: "file_removed",
  file_delete: "file_removed",
};

type Pending = {
  type: string;
  params: any;
  resolve: (event: any) => void;
  reject: (error: FileError) => void;
  attempts: number;
  timer?: ReturnType<typeof setTimeout>;
};

export type FilesClientOptions = {
  limits: FileLimits;
  /** How long to wait for a reply before giving up (default 120 s). The
   * server cannot cancel a request, so its outcome is then unknown. */
  timeoutMs?: number;
  /** Retries of a request refused as `busy` (default 5). */
  busyRetries?: number;
  /** Delay before retrying a `busy` request (default 300 ms). */
  busyDelayMs?: number;
};

let serial = 0;
function requestId(): string {
  const random = new Uint8Array(4);
  crypto.getRandomValues(random);
  const hex = Array.from(random, (b) => b.toString(16).padStart(2, "0")).join(
    "",
  );
  return `${FILE_REQUEST_PREFIX}${hex}-${++serial}`;
}

export class FilesClient {
  readonly limits: FileLimits;
  private transport: FilesTransport;
  private timeoutMs: number;
  private busyRetries: number;
  private busyDelayMs: number;
  /** Requests sent and waiting for their reply, by identifier. */
  private running = new Map<string, Pending>();
  /** Requests waiting for a free slot, in order. */
  private queue: Pending[] = [];
  /** Requests refused as `busy`, waiting to be retried. */
  private retrying = new Set<Pending>();
  private stop: () => void;
  private closed = false;

  constructor(transport: FilesTransport, options: FilesClientOptions) {
    this.transport = transport;
    this.limits = options.limits;
    this.timeoutMs = options.timeoutMs ?? 120_000;
    this.busyRetries = options.busyRetries ?? 5;
    this.busyDelayMs = options.busyDelayMs ?? 300;
    this.stop = transport.listen((event) => this.event(event));
  }

  /** A directory's entries. `path` is absolute, `~` or starts with `~/`. */
  async list(path: string): Promise<FileListing> {
    const e = await this.request("file_list", { path });
    return {
      path: e.path,
      entries: e.entries,
      truncated: !!e.truncated,
      skipped: e.skipped || 0,
    };
  }
  /** One entry, not following a final symlink. */
  async stat(path: string): Promise<{ path: string; entry: FileEntry }> {
    const e = await this.request("file_stat", { path });
    return { path: e.path, entry: e.entry };
  }
  /** A regular file's whole content, or `too_large` (with its `size`) when
   * it is larger than `maxBytes` (default: the server's `max_read_bytes`). */
  async read(path: string, options: { maxBytes?: number } = {}) {
    const params: any = { path };
    if (options.maxBytes !== undefined) params.max_bytes = options.maxBytes;
    const e = await this.request("file_read", params);
    const content: FileContent = {
      path: e.path,
      size: e.size,
      modified: e.modified,
      mode: e.mode,
      hash: e.hash,
      binary: !!e.binary,
    };
    if (e.binary)
      content.bytes =
        e.encoding === "base64"
          ? base64ToBytes(e.content)
          : new TextEncoder().encode(e.content);
    else
      content.text =
        e.encoding === "base64"
          ? new TextDecoder().decode(base64ToBytes(e.content))
          : e.content;
    return content;
  }
  /** Replaces a file atomically, or creates it with `create`. With
   * `baseHash`, a file changed since that hash is not written: the call
   * rejects with code `conflict`. Text is sent as UTF-8, bytes as base64. */
  async write(
    path: string,
    content: string | Uint8Array,
    options: { baseHash?: string; create?: boolean } = {},
  ): Promise<FileWritten> {
    const size =
      typeof content === "string"
        ? new TextEncoder().encode(content).length
        : content.length;
    if (size > this.limits.max_write_bytes)
      throw new FileError({
        code: "too_large",
        message: `The file is larger than the server's ${this.limits.max_write_bytes}-byte write limit.`,
        size,
      });
    const params: any =
      typeof content === "string"
        ? { path, content }
        : { path, content: bytesToBase64(content), encoding: "base64" };
    if (options.baseHash) params.base_hash = options.baseHash;
    if (options.create) params.create = true;
    const e = await this.request("file_write", params);
    return {
      path: e.path,
      hash: e.hash,
      size: e.size,
      modified: e.modified,
      created: !!e.created,
    };
  }
  /** Creates one folder (its parent must exist). */
  async mkdir(path: string): Promise<void> {
    await this.request("file_mkdir", { path });
  }
  /** Renames or moves an entry within one file system; an existing `to` is
   * replaced only with `overwrite`. */
  async rename(
    from: string,
    to: string,
    options: { overwrite?: boolean } = {},
  ): Promise<void> {
    const params: any = { from, to };
    if (options.overwrite) params.overwrite = true;
    await this.request("file_rename", params);
  }
  /** Moves entries to the freedesktop home trash. */
  trash(paths: string[]): Promise<FileRemoval> {
    return this.removal("file_trash", paths);
  }
  /** Deletes entries permanently, folders with their contents. */
  delete(paths: string[]): Promise<FileRemoval> {
    return this.removal("file_delete", paths);
  }
  /** Rejects every waiting call and stops listening. */
  close() {
    if (this.closed) return;
    this.closed = true;
    this.stop();
    const error = () =>
      new FileError({ code: "closed", message: describeCode("closed") });
    for (const pending of [
      ...this.running.values(),
      ...this.queue,
      ...this.retrying,
    ]) {
      clearTimeout(pending.timer);
      pending.reject(error());
    }
    this.running.clear();
    this.queue = [];
    this.retrying.clear();
  }

  private async removal(type: string, paths: string[]): Promise<FileRemoval> {
    if (!paths.length) return { removed: [], failed: [] };
    if (paths.length > this.limits.max_paths)
      throw new FileError({
        code: "invalid_request",
        message: `At most ${this.limits.max_paths} entries can be removed at once.`,
      });
    const e = await this.request(type, { paths });
    return {
      removed: e.removed || [],
      failed: (e.failed || []).map((f: any) => ({
        path: f.path,
        error: new FileError(f.error),
      })),
    };
  }
  private request(type: string, params: any): Promise<any> {
    if (this.closed)
      return Promise.reject(
        new FileError({ code: "closed", message: describeCode("closed") }),
      );
    return new Promise((resolve, reject) => {
      this.queue.push({ type, params, resolve, reject, attempts: 0 });
      this.pump();
    });
  }
  private pump() {
    while (
      !this.closed &&
      this.queue.length &&
      this.running.size < Math.max(1, this.limits.max_in_flight)
    )
      this.dispatch(this.queue.shift()!);
  }
  private dispatch(pending: Pending) {
    const id = requestId();
    this.running.set(id, pending);
    pending.timer = setTimeout(() => {
      if (this.running.get(id) !== pending) return;
      this.running.delete(id);
      pending.reject(
        new FileError({ code: "timeout", message: describeCode("timeout") }),
      );
      this.pump();
    }, this.timeoutMs);
    try {
      this.transport.send(pending.type, { request: id, ...pending.params });
    } catch (error) {
      clearTimeout(pending.timer);
      this.running.delete(id);
      const message = error instanceof Error ? error.message : String(error);
      pending.reject(
        new FileError({
          code: /too large/i.test(message) ? "too_large" : "closed",
          message,
        }),
      );
      this.pump();
    }
  }
  private event(e: any) {
    if (!e || typeof e.request !== "string") return;
    const pending = this.running.get(e.request);
    if (!pending) return;
    let outcome: "resolve" | FileError;
    if (e.type === replies[pending.type]) outcome = "resolve";
    else if (e.type === "file_error")
      outcome = new FileError(
        e.error || { code: "io", message: "The request failed" },
      );
    else if (e.type === "error")
      outcome = new FileError({
        code: "invalid_request",
        message: e.message || "The request was refused",
      });
    else return;
    clearTimeout(pending.timer);
    this.running.delete(e.request);
    if (
      outcome !== "resolve" &&
      outcome.code === "busy" &&
      pending.attempts < this.busyRetries
    ) {
      pending.attempts += 1;
      this.retrying.add(pending);
      setTimeout(() => {
        if (!this.retrying.delete(pending) || this.closed) return;
        this.queue.unshift(pending);
        this.pump();
      }, this.busyDelayMs * pending.attempts);
    } else if (outcome === "resolve") pending.resolve(e);
    else pending.reject(outcome);
    this.pump();
  }
}

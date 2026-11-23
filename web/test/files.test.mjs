import { test } from "node:test";
import assert from "node:assert/strict";
import {
  FileError,
  FilesClient,
  base64ToBytes,
  describeCode,
  isFileEvent,
  readFileLimits,
} from "../src/files/api.ts";
import {
  baseName,
  breadcrumbs,
  displayPath,
  isInside,
  joinPath,
  moveProblem,
  nameProblem,
  parentPath,
  pastablePath,
  sortEntries,
  typedPaths,
} from "../src/files/paths.ts";
import {
  canSave,
  languageFor,
  loadFailed,
  loaded,
  newDoc,
  saveFailed,
  saveProblem,
  saved,
} from "../src/files/model.ts";
import {
  fileContentLimit,
  fileWriteLimit,
  payloadLimit,
  queueLimit,
  requestLimit,
} from "../src/request-limits.ts";

const limits = {
  max_read_bytes: 8388608,
  max_write_bytes: 8388608,
  max_list_entries: 5000,
  max_paths: 256,
  max_in_flight: 2,
  max_path_bytes: 4096,
  max_depth: 64,
};
const hash = "a".repeat(64);

/** A transport that records requests and lets the test answer them. */
function fakeTransport({ fail } = {}) {
  const sent = [];
  let handler = null;
  return {
    sent,
    transport: {
      send(type, params) {
        if (fail) throw Error(fail);
        sent.push({ type, ...params });
      },
      listen(h) {
        handler = h;
        return () => {
          handler = null;
        };
      },
    },
    reply: (event) => handler?.(event),
    listening: () => !!handler,
  };
}
const tick = () => new Promise((resolve) => setImmediate(resolve));

test("file limits are read from the Welcome and checked", () => {
  assert.deepEqual(readFileLimits(limits), limits);
  assert.equal(readFileLimits(undefined), null);
  assert.equal(readFileLimits({ ...limits, max_paths: 0 }), null);
  assert.equal(readFileLimits({ ...limits, max_depth: "64" }), null);
  const { max_in_flight, ...missing } = limits;
  assert.equal(readFileLimits(missing), null);
});

test("file events are recognised by type or by the request they answer", () => {
  assert.ok(isFileEvent({ type: "file_list", request: "x" }));
  assert.ok(isFileEvent({ type: "file_error", request: "x" }));
  assert.ok(isFileEvent({ type: "done", request: "file-ab-1" }));
  assert.ok(isFileEvent({ type: "error", request: "file-ab-1" }));
  assert.ok(!isFileEvent({ type: "done", request: "upload-1" }));
  assert.ok(!isFileEvent({ type: "error", message: "x" }));
  assert.ok(!isFileEvent({ type: "update" }));
  assert.ok(!isFileEvent(null));
});

test("requests are correlated by identifier and replies may come in any order", async () => {
  const t = fakeTransport();
  const client = new FilesClient(t.transport, { limits });
  const listing = client.list("~");
  const stat = client.stat("/home/me/a");
  await tick();
  assert.deepEqual(
    t.sent.map((r) => r.type),
    ["file_list", "file_stat"],
  );
  assert.ok(t.sent.every((r) => /^file-[0-9a-f]{8}-\d+$/.test(r.request)));
  assert.notEqual(t.sent[0].request, t.sent[1].request);
  assert.equal(t.sent[0].path, "~");
  const entry = { name: "a", kind: "file", size: 1, modified: 2, mode: 420 };
  // Unrelated events and replies of other requests are ignored.
  t.reply({ type: "file_list", request: "file-other-1", path: "/x" });
  t.reply({ type: "update" });
  t.reply({
    type: "file_stat",
    request: t.sent[1].request,
    path: "/home/me/a",
    entry,
  });
  t.reply({
    type: "file_list",
    request: t.sent[0].request,
    path: "/home/me",
    entries: [entry],
    truncated: false,
    skipped: 0,
  });
  assert.deepEqual(await stat, { path: "/home/me/a", entry });
  assert.deepEqual(await listing, {
    path: "/home/me",
    entries: [entry],
    truncated: false,
    skipped: 0,
  });
  client.close();
  assert.equal(t.listening(), false);
});

test("at most max_in_flight requests run; the rest wait for a reply", async () => {
  const t = fakeTransport();
  const client = new FilesClient(t.transport, { limits });
  const calls = [client.mkdir("/a"), client.mkdir("/b"), client.mkdir("/c")];
  await tick();
  assert.deepEqual(
    t.sent.map((r) => r.path),
    ["/a", "/b"],
  );
  t.reply({ type: "done", request: t.sent[1].request });
  await tick();
  assert.deepEqual(
    t.sent.map((r) => r.path),
    ["/a", "/b", "/c"],
  );
  t.reply({ type: "done", request: t.sent[0].request });
  t.reply({ type: "done", request: t.sent[2].request });
  await Promise.all(calls);
});

test("errors map to FileError with the server's code, path, hash and size", async () => {
  const t = fakeTransport();
  const client = new FilesClient(t.transport, { limits });
  const write = client.write("/home/me/a.txt", "mine", { baseHash: hash });
  const read = client.read("/home/me/big", { maxBytes: 10 });
  await tick();
  assert.equal(t.sent[0].base_hash, hash);
  assert.equal(t.sent[0].encoding, undefined, "text goes as UTF-8");
  assert.equal(t.sent[1].max_bytes, 10);
  t.reply({
    type: "file_error",
    request: t.sent[0].request,
    error: {
      code: "conflict",
      message: "the file changed since it was read",
      path: "/home/me/a.txt",
      current_hash: "b".repeat(64),
    },
  });
  t.reply({
    type: "file_error",
    request: t.sent[1].request,
    error: { code: "too_large", message: "too large", size: 99 },
  });
  await assert.rejects(write, (error) => {
    assert.ok(error instanceof FileError);
    assert.equal(error.code, "conflict");
    assert.equal(error.path, "/home/me/a.txt");
    assert.equal(error.currentHash, "b".repeat(64));
    assert.equal(error.message, "the file changed since it was read");
    return true;
  });
  await assert.rejects(read, (error) => error.code === "too_large" && error.size === 99);
  // A plain stream error naming the request (a malformed field) rejects too.
  const rename = client.rename("/a", "/b", { overwrite: true });
  await tick();
  assert.equal(t.sent[2].overwrite, true);
  t.reply({ type: "error", request: t.sent[2].request, message: "invalid" });
  await assert.rejects(rename, (error) => error.code === "invalid_request");
  assert.equal(describeCode("not_found"), "No such file or folder");
});

test("busy refusals are retried, then reported", async () => {
  const t = fakeTransport();
  const client = new FilesClient(t.transport, {
    limits,
    busyRetries: 1,
    busyDelayMs: 1,
  });
  const listing = client.list("/");
  await tick();
  const busy = (request) =>
    t.reply({
      type: "file_error",
      request,
      error: { code: "busy", message: "busy" },
    });
  busy(t.sent[0].request);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(t.sent.length, 2, "retried once");
  assert.notEqual(t.sent[1].request, t.sent[0].request);
  busy(t.sent[1].request);
  await assert.rejects(listing, (error) => error.code === "busy");
});

test("reads decode text and binary content", async () => {
  const t = fakeTransport();
  const client = new FilesClient(t.transport, { limits });
  const base = { size: 3, modified: 1, mode: 420, hash };
  const reads = [client.read("/t"), client.read("/e"), client.read("/b")];
  await tick();
  t.reply({
    type: "file_content",
    request: t.sent[0].request,
    path: "/t",
    ...base,
    binary: false,
    encoding: "utf8",
    content: "héllo",
  });
  // Text whose JSON escaping would overflow a record comes as base64.
  t.reply({
    type: "file_content",
    request: t.sent[1].request,
    path: "/e",
    ...base,
    binary: false,
    encoding: "base64",
    content: Buffer.from("\u0001é\u0002").toString("base64"),
  });
  t.reply({
    type: "file_content",
    request: t.sent[2].request,
    path: "/b",
    ...base,
    binary: true,
    encoding: "base64",
    content: "AP8=",
  });
  const [text, escaped, binary] = await Promise.all(reads);
  assert.equal(text.text, "héllo");
  assert.equal(text.binary, false);
  assert.equal(escaped.text, "\u0001é\u0002");
  assert.equal(binary.binary, true);
  assert.equal(binary.text, undefined);
  assert.deepEqual([...binary.bytes], [0, 255]);
  assert.deepEqual([...base64ToBytes("AQID")], [1, 2, 3]);
});

test("writes send bytes as base64, refuse oversized content and report removal outcomes", async () => {
  const t = fakeTransport();
  const client = new FilesClient(t.transport, {
    limits: { ...limits, max_write_bytes: 4, max_paths: 2 },
  });
  const write = client.write("/b", new Uint8Array([0, 255]), { create: true });
  await tick();
  assert.equal(t.sent[0].encoding, "base64");
  assert.equal(t.sent[0].content, "AP8=");
  assert.equal(t.sent[0].create, true);
  assert.equal(t.sent[0].base_hash, undefined);
  t.reply({
    type: "file_written",
    request: t.sent[0].request,
    path: "/b",
    hash,
    size: 2,
    modified: 5,
    created: true,
  });
  assert.deepEqual(await write, {
    path: "/b",
    hash,
    size: 2,
    modified: 5,
    created: true,
  });
  await assert.rejects(client.write("/c", "héllo"), (error) => {
    return error.code === "too_large" && error.size === 6;
  });
  await assert.rejects(
    client.trash(["/1", "/2", "/3"]),
    (error) => error.code === "invalid_request",
  );
  assert.deepEqual(await client.delete([]), { removed: [], failed: [] });
  const trash = client.trash(["/1", "/2"]);
  await tick();
  assert.equal(t.sent.length, 2, "nothing sent for refused requests");
  t.reply({
    type: "file_removed",
    request: t.sent[1].request,
    removed: [{ path: "/1", trashed_as: "/t/files/1" }],
    failed: [
      { path: "/2", error: { code: "cross_device", message: "elsewhere" } },
    ],
  });
  const outcome = await trash;
  assert.deepEqual(outcome.removed, [{ path: "/1", trashed_as: "/t/files/1" }]);
  assert.equal(outcome.failed[0].path, "/2");
  assert.ok(outcome.failed[0].error instanceof FileError);
  assert.equal(outcome.failed[0].error.code, "cross_device");
});

test("a closed connection, a refused send or a missing reply reject the call", async () => {
  const refused = fakeTransport({ fail: "Request too large" });
  const client = new FilesClient(refused.transport, { limits });
  await assert.rejects(client.list("/"), (error) => error.code === "too_large");
  const gone = fakeTransport({ fail: "The SSH connection is closed" });
  await assert.rejects(
    new FilesClient(gone.transport, { limits }).list("/"),
    (error) => error.code === "closed",
  );
  const slow = fakeTransport();
  const timed = new FilesClient(slow.transport, { limits, timeoutMs: 5 });
  await assert.rejects(timed.list("/"), (error) => error.code === "timeout");
  const open = fakeTransport();
  const closing = new FilesClient(open.transport, { limits });
  const waiting = [closing.list("/a"), closing.list("/b"), closing.list("/c")];
  closing.close();
  for (const call of waiting)
    await assert.rejects(call, (error) => error.code === "closed");
  await assert.rejects(closing.list("/"), (error) => error.code === "closed");
});

test("paths join, split and display relative to the home folder", () => {
  assert.equal(joinPath("/", "etc"), "/etc");
  assert.equal(joinPath("/home/me", "a b"), "/home/me/a b");
  assert.equal(parentPath("/home/me/a"), "/home/me");
  assert.equal(parentPath("/etc"), "/");
  assert.equal(parentPath("/"), "/");
  assert.equal(baseName("/home/me/a.txt"), "a.txt");
  assert.equal(baseName("/"), "/");
  assert.ok(isInside("/home/me/a", "/home/me"));
  assert.ok(isInside("/home/me", "/home/me"));
  assert.ok(!isInside("/home/meow", "/home/me"));
  assert.ok(isInside("/x", "/"));
  assert.equal(displayPath("/home/me", "/home/me"), "~");
  assert.equal(displayPath("/home/me/src", "/home/me"), "~/src");
  assert.equal(displayPath("/home/meow", "/home/me"), "/home/meow");
  assert.equal(displayPath("/etc", undefined), "/etc");
  assert.deepEqual(breadcrumbs("/home/me/src/app", "/home/me"), [
    { label: "~", path: "/home/me" },
    { label: "src", path: "/home/me/src" },
    { label: "app", path: "/home/me/src/app" },
  ]);
  assert.deepEqual(breadcrumbs("/etc/ssh", "/home/me"), [
    { label: "/", path: "/" },
    { label: "etc", path: "/etc" },
    { label: "ssh", path: "/etc/ssh" },
  ]);
  assert.deepEqual(breadcrumbs("/", "/home/me"), [{ label: "/", path: "/" }]);
});

test("names and moves are checked before they are sent", () => {
  assert.equal(nameProblem("notes.txt"), null);
  assert.equal(nameProblem(".env"), null);
  for (const bad of ["", ".", "..", "a/b", "a\0b", "é".repeat(128)])
    assert.ok(nameProblem(bad), JSON.stringify(bad));
  assert.equal(moveProblem("/home/me/a", "/home/me/docs"), null);
  assert.ok(moveProblem("/home/me/a", "/home/me"), "same folder");
  assert.ok(moveProblem("/home/me/docs", "/home/me/docs/sub"), "into itself");
  assert.ok(moveProblem("/home/me/docs", "/home/me/docs"), "onto itself");
});

test("paths typed into a terminal are shell-quoted and never carry control characters", () => {
  assert.equal(typedPaths(["/home/me/a.txt"]), "/home/me/a.txt");
  assert.equal(
    typedPaths(["/home/me/it's here", "/tmp/x y"]),
    "'/home/me/it'\\''s here' '/tmp/x y'",
  );
  assert.equal(typedPaths(["/home/me/a\nrm -rf ~"]), null);
  assert.ok(!pastablePath("/a\u001b[2J"));
  assert.ok(pastablePath("/home/me/é ü"));
});

test("entries sort folders first, by natural name, hiding dot files unless asked", () => {
  const entry = (name, kind = "file") => ({ name, kind });
  const entries = [
    entry("b10"),
    entry("B2"),
    entry(".hidden"),
    entry("zeta", "dir"),
    entry(".config", "dir"),
    entry("alpha", "dir"),
    entry("a"),
  ];
  assert.deepEqual(
    sortEntries(entries).map((e) => e.name),
    ["alpha", "zeta", "a", "B2", "b10"],
  );
  assert.deepEqual(
    sortEntries(entries, { hidden: true }).map((e) => e.name),
    [".config", "alpha", "zeta", ".hidden", "a", "B2", "b10"],
  );
});

test("languages are chosen by file name or extension", () => {
  assert.equal(languageFor("/x/app.tsx"), "tsx");
  assert.equal(languageFor("/x/main.rs"), "rust");
  assert.equal(languageFor("/x/README.md"), "markdown");
  assert.equal(languageFor("/x/Dockerfile.dev"), "dockerfile");
  assert.equal(languageFor("/x/.zshrc"), "shell");
  assert.equal(languageFor("/x/Cargo.lock"), "toml");
  assert.equal(languageFor("/x/config.yml"), "yaml");
  assert.equal(languageFor("/x/notes"), "text");
  assert.equal(languageFor("/x/.env"), "text");
  assert.equal(languageFor("/x/data.unknown"), "text");
});

test("documents load, save and turn a conflict into a choice", () => {
  const content = (text, extra = {}) => ({
    path: "/a",
    size: text.length,
    modified: 1,
    mode: 420,
    hash,
    binary: false,
    text,
    ...extra,
  });
  let doc = newDoc("/a");
  assert.equal(doc.status, "loading");
  assert.equal(canSave(doc), false);
  doc = loaded(doc, content("one"), 1024);
  assert.equal(doc.status, "ready");
  assert.equal(doc.saved, "one");
  assert.equal(doc.version, 1);
  assert.equal(doc.readOnly, "");
  assert.ok(canSave(doc));
  doc = { ...doc, dirty: true, status: "saving" };
  // The buffer changed again while the write was in flight: still dirty.
  const after = saved(doc, "two", { hash: "c".repeat(64), size: 3, modified: 2 }, "three");
  assert.equal(after.status, "ready");
  assert.equal(after.hash, "c".repeat(64));
  assert.equal(after.saved, "two");
  assert.equal(after.dirty, true);
  assert.equal(
    saved(doc, "two", { hash, size: 3, modified: 2 }, "two").dirty,
    false,
  );
  const conflict = saveFailed(
    doc,
    new FileError({ code: "conflict", message: "changed" }),
  );
  assert.equal(conflict.status, "conflict");
  assert.ok(canSave(conflict), "overwrite remains possible");
  const failed = saveFailed(doc, new FileError({ code: "no_space", message: "disk full" }));
  assert.equal(failed.status, "ready");
  assert.equal(failed.error, "disk full");
  // Binary and oversized files get an information view, large text opens
  // read-only.
  assert.equal(
    loaded(newDoc("/b"), content("", { binary: true, text: undefined }), 1024).status,
    "binary",
  );
  const big = loadFailed(
    newDoc("/c"),
    new FileError({ code: "too_large", message: "x", size: 9e9 }),
  );
  assert.equal(big.status, "too_large");
  assert.equal(big.size, 9e9);
  assert.equal(loadFailed(newDoc("/d"), Error("gone")).status, "error");
  const readOnly = loaded(newDoc("/e"), content("x".repeat(20)), 10);
  assert.match(readOnly.readOnly, /larger than 10 bytes/);
  assert.equal(canSave(readOnly), false);
  assert.equal(saveProblem("é".repeat(6), 12), "");
  assert.match(saveProblem("é".repeat(7), 12), /larger than 12 bytes/);
  assert.match(saveProblem("x".repeat(3 * 2 ** 20), 2 * 2 ** 20), /2 MiB/);
});

test("a file write may exceed the ordinary request limits, nothing else may", () => {
  const write = { Stream: { type: "file_write", request: "file-1" } };
  const input = { Stream: { type: "input", pane: 1 } };
  assert.equal(payloadLimit(input), requestLimit);
  assert.equal(payloadLimit(write), fileWriteLimit);
  assert.ok(fileWriteLimit >= 6 * fileContentLimit);
  assert.ok(fileWriteLimit < 16 * 2 ** 20, "within one stream record");
  assert.ok(queueLimit(write) > queueLimit(input));
});

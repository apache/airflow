<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 4. Self-contained executable bundle (footer-embedded source and metadata)

Date: 2026-05-04

## Status

Accepted. Supersedes the ZIP-archive container portion of
[ADR 0001](0001-bundle-packing-options.md) and the ZIP output sketched
in [ADR 0002](0002-use-go-tool-directive-for-bundle-packer.md). The
packer mechanism (Option A standalone packer + Option D introspection
contract + Option H `tool` directive) is unchanged; only the artefact
the packer writes is changed.

## Context

ADR 0001 / ADR 0002 picked a ZIP archive as the bundle container,
following the executable provider's existing
[bundle spec](../../providers/sdk/executable/docs/bundle-spec.rst).
A conforming bundle today is `bundle.zip` with three required entries:
`airflow-metadata.yaml`, the primary DAG source file, and the compiled
executable.

That layout has three properties we want to preserve:

1. **Discovery without execution.** The scanner must be able to read
   `dag_id` / `task_id` and the SDK language/version from a bundle on
   disk without running the binary. ADR 0002 already enforces this —
   `airflow-go-pack` runs the binary once at build time, captures its
   `--dump-bundle-spec` output into the manifest, and the scanner reads
   the manifest at deploy time.
2. **Source available for the UI.** The Airflow UI's source-view
   panel needs to render the DAG file. The current spec ships it as a
   verbatim ZIP entry referenced by the manifest's `source` field.
3. **Single deployment unit.** Drop one file in
   `[executable] bundles_folder` and the scanner picks it up.

What the ZIP container costs us:

- **Two artefacts in flight.** `go build` produces a binary; the
  packer wraps it into a ZIP. Anything that touches the binary after
  it is wrapped (re-strip, re-sign, swap-in a debug build) drifts from
  the manifest unless the wrapping is redone. The wrapping step is
  cheap but the drift mode is real.
- **A second container format on the consumer side.** The scanner
  must open archives, find members by name, and materialise the
  executable into a transient cache before the runtime can exec it.
  That is `archive/zip` on the Python side plus a per-bundle cache
  directory.
- **Inspection requires a different tool than running.** `unzip` to
  inspect, then run; or run, then `unzip` to debug. Two muscle memories.

Native-executable SDKs (Go, Rust, C++, Zig) all produce a single
self-contained binary by static linking. The binary itself is already
the only thing that has to land on the worker host to run a task. The
manifest and the source file are small data the scanner needs but the
runtime doesn't. Both can ride along in a footer appended to the
binary, with the binary remaining a runnable executable.

This is the same pattern self-extracting installers, `goreleaser`-style
self-update images, and embedded-asset binaries already use: append
data after the OS-recognised binary structure, leave a fixed-size
trailer at the very end so a reader can locate the data, and validate
with a magic value.

The user-facing claim becomes "the executable *is* the bundle." A
bundle directory looks like:

```
/opt/airflow/executable-bundles/
├── example
├── pipeline
└── analytics
```

(Filenames follow OS conventions: no extension on Linux/macOS, `.exe`
on Windows. The scanner identifies bundles by the trailer's magic, not
by the filename.)

## Decision

Replace the bundle's ZIP container with a footer appended to the
compiled executable. The executable's normal byte content is unchanged
and it remains directly runnable; the footer is data that follows the
last byte the OS loader cares about.

### Footer layout

A bundle file is laid out as:

```text
+---------------------------------+
| <native executable: ELF/Mach-O/PE,                                |
|  including any code-signing structures>                           |
+---------------------------------+   <- end of "binary" region
| source bytes (variable length)  |   raw root source file, UTF-8,
|                                 |   length = source_len; MAY be 0
+---------------------------------+
| metadata bytes (variable length)|   airflow-metadata.yaml content,
|                                 |   UTF-8, length = metadata_len
+---------------------------------+
| trailer (32 bytes, little-endian fixed layout):                   |
|   bytes  0..3  source_len    u32                                  |
|   bytes  4..7  metadata_len  u32                                  |
|   bytes  8..11 footer_ver    u32  (= 1)                           |
|   bytes 12..23 reserved      12 bytes, zero                       |
|   bytes 24..31 magic         8 bytes ASCII "AFBNDL01"             |
+---------------------------------+   <- EOF
```

`AFBNDL01` is `0x41 0x46 0x42 0x4E 0x44 0x4C 0x30 0x31`. The two
trailing ASCII digits are the footer-format version, repeated for human
inspection (`tail -c 8 ./mybundle | xxd`); the binary `footer_ver`
field is the source of truth for parsing.

Reader algorithm:

1. Open the file. Seek to `EOF - 32`. Read 32 bytes.
2. Compare bytes 24..31 against `AFBNDL01`. If different, the file is
   not a bundle; the scanner ignores it.
3. Parse `footer_ver`. If unknown, fail with a versioning error.
4. Compute `metadata_start = filesize - 32 - metadata_len` and
   `source_start  = metadata_start - source_len`.
5. Read `metadata_len` bytes from `metadata_start` for the manifest.
6. Read `source_len` bytes from `source_start` for the source view.
   If `source_len == 0`, no source is embedded; the UI falls back to
   "(source not available)".
7. Validate that `source_start >= 0` and that the implied "binary
   region" (bytes `[0, source_start)`) is non-empty.

Ordering note: source comes *before* metadata so a future
`format_version` can introduce extra trailing blobs (e.g. signed
checksums, compressed deps) by extending the trailer rather than
inserting between existing blobs.

### Manifest schema changes

The manifest content is the same YAML as today, with two field-level
changes that follow from the footer container:

- **Drop `executable`.** The binary *is* the file; there is no
  archive-relative path to record.
- **Redefine `source` as a display filename, not a path.** The source
  bytes live in the footer; the manifest's `source` field carries the
  original filename (e.g. `example.go`) so the UI can show it as a
  filename in the source-view panel and pick a syntax-highlighting
  mode from the extension.

Everything else (`format_version`, `sdk.language`, `sdk.version`,
`dags`, the open-additivity rule for unknown keys) is unchanged.

### Build pipeline

The packer's behaviour from ADR 0002 changes only at the final write
step:

1. Resolve target package, locate the file with `func main()`. (No
   change.)
2. Run `go build [forwarded flags] -o <out> <pkg>`. (No change.)
3. Exec the freshly built binary with `--dump-bundle-spec` to obtain
   the manifest. (No change.)
4. **New:** read the source file's bytes; serialise the manifest to
   YAML; append `<source><metadata><trailer>` to `<out>`.
5. Default output path becomes `<bundleName>` (or `<bundleName>.exe`
   on Windows), not `<bundleName>.zip`.

Ordering against post-build steps:

- **Strip:** must run *before* append. Stripping a file that already
  has a footer either leaves the footer intact (most strip
  implementations stop at the OS-defined end of the binary) or
  truncates it; do not rely on either.
- **Code-sign:** must run *after* append on platforms whose signature
  covers the entire file (Linux dm-verity, macOS post-Big-Sur for
  certain notarisation flows, Windows Authenticode). The signature
  then attests to the footer's contents along with the binary, which
  is the property we want.
- **Compressors (UPX, etc.):** unsupported. UPX rewrites the file end
  to end, destroying the trailer. Bundle binaries should not be
  compressor-wrapped; this matches typical production deployment
  practice.

Determinism: the footer is byte-identical for byte-identical inputs
(source bytes, manifest YAML, layout), so a deterministic `go build`
plus a deterministic manifest serialisation produces a byte-identical
bundle file. We canonicalise the manifest as sorted-key YAML at write
time to avoid map-order non-determinism on the Go side.

### Cross-language scope

The bundle spec is language-agnostic by design. Every native-SDK
language we currently target (Go, Rust, C++, Zig) emits a single
statically-linked native executable; appending a fixed-format footer
is a few lines of code in each. The footer layout above is the
contract every SDK packer implements; the consumer-side scanner reads
it identically regardless of source language.

Interpreted languages without a single binary artefact are out of
scope for the executable provider and therefore for this ADR.

### Consumer-side changes

The scanner currently iterates `*.zip` in `bundles_folder` and opens
each as an archive. It now iterates *all* regular files, reads the
last 32 bytes of each, and treats files whose magic matches as
bundles. Files without the magic are silently ignored (so a stray
README in the directory does not fail the scan).

The runtime no longer has to materialise an executable from an
archive. It execs the bundle file directly, which removes the
transient cache directory and the chmod-after-extract step from
the spec.

## Consequences

### What this buys

- **One artefact.** No `.zip` wrapper around a binary; the binary is
  the deployment unit. `cp ./mybundle /opt/airflow/executable-bundles/`
  is the deploy workflow.
- **No drift between binary and manifest.** They are produced and
  committed in the same step and physically attached.
- **Atomic deploy.** A partially written file fails the magic check;
  the scanner skips it cleanly instead of seeing half a manifest.
- **Smaller consumer surface.** No `archive/zip` dependency, no
  per-bundle cache directory, no chmod-after-extract path, no
  external-attributes handling for the executable bit.
- **Simpler runtime.** Exec the file directly.

### What this costs

- **Inspection needs a tool.** With ZIP, `unzip -p bundle.zip
  airflow-metadata.yaml` worked from any shell. With the footer
  format, ops needs a small CLI (`go tool airflow-go-pack inspect
  ./mybundle` or equivalent) to dump the manifest and source. Cheap
  to implement; the obligation is "ship it alongside the packer."
- **Build pipeline ordering matters.** Strip-then-append-then-sign is
  the only correct order. Documented in the packer and in this ADR;
  failure modes (stripped trailer, signature over the wrong bytes)
  are loud (magic check fails, signature verification fails) rather
  than silent.
- **Compressor incompatibility.** UPX and similar are not supported
  for bundle binaries. Acceptable; production deployments do not
  typically compress executables this way.
- **Magic-collision handling.** A non-bundle file in
  `bundles_folder` whose last 8 bytes happen to be `AFBNDL01` would
  be picked up as a bundle. Probability is negligible for a fixed
  8-byte ASCII string, and the next parse step (`footer_ver` check,
  bounds check, YAML parse) catches a false positive cleanly. Not
  worth a checksum in v1.
- **Footer format is now a wire format the SDK has to keep stable.**
  `footer_ver = 1` is the only currently defined value; future
  versions append fields after the version field but before the
  reserved region, or use the reserved region. Older readers reject
  unknown `footer_ver` rather than guessing.

### Out of scope

- Signed checksums in the footer. We rely on platform code-signing
  (Authenticode, codesign, dm-verity) for tamper detection. A
  bundle-level checksum could be added in a future `footer_ver` if
  signing-free tamper detection becomes a requirement.
- Multiple source files. Only the root file (the file containing
  `func main()`) is embedded. DAGs split across multiple source
  files keep the rest of their sources outside the bundle; the UI
  source-view shows only the entry file. Revisit if user feedback
  requests broader source visibility.
- Compression of the source/metadata blobs. Both are tiny (kilobytes)
  next to the binary; deflating them adds reader complexity for no
  measurable space win.

## Implementation notes

- The append step is `os.OpenFile(out, O_RDWR|O_APPEND, 0)` plus three
  writes (source, metadata, trailer) followed by `Close`. No mmap
  needed.
- The executable bit on the output file is set by `go build` itself.
  The append step preserves it (we write through, not truncate).
- The packer's existing reproducibility guarantees (sorted entries,
  fixed mtimes) reduce to "write a deterministic YAML manifest"; the
  ZIP-specific concerns (entry ordering, entry mtimes, external
  attributes) go away.
- The Python-side scanner's bundle-detection helper lives next to
  `BundleScanner`; it reads 32 bytes per file and parses YAML for
  matching files. Keep it tolerant of trailing whitespace or short
  files (anything `< 32` bytes is not a bundle).

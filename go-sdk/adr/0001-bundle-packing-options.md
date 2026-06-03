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

# 1. Post-build bundle-packing options for the Go SDK

Date: 2026-04-30

## Status

Accepted as the option register. The packer-mechanism decision is
recorded in [ADR 0002](0002-use-go-tool-directive-for-bundle-packer.md):
Option H (Go 1.24 `tool` directive) for delivery, paired with Option A
(standalone `airflow-go-pack` binary) and Option D (standardised
`--airflow-metadata` introspection contract — the single metadata flag
that prints the bundle's `airflow-metadata.yaml` spec as JSON, which
`airflow-go-pack` reads to populate the manifest). The shipped runtime
([`bundlev1server.Serve`](../bundle/bundlev1/bundlev1server/server.go))
routes through a `decideMode` switch with three modes —
`--airflow-metadata`, `--comm`/`--logs` (coordinator mode), and the
default go-plugin path.

The container-format assumption running through this ADR — that the
output is a ZIP archive — is superseded by
[ADR 0004](0004-self-contained-executable-bundle.md), which embeds the
source and manifest in a footer appended to the executable. The
options below still describe valid *packer mechanisms*; only the
artefact each one writes has changed from a ZIP to a footer-augmented
executable.

## Context

The executable provider's bundle spec
([`task-sdk/docs/executable-bundle-spec.rst`](../../task-sdk/docs/executable-bundle-spec.rst))
defined the deployment artifact, *at the time this ADR was written*, as a
ZIP archive containing:

1. `airflow-metadata.yaml` declaring `airflow_bundle_metadata_version`, `sdk`
   (`language`, `version`, `supervisor_schema_version`), `source`
   (archive-relative path to the DAG source file), `executable`
   (archive-relative path to the compiled binary), and `dags` (a mapping of
   `dag_id` to `{tasks: [task_id, ...]}`). The shipped spec replaced the ZIP
   with a footer-augmented executable (see
   [ADR 0004](0004-self-contained-executable-bundle.md)): it dropped the
   `executable` field (the binary *is* the file) and redefined `source` as a
   display filename rather than an archive-relative path. The manifest keys
   above are otherwise unchanged.
2. The primary DAG source file, included verbatim.
3. The compiled native executable, which speaks the coordinator protocol
   (`--comm=<addr>` / `--logs=<addr>`).

Bundle authors today produce the executable with a plain `go build`
(see [`go-sdk/example/bundle/Justfile`](../example/bundle/Justfile)). There is
no SDK-provided way to produce the conforming ZIP, so each author would need
to hand-roll one.

The bundle binary already exposes a `--airflow-metadata` flag (defined in
[`bundle/bundlev1/bundlev1server/server.go`](../bundle/bundlev1/bundlev1server/server.go))
that prints the `BundleInfo{Name, Version}` returned by the author's
`BundleProvider.GetBundleVersion()`. It does **not** currently invoke
`RegisterDags`, so it does not yet enumerate `dag_id` / `task_id` for the
manifest. This is relevant context: the binary itself is the authoritative
source of dag/task identity at runtime, and the SDK can extend the
introspection path cheaply.

The user's initial framing was `go build -toolexec`. `-toolexec` wraps each
toolchain invocation (compile, asm, link) and does not have visibility into
the final `-o` output path or a single "build finished" hook, so it is a poor
fit for producing the final ZIP. The options below cover the mechanisms that
do fit, plus the `-toolexec` variant for completeness.

A packing mechanism has two sub-decisions:

- **Where the packing logic runs.** In the bundle binary itself
  (self-pack), in a separate SDK CLI, or in build tooling outside the SDK
  (Makefile/Justfile snippet).
- **How dag/task IDs reach the manifest.** Runtime introspection of the
  built binary (call into `RegisterDags` against an in-memory
  registry recorder), static AST scan of the source file, or
  hand-written manifest.

The options below combine those two sub-decisions in different ways.

## Options

### Option A: Standalone SDK packer CLI (`airflow-go-pack`)

A new binary under `go-sdk/cmd/airflow-go-pack` that takes
already-built inputs and writes the ZIP:

```
airflow-go-pack \
    --source ./example/bundle/main.go \
    --executable ./bin/example-dag-bundle \
    --output ./bin/example.zip
```

Manifest population: the packer execs the supplied executable with
`--airflow-metadata` and reads the JSON from stdout to fill `sdk`
(`language`, `version`, `supervisor_schema_version`) and to enumerate
`dags`. Source language is hard-coded to `go`; SDK version is read from the
build info embedded in the binary or from a build-time `-ldflags` value.

- **Pros:** simple, single-purpose binary; works against any binary the user
  built however they like (CGO, cross-compile, custom `-ldflags`); no
  coupling to `go build` invocation; trivially callable from `just`,
  `make`, CI, or `go generate`.
- **Cons:** two-step UX (`go build` then `airflow-go-pack`); user has to
  install or `go run` the tool; nothing prevents pack/build mismatch
  (e.g. packing yesterday's binary).

### Option B: All-in-one SDK CLI with a `build` subcommand

A single SDK CLI (`airflow-go`) with subcommands that wrap `go build` and
then pack:

```
airflow-go build ./example/bundle --output ./bin/example.zip
```

Internally: spawn `go build -o <tmp>/bundle <pkg>`, then run the same
introspection step as Option A, then write the ZIP.

- **Pros:** single command; no chance of pack/build skew; easy to add
  related subcommands later (`airflow-go new`, `airflow-go run`,
  `airflow-go validate`); good defaults for `-ldflags` (e.g.
  `-X main.bundleVersion=...`) without the author having to know them.
- **Cons:** the SDK now owns a `go build` wrapper and inherits
  responsibility for forwarding the long tail of `go build` flags
  (`-tags`, `-trimpath`, `GOOS`/`GOARCH` env, `-ldflags` passthrough,
  `-buildvcs`, etc.); harder to integrate with non-trivial existing build
  systems that already drive `go build` themselves.

### Option C: Self-packing binary (`--pack-bundle <out.zip>`)

Extend `bundlev1server.Serve` so that when the binary is invoked with
`--pack-bundle <out.zip>`, it builds the ZIP itself: it knows its own
executable path (`os.Executable()`), its embedded source (via `//go:embed`
of the DAG source file at build time), and its dag/task list (by
calling `RegisterDags` against an in-memory recorder). After writing
the archive, it exits.

- **Pros:** zero extra tools; the binary is fully self-describing; pack
  output is provably consistent with the binary's runtime behaviour.
- **Cons:** requires the author's `main` package to embed its own source
  (`//go:embed main.go` or similar), which is awkward when the DAG is
  spread across multiple files or the source path is non-obvious;
  bloats every bundle binary with packing code and an embedded copy of
  the source; mixes build-time concerns into a runtime entrypoint.

### Option D: Two-phase external introspection (introspection binary + packer)

Same shape as Option A or B, but standardise the introspection contract:
the SDK guarantees that every bundle binary supports a single
`--airflow-metadata` flag which prints a JSON blob containing
`sdk.language`, `sdk.version`, `sdk.supervisor_schema_version`, and the
full `dags` mapping. The packer's only job is to combine that JSON, the
source file path the user passes in, and the binary itself into a bundle.

This is really a refinement of A/B that fixes the introspection contract
in the SDK protocol, rather than an independent option, but is worth
calling out because the shape of the introspection flag is itself a
decision (single flag vs. several; JSON vs. YAML; pretty vs. compact).
The SDK settles that sub-decision in favour of a *single*
`--airflow-metadata` flag.

- **Pros:** decouples "how do we enumerate dags" from "how do we ZIP";
  any future packer (third-party CI plugin, IDE, etc.) can rely on the
  same contract; trivial to unit-test.
- **Cons:** locks in a wire format the SDK has to keep stable; slightly
  more code in the bundle binary than today; exec-based introspection
  requires a host-runnable binary, so cross-compile targets (and
  `--executable` paths that hand the packer a pre-built cross-target
  binary) force the packer to produce a host-arch sidecar purely to
  run `--airflow-metadata`. See [ADR 0002](0002-use-go-tool-directive-for-bundle-packer.md)
  for the pipeline.

### Option E: Static AST scan, no introspection

Parser-only packer: walk the DAG source AST, find `dagbag.AddDag("X")`
calls and the `.AddTask(fn)` calls chained off them, and synthesise the
manifest without running the binary.

- **Pros:** no runtime dependency on the binary (works even if it
  doesn't build for the host platform, e.g. cross-compiled for Linux on
  a macOS dev box); fast.
- **Cons:** brittle to anything dynamic (`for _, name := range names {
  dagbag.AddDag(name) }`, helper functions, generated code); the SDK
  becomes the second source of truth for dag/task identity, which can
  drift from `RegisterDags`; users will hit "I added a DAG and the
  packer didn't see it" failures.

### Option F: `go generate` directive

Document a recommended `//go:generate` line in the author's `main.go`:

```go
//go:generate go run github.com/apache/airflow/go-sdk/cmd/airflow-go-pack ...
```

`go generate` is then the build-time entrypoint.

- **Pros:** stdlib-blessed mechanism; no new tool installation needed
  (`go run` fetches the packer from the module cache); discoverable from
  the source file itself.
- **Cons:** `go generate` has to be run explicitly; users frequently
  forget; doesn't actually pack the *binary*, only triggers a tool that
  must still build and pack it; fits awkwardly because the natural
  ordering is `pack` -> `build`, not `build` -> `pack`.

### Option G: `go build -toolexec` wrapper

Provide a binary that the user passes as
`go build -toolexec=airflow-go-toolexec ...`. The wrapper proxies every
toolchain call and, on detecting the final `link` invocation, copies the
linker's output path, then runs the packing step against it.

- **Pros:** single `go build` invocation; nominally fits into existing
  `go build` workflows.
- **Cons:** `-toolexec` was not designed for this. It receives the
  toolchain executable path and an argv that varies across Go versions;
  the wrapper has to parse the linker's `-o` to discover the binary
  location, which is undocumented/internal behaviour and changes
  between releases. It also runs once per package compile, so the
  wrapper must distinguish "real" link invocations from intermediate
  ones. Operationally fragile; recommended against.

### Option H: `go.mod` `tool` directive (Go 1.24+)

Register the packer in the consuming project's `go.mod` via the
`tool` directive and invoke it as `go tool airflow-go-pack`. This is
orthogonal to A/B/D (it's a *delivery* mechanism, not a different
implementation) but is worth listing because it changes the install
story significantly.

- **Pros:** version-pinned per project; no `uv tool`-style global install;
  works the same in every checkout; aligns with `breeze`'s direction
  ([ADR 0017](../../dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md)
  for the Python side).
- **Cons:** requires Go 1.24 in the consumer's toolchain; one more line
  the author has to add to `go.mod`.

### Option I: Build-system recipe only (no SDK code)

Ship a documented `Justfile` / `Makefile` / `Taskfile` snippet that
sequences `go build` -> `zip` -> manifest write, and let users copy it
into their projects. The SDK provides only the spec and an example.

- **Pros:** zero new code in the SDK; users see exactly what is
  happening.
- **Cons:** every project re-implements (and slowly diverges from) the
  spec; manifest generation in shell is painful; no introspection of
  dag/task IDs without re-running the binary anyway, so the recipe
  ends up calling some helper, which is just Option A by another name.

## Cross-cutting sub-decisions

These apply to whichever top-level option is chosen:

1. **Manifest source of truth.** Runtime introspection (D) is the only
   approach that cannot drift from `RegisterDags`. Everything else
   trades that guarantee for some other property (no binary needed,
   no extra flag, etc.).
2. **Source-file discovery.** The spec requires the source file path
   to appear in the manifest's `source` field and the file itself to
   be present in the ZIP. The packer needs either (a) an explicit
   `--source` argument, (b) a convention (e.g. the `main.go` of the
   `main` package being built), or (c) a `//go:embed`-d copy inside
   the binary (Option C).
3. **SDK version reporting.** The bundle binary should expose the SDK
   version it linked against. This can come from `runtime/debug.ReadBuildInfo`
   walking the deps for `github.com/apache/airflow/go-sdk`, or from
   a build-time `-ldflags -X` value the SDK documents.
4. **Reproducibility.** The ZIP should be deterministic for a given
   set of inputs (sorted entries, fixed mtimes, no host-specific
   metadata) so two builds of the same bundle hash identically. This
   is independent of which option is picked but easiest to enforce
   inside SDK-owned code (A/B/C/D) than in a shell recipe (I).
5. **Executable bit.** The spec says the archive entry SHOULD preserve
   the executable bit via the ZIP external-attributes field. The
   packer must set `0755` (or similar) on the executable entry; this
   is trivial in Go's `archive/zip` but easy to get wrong in shell.

## Decision

Recorded in [ADR 0002](0002-use-go-tool-directive-for-bundle-packer.md).
Summary: deliver the packer via the Go 1.24 `tool` directive (Option H);
implement it as a standalone binary at `cmd/airflow-go-pack` (Option A);
populate the manifest by execing the bundle binary with the standardised
`--airflow-metadata` introspection flag (Option D).

## Consequences

Listing the options here, rather than only landing the chosen one,
keeps the rejected alternatives discoverable for future SDKs (Rust,
C++, Zig) which will face the same question, and documents why
`-toolexec` and AST-only scanning were considered and dropped.

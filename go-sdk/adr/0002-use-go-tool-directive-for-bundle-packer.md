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

# 2. Use the Go 1.24 `tool` directive to deliver the bundle packer

Date: 2026-04-30

## Status

Accepted. Selects from the option register in
[ADR 0001](0001-bundle-packing-options.md).

The output-format portion of this ADR (the packer writes a ZIP archive
following the bundle spec) is superseded by
[ADR 0004](0004-self-contained-executable-bundle.md): the packer now
writes a self-contained executable with an appended footer carrying
the source bytes and the manifest. The packer's *mechanism* (Option
A standalone binary + Option D introspection contract + Option H
`tool` directive) is unchanged. The decision sketches below mention
ZIP output; read them with the ADR 0004 substitution in mind, and
treat ADR 0004 as authoritative wherever the two disagree.

## Context

[ADR 0001](0001-bundle-packing-options.md) enumerated nine candidate
mechanisms for producing a conforming bundle ZIP
([`task-sdk/docs/executable-bundle-spec.rst`](../../task-sdk/docs/executable-bundle-spec.rst))
from a Go SDK build. Two reasons drive the choice:

1. **The repository already requires Go 1.24.** `go-sdk/go.mod` declares
   `go 1.24.0` with `toolchain go1.24.6`, so language features added in
   1.24 are available to every consumer of the SDK by construction.
2. **Contributors expect Go-native workflows.** The Go 1.24 `tool`
   directive is the toolchain's native answer to "depend on a
   build-time CLI without polluting the global PATH." It pins the tool
   version per-project in `go.mod`, resolves it through the standard
   module cache, and exposes it as `go tool <name>`, with no extra
   installer and no per-worktree drift. The same problem on the Python
   side led `breeze` to switch to `uvx` in
   [ADR 0017](../../dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md);
   `tool` is the analogous answer here.

The `tool` directive is a delivery mechanism. It still needs an
underlying implementation. We pair it with two of the implementation
options from ADR 0001, with a UX twist:

- **Option A (standalone packer):** a single-purpose binary at
  `go-sdk/cmd/airflow-go-pack`. It still operates as one process with
  a clear input/output contract, but it drives `go build` internally
  by default so that the common case is one command:
  `go tool airflow-go-pack ./pkg`. Authors who already produce their
  own binary can opt out via `--executable <path>` and skip the build
  phase. This is closer to Option B's ergonomics than the original
  ADR 0001 sketch, but the packer never interprets `go build` flags
  itself — arbitrary flags pass through verbatim after the `--`
  separator, so the SDK's flag surface stays small and stable as
  `go build` evolves.
- **Option D (standardised introspection contract):** every bundle
  binary supports a `--airflow-metadata` flag that prints JSON
  containing `sdk.language`, `sdk.version`, `sdk.supervisor_schema_version`,
  and the full `dags` mapping. The packer execs the freshly built binary
  with this flag to populate the manifest, which keeps `RegisterDags` as
  the single authoritative source of dag/task identity.

Options C, E, F, G, and I from ADR 0001 are rejected for the reasons
recorded there. Option B (a full `airflow-go build` wrapping `go
build`) is rejected as a separate top-level command, but its core
ergonomic win (one command for the common case) is folded into the
Option A packer through default behaviour, with a `--` passthrough
convention so authors can forward arbitrary flags to the underlying
`go build` without the SDK having to enumerate them.

## Decision

1. **Add `cmd/airflow-go-pack` to the go-sdk module.** Its default
   invocation is one command:

   ```sh
   go tool airflow-go-pack [./path/to/pkg] [-- <go build flags>...]
   ```

   The single positional argument is the Go package containing the
   bundle's `main` package (defaults to `.`, the current directory).
   Anything after `--` is forwarded verbatim to the internal
   `go build` invocation, so authors keep full control over `-tags`,
   `-trimpath`, `-ldflags`, `GOOS`/`GOARCH` (via env), `-buildvcs`,
   etc. without the packer having to enumerate them.

   With no further flags, the packer:

   1. Resolves the target package and locates the file in that
      package that defines `func main()`. That file becomes the
      manifest's `source` and is copied verbatim into the ZIP. If
      `main` is split across multiple files, the packer errors and
      asks the author to specify `--source <file>`.
   2. Runs `go build [forwarded flags] -o <tmpdir>/<binname> <pkg>`
      to produce the *target artifact*.
   3. Executes a *host-runnable introspection binary* with
      `--airflow-metadata` to obtain `sdk.{language,version,supervisor_schema_version}`
      and the `dags` mapping. The packer first tries to exec the target
      artifact directly; if that fails with "exec format error" (the
      target is built for a different OS/arch than the host), the
      packer builds a host-arch sidecar from the same package
      (`go build` with `GOOS`/`GOARCH` unset, written to
      `<tmpdir>/<binname>.introspect`) and execs that instead. Both
      binaries are produced from the same source package against the
      same module graph, so `RegisterDags` records identical dag/task
      identity regardless of which one is execed.
   4. Writes a deterministic ZIP next to the working directory at
      `<bundleName>.zip`, where `<bundleName>` comes from the
      binary's `BundleInfo.Name` (part of the same `--airflow-metadata`
      introspection output).

   Optional overrides, for advanced or pre-built workflows:

   - `--source <path>`: override the auto-detected source file.
   - `--executable <path>`: skip the internal `go build` and pack a
     pre-built binary. Mutually exclusive with `--` build-flag
     passthrough. If the supplied binary is not host-runnable (e.g.
     the user cross-built a `linux/amd64` binary from a `darwin/arm64`
     host), the packer still needs to introspect it: it builds a
     host-arch sidecar from the positional package and execs that for
     `--airflow-metadata`, then appends the resulting footer to the
     user-supplied binary. If no positional package was passed and
     the supplied binary is not host-runnable, the packer errors with
     a message asking for the source package so the sidecar can be
     built.
   - `--output <path>`: override the default output ZIP path.

   Examples:

   ```sh
   # Common case: build and pack in one command from the package dir.
   go tool airflow-go-pack

   # Pack a different package, with extra go build flags.
   go tool airflow-go-pack ./cmd/my-bundle -- -trimpath -tags=prod

   # Pack an already-built binary (skips go build).
   go tool airflow-go-pack --executable ./build/example --source main.go
   ```

2. **Extend the existing `--airflow-metadata` flag in
   `bundlev1server.Serve` to print the full spec.** Rather than adding a
   second introspection flag, `--airflow-metadata` is the single flag the
   packer relies on; it prints a JSON document of the form:

   ```json
   {
     "airflow_bundle_metadata_version": "1.0",
     "sdk": {
       "language": "go",
       "version": "<sdk version>",
       "supervisor_schema_version": "<YYYY-MM-DD>"
     },
     "dags": {
       "<dag_id>": {"tasks": ["<task_id>", "..."]}
     }
   }
   ```

   `sdk.version` is read from `runtime/debug.ReadBuildInfo()` by
   walking deps for `github.com/apache/airflow/go-sdk`;
   `sdk.supervisor_schema_version` is the dated AIP-72 wire-schema
   version the bundle was compiled against. The `dags` mapping is
   populated by calling the bundle's `RegisterDags` against an in-memory
   recording registry, then enumerating the recorded dags and their
   tasks. `--airflow-metadata` today prints only `BundleInfo`
   (`server.go`); it is extended to emit this full document, so the
   shipped `decideMode` switch needs only one metadata mode. The bundle's
   `BundleInfo.Name` (used by the packer for the default output
   filename) is carried in the same output.

3. **Bundle authors register the packer in their own `go.mod`:**

   ```
   tool github.com/apache/airflow/go-sdk/cmd/airflow-go-pack
   ```

   and pack in one step:

   ```sh
   go tool airflow-go-pack
   ```

4. **Update `go-sdk/example/bundle/Justfile`** to demonstrate the
   recipe end-to-end, including the `tool` directive in the example's
   own `go.mod`.

## Consequences

- Per-project, per-worktree version pinning of the packer through
  `go.mod`. No global install. Two checkouts on different SDK versions
  pack with the right packer for each.
- Authors get a Go-native, single-command workflow for the common case
  (`go tool airflow-go-pack`), with a `--` passthrough escape hatch
  for arbitrary `go build` flags and `--executable` for pre-built
  workflows. CI and other build systems can use whichever shape fits.
- The `--airflow-metadata` JSON becomes a stable wire format that the
  SDK has to keep backward-compatible. It is versioned implicitly
  through the bundle spec's `airflow_bundle_metadata_version` field, so
  additive changes are safe.
- Third-party tooling (IDE plugins, alternative packers, CI plugins)
  can rely on the same introspection contract without taking a Go
  dependency on the SDK.
- The packer takes on responsibility for locating the `main` source
  file and choosing a sensible default output path. Both are
  heuristics; both are overridable. Drift between the heuristic and
  the spec is the main maintenance cost introduced by this option.
- Requires Go 1.24 in any consumer project. Already a project-wide
  assumption.

## Implementation notes

- The ZIP must be deterministic: sorted entries, fixed mtimes, no
  host-specific metadata, so two builds of identical inputs hash the
  same.
- The executable entry's external-attributes field must encode mode
  `0755` (or similar), per the bundle spec's executable-bit
  requirement.
- The packer should validate that the manifest's `dags` is non-empty
  and warn (not fail) on empty `tasks` lists, matching the bundle
  spec's "permitted but discouraged" wording.
- `--airflow-metadata` runs `RegisterDags` but must not start the
  gRPC server or contact any external services; the in-memory
  registry recorder is the only side effect.
- Source-file detection uses `go/packages` (or `go list -json`) to
  load the target package, then picks the file whose AST contains a
  top-level `func main()`. If the package has zero or more than one
  such file, the packer errors with a clear message and asks for
  `--source`.
- The internal `go build` runs in a temp directory so the host's
  working tree is not polluted with build artefacts; the temp dir is
  cleaned up after the ZIP is written.
- `go build` flag passthrough uses the standard `--` separator
  convention so the packer's own flag set stays small and stable.
- Host-runnable detection is by attempted exec, not by parsing the
  binary's exec format. The packer runs the candidate introspection
  binary with `--airflow-metadata` and treats the OS's "exec format
  error" (and the Windows equivalent surfaced by `os/exec`) as the
  signal to fall back to building a host-arch sidecar. Other exec
  failures (non-zero exit, malformed JSON, missing flag) are real
  errors and are surfaced to the user as-is. The Go build cache
  amortises the sidecar to a link step when host arch is already
  involved, so there is no measurable overhead when no cross-compile
  is in play.

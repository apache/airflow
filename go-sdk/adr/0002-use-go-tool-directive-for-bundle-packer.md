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
      and the `dags` mapping. When cross-compiling, the packer builds a
      host-arch sidecar from the same package and forwarded build flags
      and execs that instead; both come from the same sources and flags,
      so `RegisterDags` records identical identity either way. The sidecar
      is skipped when `--airflow-metadata` supplies the manifest directly
      (see the overrides below).
   4. Writes a self-contained bundle: the deployable executable with
      the source bytes, the manifest bytes, and the fixed 64-byte
      `AFBNDL01` trailer appended (the format from ADR 0004 /
      `task-sdk/docs/executable-bundle-spec.rst`). By default it is
      written next to the working directory under the bundle's main
      package directory name; `--output` overrides the path.

   Optional overrides, for advanced or pre-built workflows:

   - `--source <path>`: override the auto-detected source file, and the
     escape hatch when discovery can't pick it. It skips discovery
     entirely (a plain `go list` that ignores build tags and
     `GOOS`/`GOARCH`, so it can fail or pick the wrong file).
   - `--executable <path>`: skip `go build` and pack a pre-built binary.
     Mutually exclusive with `--` build flags and with `--goos`/`--goarch`
     (it never builds). The binary must run on the host so the packer can
     exec it for `--airflow-metadata`. A non-host-runnable binary is a hard
     error: the packer won't rebuild a host sidecar, because the original
     build inputs (tags, `-ldflags`, `GOOS`/`GOARCH` files) are unknown and
     a rebuild could advertise a different dag/task set than shipped. To
     pack for another platform, use the build path (`--goos`/`--goarch`,
     below) or supply `--airflow-metadata`.
   - `--airflow-metadata <path>`: supply a captured manifest instead of
     introspecting the binary. Accepts the binary's YAML default, its
     `--format json` output, or a bundle's embedded `airflow-metadata.yaml`
     (one YAML decoder reads all three). Short-circuits introspection in
     every mode — the deterministic way to pack a pre-built cross binary
     with `--executable`.
   - `--goos <os>` / `--goarch <arch>`: cross-compile the deployable bundle.
     Prefer these over the `GOOS`/`GOARCH` env vars: under `go tool` those
     env vars cross-build the packer itself, which then can't exec on the
     host. The flags target only the internal `go build`, leaving the
     packer build host-native; they fall back to the env vars, then the
     host. Mutually exclusive with `--executable`.
   - `--output <path>`: override the default output path. Its parent
     directory is created if missing.

   Examples:

   ```sh
   # Common case: build and pack in one command from the package dir.
   go tool airflow-go-pack

   # Pack a different package, with extra go build flags.
   go tool airflow-go-pack ./cmd/my-bundle -- -trimpath -tags=prod

   # Pack an already-built binary for THIS host (skips go build).
   go tool airflow-go-pack --executable ./build/example --source main.go

   # Pack a bundle for a different platform: cross-build via the build
   # path (no --executable), forwarding go build flags after "--".
   # Use --goos/--goarch, NOT the GOOS/GOARCH env vars, under `go tool`.
   go tool airflow-go-pack --goos linux --goarch amd64 ./cmd/my-bundle -- -trimpath
   ```

2. **Extend the existing `--airflow-metadata` flag in
   `bundlev1server.Serve` to print the full spec.** Rather than adding a
   second introspection flag, `--airflow-metadata` is the single flag the
   packer relies on; it prints a manifest document (YAML by default, JSON
   under `--format json`) of the form (shown here as JSON for structure):

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
   shipped `decideMode` switch needs only one metadata mode. The packer
   derives the default output filename from the bundle's main package
   directory name (what `go build` itself names the binary), resolved
   before the build so a bad `--output` fails fast; it does not come from
   `BundleInfo.Name`, and no name field is carried in this output.

   A `--format yaml|json` flag selects the encoding and is only valid with
   `--airflow-metadata` (misuse is a hard error). The default is YAML,
   matching a bundle's embedded `airflow-metadata.yaml`, so
   `mybundle --airflow-metadata > airflow-metadata.yaml` is ready to use.
   The packer never sets `--format`; its YAML decoder reads both the YAML
   default and `--format json` output (JSON is a subset of YAML).

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
  binary's format: the packer runs the candidate with `--airflow-metadata`
  and treats an "exec format error" (and its Windows equivalent) as
  not-startable. Other failures (non-zero exit, malformed output, missing
  flag) are surfaced as-is.
- A not-startable binary is handled differently per mode. The **build**
  path owns the build, so it builds a host-arch sidecar from the same
  package and `--` flags (the build cache amortises this to a link step,
  so no measurable overhead without a cross-compile). In **`--executable`**
  mode the build inputs are unknown, so the packer must not synthesise a
  sidecar that could diverge from the shipped binary: it fails fast and
  points the user at the cross-build path or `--airflow-metadata`.

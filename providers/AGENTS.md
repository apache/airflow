<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Providers — Agent Instructions

Each provider is an independent package with its own `pyproject.toml`, tests, and documentation.

## Structure

- `provider.yaml` — metadata, dependencies, and configuration for the provider.
- Building blocks: Hooks, Operators, Sensors, Transfers.
- Use `version_compat.py` patterns for cross-version compatibility.

## Checklist

- Keep `provider.yaml` metadata, docs, and tests in sync.
- Don't upper-bound dependencies by default; add limits only with justification.
- Tests live alongside the provider — mirror source paths in test directories.
- Full guide: [`contributing-docs/12_provider_distributions.rst`](../contributing-docs/12_provider_distributions.rst)

## Changelog — never use newsfragments

**Never create newsfragments for providers.** Providers are released from `main` in waves, so
per-PR newsfragments are not consumed by the release process — the release manager regenerates the
changelog from `git log`. The towncrier-managed `newsfragments/` workflow is used only by
`airflow-core/`, `chart/`, and `dev/mypy/`. (`airflow-ctl/` follows the same "no newsfragments,
direct edit" pattern as providers — see `dev/README_RELEASE_AIRFLOWCTL.md`.)

When a provider change needs a user-visible note (typically a breaking change or important behavior
change that warrants explanation), update the provider's `docs/changelog.rst` directly in the same
PR, just below the `Changelog` header — exactly as the in-file `NOTE TO CONTRIBUTORS` block
describes. Routine entries (features, bug fixes, misc) are collected automatically by the release
manager from commit messages, so most PRs do not need to touch the changelog at all.

## Security: Connection extras must not be forwarded blindly to hooks/operators

**Never pass the whole `Connection.extra` dict (or `**conn.extra_dejson`) as
keyword arguments to a hook, operator, client constructor, or any underlying
library call.** Forward only the specific extra keys you have explicitly
reviewed and know are safe to expose.

### Why this matters — different security boundaries

Airflow has two distinct user roles with different trust levels:

- **Connection editors** — UI/API users with permission to create or edit
  Connections. They control the host, login, password, and the `extra` JSON
  blob.
- **Dag authors** — users who write the Python code that constructs hooks
  and operators. They control which arguments are passed at call sites.

These are *not* the same population, and the security model treats them
differently. A Connection editor is trusted to supply credentials for a target
system; they are **not** trusted to alter how the worker process behaves, load
arbitrary Python code, change file paths the worker reads, or pass options
into client libraries that the Dag author did not opt into.

### What goes wrong when extras are forwarded blindly

Many client libraries accept constructor or method kwargs that are dangerous
when attacker-controlled. Concrete examples seen in the wild:

- A kwarg that names a Python callable, plugin path, or import string —
  attacker sets it to a module they control → **remote code execution** on
  the worker.
- A kwarg that points at a local file (cert path, key file, log path,
  config file) — attacker redirects it to read or overwrite worker-side
  files.
- A kwarg that sets a proxy, endpoint URL, or hostname — attacker
  redirects traffic to an MITM endpoint and harvests credentials or
  tokens for *other* systems.
- A kwarg that toggles TLS verification, signing, or auth — attacker
  silently downgrades security.
- A kwarg that controls subprocess execution, shell invocation, or
  template rendering — attacker reaches command/template injection.

In all of these, the Connection editor effectively gains capabilities
(RCE, file read/write, traffic redirection, auth bypass) that the security
model does not grant them.

### The rule

- **Allowlist, never passthrough.** In the hook, read each extra key by
  name (`conn.extra_dejson.get("region_name")`,
  `conn.extra_dejson.get("verify")`, …) and pass only those named values
  forward. Reject or ignore unknown keys.
- **Do not** write `SomeClient(**conn.extra_dejson)`,
  `hook = MyHook(**conn.extra_dejson)`, or
  `kwargs.update(conn.extra_dejson)` followed by a downstream call.
- When adding support for a new extra key, treat it like any other public
  argument: review what the underlying library does with it, and document
  it in the provider's connection docs.
- If a Dag author genuinely needs to pass a non-allowlisted option, that
  option should be a **Dag-author-supplied argument** on the operator or
  hook (with its own review), not something a Connection editor can set.

### When reviewing provider PRs

Flag any of these patterns:

- `**conn.extra_dejson` or `**self.extra_dejson` spread into a constructor
  or call.
- Looping over `conn.extra_dejson.items()` and forwarding every key.
- New code paths where extras are merged into `kwargs` and then passed on
  unfiltered.
- "Convenience" features that let users put arbitrary client kwargs into
  `extra` — they widen the Connection-editor blast radius.

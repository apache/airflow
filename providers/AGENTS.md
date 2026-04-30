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

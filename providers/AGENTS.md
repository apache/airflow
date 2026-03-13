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

 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# AGENTS instructions

The main developer documentation lives in the `contributing-docs` directory. The following points summarise
how to set up the environment, run checks, build docs and follow the PR workflow.

## Local virtualenv and Breeze

- [`07_local_virtualenv.rst`](contributing-docs/07_local_virtualenv.rst) explains how to prepare a local Python environment using `uv`. The tool creates and syncs a `.venv` and installs dependencies with commands such as `uv venv` and `uv sync`.
- [`06_development_environments.rst`](contributing-docs/06_development_environments.rst) compares the local virtualenv with the Docker based Breeze environment. Breeze replicates CI and includes services like databases for integration tests.

## Prek hooks

- Installation and usage of `prek` are described in [`03a_contributors_quick_start_beginners.rst`](contributing-docs/03a_contributors_quick_start_beginners.rst). Install with `uv tool install prek` and run checks via `prek --all-files`.
- [`08_static_code_checks.rst`](contributing-docs/08_static_code_checks.rst) provides more details on the available hooks and prerequisites. Enable the hooks with `prek install` so they run automatically on each commit.

## Running tests

- [`03a_contributors_quick_start_beginners.rst`](contributing-docs/03a_contributors_quick_start_beginners.rst) shows running tests inside Breeze. Use `pytest` inside the container for individual files or invoke `breeze testing` commands to run full suites, e.g. `breeze --backend postgres --python 3.10 testing tests --test-type All`.

## Building documentation

- Documentation can be built locally using `uv run --group docs build-docs` as described in [`11_documentation_building.rst`](contributing-docs/11_documentation_building.rst). Within Breeze the equivalent command is `breeze build-docs`.

## Pull request guidelines

- Follow the PR guidance in [`05_pull_requests.rst`](contributing-docs/05_pull_requests.rst). Always add tests, keep your branch rebased instead of merged, and adhere to the commit message recommendations from [cbea.ms/git-commit](https://cbea.ms/git-commit/).

For advanced topics such as packaging providers and API versioning see [`12_provider_distributions.rst`](contributing-docs/12_provider_distributions.rst) and [`19_execution_api_versioning.rst`](contributing-docs/19_execution_api_versioning.rst).

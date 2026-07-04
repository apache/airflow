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
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Airflow dev MCP server](#airflow-dev-mcp-server)
  - [Quick start: stdio (any local MCP client)](#quick-start-stdio-any-local-mcp-client)
  - [Quick start: HTTP (Breeze)](#quick-start-http-breeze)
  - [Configuration](#configuration)
  - [Safety model](#safety-model)
  - [Tool surface](#tool-surface)
  - [Testing](#testing)
  - [Design notes: relationship to AIP-91 and the dev-list discussion](#design-notes-relationship-to-aip-91-and-the-dev-list-discussion)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Airflow dev MCP server

An internal, development-only [MCP](https://modelcontextprotocol.io/) server that lets any
MCP-capable coding agent (Claude Code, Cursor, or any other client of the vendor-neutral MCP
protocol) inspect and debug an Airflow instance -- typically a local Breeze development
environment -- through Airflow's public REST API (`/api/v2/...`).

**Scope:** this is groundwork for AIP-91 (Airflow MCP server), specifically its "internal
Breeze tool" phase. It is **not** shipped to users, **not** part of any released Airflow
distribution, and **not** intended for production use. It talks to exactly one Airflow
instance over plain HTTP, the same way any REST API client would; it does not touch the
metastore directly and adds no new capability to Airflow itself.

## Quick start: stdio (any local MCP client)

Point your MCP client's config at this project directory with `uvx`, substituting your own
checkout path for `/abs/path/to/airflow`:

```json
{
  "mcpServers": {
    "airflow-dev": {
      "command": "uvx",
      "args": ["--from", "/abs/path/to/airflow/dev/mcp_server", "airflow-dev-mcp"]
    }
  }
}
```

`uvx` builds and runs the server from local sources on demand -- no separate install step,
and each invocation picks up your current checkout. Equivalently, from a shell:

```bash
uvx --from /abs/path/to/airflow/dev/mcp_server airflow-dev-mcp
```

## Quick start: HTTP (Breeze)

Start Breeze with the MCP server wired into the same container as the rest of Airflow:

```bash
breeze start-airflow --mcp-server
```

Then point your MCP client at the streamable-HTTP endpoint:

```json
{
  "mcpServers": {
    "airflow-dev": {
      "url": "http://localhost:28081/mcp"
    }
  }
}
```

Port `28081` is the host-forwarded port for the MCP server inside the Breeze container
(analogous to `28080` for the API server). In this mode the server already knows how to
reach the in-container API server and, being a disposable dev environment, ships with writes
enabled by default (`AIRFLOW_MCP_ALLOW_WRITES=true`). Deletions stay off even here and must be
enabled deliberately with `AIRFLOW_MCP_ALLOW_DELETES=true`. See
[`dev/breeze/doc/03_developer_tasks.rst`](../breeze/doc/03_developer_tasks.rst) for the full
Breeze-side wiring.

## Configuration

Every setting is read from the environment at call time:

| Variable                  | Default                   | Purpose                                                          |
|----------------------------|---------------------------|-------------------------------------------------------------------|
| `AIRFLOW_API_URL`          | `http://localhost:28080`  | Base URL of the Airflow API server.                                |
| `AIRFLOW_USERNAME`         | `admin`                   | SimpleAuthManager username used to log in via `/auth/token`.       |
| `AIRFLOW_PASSWORD`         | `admin`                   | SimpleAuthManager password.                                       |
| `AIRFLOW_ACCESS_TOKEN`     | (unset)                   | Pre-issued JWT; when set, the login flow is skipped entirely.     |
| `AIRFLOW_MCP_ALLOW_WRITES` | (unset, i.e. read-only)   | Set to `true`/`1` to allow `POST`/`PATCH`/`PUT` write tools.       |
| `AIRFLOW_MCP_ALLOW_DELETES` | (unset, i.e. no deletes) | Set to `true`/`1` to allow `DELETE` requests. Separate from and stricter than the write flag: off by default even when writes are enabled, since deletions are irreversible. |

The `admin` / `admin` defaults match Breeze's default SimpleAuthManager user
(`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`) -- see
[`dev/breeze/src/airflow_breeze/params/shell_params.py`](../breeze/src/airflow_breeze/params/shell_params.py).

## Safety model

- **Read-only by default.** Every tool that reads data (list/get Dags, runs, task instances,
  logs, import errors, variables, connections, pools, ...) always works. Tools that write
  (`trigger_dag_run`, `set_dag_paused`, `clear_task_instances`, `airflow_api_call`) are always
  *registered* -- so the tool list stays stable across restarts -- but raise a clear error
  explaining how to enable writes unless `AIRFLOW_MCP_ALLOW_WRITES=true` is set.
- **`DELETE` is gated more strictly than other writes.** It is disabled by default *separately*
  from the write flag — enabling writes does not enable deletions — and requires its own
  `AIRFLOW_MCP_ALLOW_DELETES=true`. Because deletions are irreversible, the `DELETE`-capable
  tool is also annotated as destructive so MCP clients prompt for explicit per-call confirmation.
  It is reachable only through the `airflow_api_call` escape hatch; there is no dedicated
  delete tool.
- **Secrets are never echoed.** Connection passwords/extras are stripped from list responses;
  Variable values are omitted from `list_variables` (use `get_variable` to fetch one value on
  purpose). Tokens are never logged.
- **No inbound auth layer.** The server does not verify the identity of MCP clients connecting
  to it (no JWKS/OAuth). stdio mode has no network exposure by construction; HTTP mode is for
  local development only -- do not expose it beyond localhost.

## Tool surface

See the docstrings in [`src/airflow_mcp_server/server.py`](src/airflow_mcp_server/server.py)
for the full, current list -- they are also what your MCP client displays as each tool's
description. Highlights: `list_dags`, `get_dag`, `get_dag_source`, `list_dag_runs`,
`list_task_instances`, `get_task_log` (tailed to protect context), `diagnose_dag_run` (one-call
failure summary), `list_import_errors`, `list_warnings`, `list_variables`/`get_variable`,
`list_connections`, `list_pools`, plus `airflow_api_get`/`airflow_api_call` escape hatches for
any endpoint not covered by a dedicated tool.

## Testing

This package is a member of the root `uv` workspace (its `fastmcp` dependency adds only a
handful of packages to the shared `uv.lock`, since Airflow already ships the `mcp` SDK,
`httpx`, `pydantic`, `starlette`, and `uvicorn`). Like `dev/breeze`, it also keeps a small
standalone `uv.lock` of its own so it can be built and run in isolation (via `uvx` or
`uv run --project`) without installing the whole Airflow workspace. Run the tests with:

```bash
uv run --project dev/mcp_server pytest dev/mcp_server/tests -x -q
```

These tests mock the Airflow API (`httpx.MockTransport`) and use `fastmcp.Client` in-memory --
no Breeze, database, or network access required. They include a **REST-contract test**
(`test_openapi_contract.py`) that asserts every endpoint this server calls exists, with the
method it uses, in Airflow's committed OpenAPI spec
(`airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml`). If the
Airflow REST API renames, removes, or changes the method of an endpoint we proxy, that test
fails -- turning a would-be silent runtime break into a caught contract mismatch.

**Continuous integration.** These tests run in CI on every build via the `Dev MCP server tests`
job in [`.github/workflows/basic-tests.yml`](../../.github/workflows/basic-tests.yml), which
resolves this project from its standalone lock (`uv run --project . pytest`, like `dev/breeze`)
and runs the same command as above. The opt-in live and manual tests below are **not** run in CI
(the live tests need a running Breeze; the LLM-client test needs an API key -- see the note on
integration testing).

Two additional test entry points are opt-in and not run in CI or by the command above:

- `dev/mcp_server/tests/test_live_breeze.py` -- round-trips real HTTP calls against a running
  Breeze instance. Opt in with:

  ```bash
  AIRFLOW_MCP_LIVE_TESTS=1 uv run --project dev/mcp_server pytest dev/mcp_server/tests/test_live_breeze.py
  ```

- `dev/mcp_server/tests/manual/llm_smoke.py` -- the **AI-client integration test**: a standalone
  [PEP 723](https://peps.python.org/pep-0723/) script that connects a real LLM agent to this
  server over stdio and asks it to list Dags. This is the end-to-end "does an actual agent drive
  the tools correctly" check. It **runs manually for now** and is deliberately not wired into CI:
  it needs a real LLM API key, and provisioning one securely for CI (and deciding which
  model/provider CI should exercise) is unresolved -- until then it stays a manual, opt-in check.
  It requires an LLM API key (any provider supported by pydantic-ai, selected via
  `AIRFLOW_MCP_LLM_MODEL`); run directly with `uv`:

  ```bash
  ANTHROPIC_API_KEY=... uv run dev/mcp_server/tests/manual/llm_smoke.py
  # or e.g.:
  OPENAI_API_KEY=... AIRFLOW_MCP_LLM_MODEL=openai:gpt-5 uv run dev/mcp_server/tests/manual/llm_smoke.py
  ```

## Design notes: relationship to AIP-91 and the dev-list discussion

This package is the "internal Breeze tool" step of
[AIP-91](https://cwiki.apache.org/confluence/spaces/AIRFLOW/pages/364349979): a stateless proxy
translating MCP tool calls into Airflow REST API requests, deliberately following the AIP-91
architecture (REST proxy, no direct metastore access, read-only by default) so later phases can
grow out of it. How the concerns raised in the
[dev-list discussion](https://lists.apache.org/thread/xgd66v6s7zf0xkvy3c7ysqvn4csgmw06) map here:

- **"1:1 API wrapping produces too many tools and hides destructiveness"** -- the server exposes
  a small curated set (~20 tools) with agent-oriented descriptions and MCP annotations
  (`readOnlyHint`/`destructiveHint`), plus a composite `diagnose_dag_run` instead of forcing the
  agent to chain raw endpoints. The rest of the API stays reachable through two documented
  escape hatches rather than dozens of generated tools.
- **"Use-case first, not API-wrapping first"** -- the tool set is scoped to the contributor
  debugging loop in Breeze: is my Dag parsed (import errors, warnings, source), did the run
  succeed (runs, task instances), why did it fail (logs, diagnose), rerun it (trigger, clear).
- **"One opinionated server cannot fit all deployments"** -- true for production Airflow, which
  is why this stays a dev-only tool; Airflow contributors using Breeze are a homogeneous
  audience for which one opinionated server is appropriate.
- **"The agent must not exceed the user's permissions / human-in-the-loop for elevated
  actions"** -- there is no RBAC boundary to cross: the server logs in through the normal
  `/auth/token` endpoint as the Breeze dev user and can never exceed that user's permissions;
  no airflow-core code is modified. Destructiveness is bounded by the hard server-side method
  gate (see "Safety model"), which is enforcement, not advisory annotation.
- **Production-only concerns** (SecretsMasker re-filtering on the log read path, JWT session
  refresh middleware, per-user rate limiting, audit logging) are deliberately out of scope for
  a single-user disposable dev environment -- they belong to the production phases of AIP-91.
  Consequently: task-log content is served as-is, so do not point this server at a real
  deployment holding real credentials.

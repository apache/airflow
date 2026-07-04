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

- [18. Run an internal MCP server for the Airflow REST API as a Breeze service](#18-run-an-internal-mcp-server-for-the-airflow-rest-api-as-a-breeze-service)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 18. Run an internal MCP server for the Airflow REST API as a Breeze service

Date: 2026-07-04

## Status

Accepted

## Context

Contributors increasingly drive their Breeze development environment with coding agents
(Claude Code, Cursor, and any other client of the vendor-neutral
[Model Context Protocol](https://modelcontextprotocol.io/)). The recurring loop in local
development is not writing code — it is *debugging a running Airflow*: is my Dag parsed, did
the run succeed, why did a task fail, what do the logs say, re-run it and check again. Today an
agent can only answer those questions by shelling out: hand-rolling a JWT against
`POST /auth/token`, then stringing together `curl` calls to the REST API, parsing JSON out of
each one, and keeping track of run ids and map indexes by hand. That is brittle, easy to get
wrong, and burns the agent's steps on plumbing instead of on the actual problem.

To be precise about the target: what we want to expose is **Airflow's own REST API** — the data
and state behind the running Airflow instance (Dags, Dag runs, task instances, logs, import
errors) — **not Breeze**. Breeze has no data of its own to serve; it is only *how* the Airflow
under test (and this proxy) is launched. This is therefore not a wrapper around Breeze or its
CLI, and it does not expose Breeze commands. Encoding *how to drive Breeze itself* — build,
run the right static checks, reproduce CI, host-vs-container awareness — is the job of agent
skills (e.g. the GSoC Breeze Agent Skills effort); this MCP is the complementary layer that
inspects the *running Airflow* those workflows produce. The two operate at different layers and
compose (a verification skill can drive Breeze and then use these tools to check the outcome).

Separately, [AIP-91](https://cwiki.apache.org/confluence/spaces/AIRFLOW/pages/364349979)
proposes a *user-facing* Airflow MCP server: a stateless proxy over the public REST API,
read-only in its first phase, with per-user RBAC enforced by passing the caller's token through
to Airflow's own auth, deployable via the official Helm chart and Breeze, opt-in and
non-breaking. That is a production feature with production concerns (RBAC, secret masking on the
log path, session refresh, rate limiting, audit logging) and its own review/approval track.

There is a gap between "an agent debugging my local Breeze" and "a hardened multi-user
production server". The debugging need is real and immediate; the production server is neither.
We want to serve the debugging need now, cheaply, without waiting on — or prejudging — the
larger AIP-91 design, while deliberately following the same architecture so the two do not
diverge.

## Decision

We ship a small, **internal, development-only** MCP server that lives in the repository at
`dev/mcp_server` and is wired into Breeze as a first-class service.

- **It is a stateless proxy for the Airflow REST API** (not for Breeze). Every tool call is
  translated into a request against the running *Airflow* instance's public REST API
  (`/api/v2/...`) — the instance Breeze happens to launch. It never touches the metadata database
  directly and adds no capability to Airflow itself — it only repackages the REST API behind a
  curated, agent-oriented tool surface (`list_dags`, `list_task_instances`, `get_task_log`, a
  composite `diagnose_dag_run`, `trigger_dag_run`, `clear_task_instances`, …), plus two escape
  hatches (`airflow_api_get` / `airflow_api_call`) so no endpoint is out of reach. The surface is
  overwhelmingly read-only runtime *data*; no tool shells out to Breeze or the CLI.
- **It runs as a Breeze service.** `breeze start-airflow --mcp-server` starts it inside the same
  container as the rest of Airflow (host port `28081` → container `8081`, analogous to `28080`
  for the API server). It is launched straight from the mounted sources via
  `uvx --from /opt/airflow/dev/mcp_server` (consistent with [ADR 0017](0017-use-uvx-to-run-breeze-from-local-sources.md)),
  so it always runs the current worktree's code and never installs anything into the checkout.
  The server's lifetime is exactly the Breeze session's: **when Breeze is off, the endpoint is
  gone.** There is nothing to stop, clean up, or leave running.
- **Adding it to an agent is minimal client configuration.** The contributor points their MCP
  client at the endpoint — for HTTP, a few lines:

  ```json
  { "mcpServers": { "airflow-dev": { "type": "http", "url": "http://localhost:28081/mcp" } } }
  ```

  (a `uvx`-based stdio entry is also supported). No Airflow-side change, no account setup.
- **It is safe by construction for a single-user disposable environment.** Read-only by default;
  writes are opt-in behind `AIRFLOW_MCP_ALLOW_WRITES` (Breeze sets it on by default because the
  environment is disposable); `DELETE` is gated more strictly still, behind its own
  `AIRFLOW_MCP_ALLOW_DELETES` flag that stays off even when writes are enabled (deletions are
  irreversible) and whose tool is annotated destructive so MCP clients ask for explicit per-call
  confirmation. Deliberately, there is **no dedicated `delete_*` tool**: deletion is reachable
  only through the generic `airflow_api_call` escape hatch, so an agent must construct the method
  and path explicitly rather than invoke a one-purpose delete tool — keeping deletion a conscious,
  visible action and avoiding a standing, easily-triggered destructive verb in the tool list.
  Connection secrets and Variable values are stripped from list responses. It logs in as the
  Breeze dev user and can never exceed that user's permissions.
- **It is strictly opt-in.** Nothing starts unless `--mcp-server` is passed, and nothing forces
  an agent on anyone: a contributor who prefers not to use AI tooling simply never starts it, and
  a contributor who does not need it keeps it off so it does not consume container resources. It
  is not part of any released distribution and is not shipped to users.

We deliberately keep the tool surface use-case-first (the contributor debugging loop) rather than
auto-generating one tool per REST endpoint, following the concerns raised on the
[dev-list discussion](https://lists.apache.org/thread/xgd66v6s7zf0xkvy3c7ysqvn4csgmw06) of AIP-91.

**Testing and CI.** Because the server is a thin proxy over the public REST API, its correctness
has two parts, both guarded automatically:

- **Contract compatibility with Airflow's REST API.** A contract test asserts that every endpoint
  the server calls exists — with the method it uses — in Airflow's *committed, generated* OpenAPI
  spec (`airflow-core/.../openapi/v2-rest-api-generated.yaml`). If Airflow renames, removes, or
  changes the method of an endpoint we proxy, that test fails, so a REST-API change that would
  otherwise break the server silently at runtime is caught at build time instead. This is the
  mechanism that keeps the proxy honest against the API it depends on. The check runs against a
  hand-maintained list of the endpoints the server uses, so a second test guards *that* list
  against drift: it extracts every `/api/v2/...` path the server source actually calls and fails
  if any is missing from the list — a new tool cannot silently escape contract coverage by being
  added without registering its endpoint.
- **Unit tests run in CI.** The server's unit tests (mocked transport, in-memory MCP client,
  plus the contract test) run on every build as their own job (`Dev MCP server tests` in
  `.github/workflows/basic-tests.yml`). It is a *separate* job from Breeze's own tests because
  this is a separate standalone `uv` project with its own lock, resolved and run in isolation
  (`uv run --project . pytest`) exactly the way `dev/breeze` is — not something Breeze itself
  needs to be running for.
- **AI-client integration is manual for now.** There is an end-to-end test that drives the server
  with a real LLM agent over stdio (`tests/manual/llm_smoke.py`). It is intentionally kept manual
  and out of CI until we resolve how to provision an LLM API key securely for CI and which
  model/provider CI should exercise; the live-Breeze round-trip tests are likewise opt-in
  (they need a running instance). Neither runs in the CI job above.

**Relationship to AIP-91 and a possible shared future.** This internal server is intentionally
the same shape as the AIP-91 user-facing server — REST proxy, no direct DB access, read-only by
default, opt-in, Breeze-deployable — so it prefigures that design rather than competing with it.
The honest differences follow from its single-user, disposable scope: it does **not** implement
per-user RBAC token pass-through (there is one user); it allows opt-in writes and even opt-in,
separately-gated deletions, whereas AIP-91's user-facing Phase 1 is read-only and would block
both; and it deliberately omits the production concerns (secret re-masking on the log path, JWT
session refresh, per-user rate limiting, audit logging, Helm packaging). Those extra capabilities
are acceptable *here* precisely because the environment is a throwaway single-user dev instance;
they are exactly the kind of thing the production server would not expose until later phases. Ideally the
user-facing AIP-91 server for non-Breeze deployments could later be **built on top of** this one.
If we decide the two should share a single interface, this server can be **relocated to a
provider (e.g. `common.ai`) and deployed from there**, with the production hardening added at that
layer — leaving this ADR's server as the internal, dev-only entry point. That is a forward option,
not a commitment.

## Consequences

**Wins**

* **Less time spent reaching a correct fix on API-heavy problems.** Giving the agent cheap, typed,
  authenticated access to the running instance lets it *observe* state directly instead of
  reconstructing it through trial and error, which measurably shortens the debugging loop (see the
  evidence below).
* **No auth or plumbing friction.** Login, tokens, and endpoint shapes are handled by the server;
  the agent spends its budget on the problem, not on `curl` and JWT juggling.
* **Zero-footprint lifecycle.** The server is a Breeze pane that lives and dies with the session;
  there is no daemon to manage and no exposure once Breeze is down.
* **No airflow-core changes and no new attack surface in Airflow.** It is a thin proxy over the
  existing REST API with a hard server-side method gate (GET always; writes behind one flag;
  DELETE behind a second, stricter flag).
* **Architecture-aligned with AIP-91.** The same proxy design can grow into (or under-pin) the
  user-facing server, so the internal tool is not throwaway.

**Costs**

* **More token (and therefore money) usage.** An agent driving the MCP server consumes tokens on
  tool calls and their results; a human running the same CLI commands does not. This is a real,
  ongoing cost that scales with how much the agent inspects.
* **Little to no benefit when the problem does not require extensive API interaction.** For a
  well-specified bug whose fix is obvious from the issue text and the source, the running instance
  adds nothing — the agent would solve it just as fast reading code and running unit tests, and the
  MCP calls are pure overhead.
* **Another Breeze service to keep working.** It is one more pane that can break, one more port to
  forward, and its curated tool surface has to be maintained as the REST API evolves.
* **Not a substitute for the CLI.** For deterministic, scriptable, human-run tasks, plain
  `breeze`/`airflow` CLI commands remain simpler and cheaper; the MCP server earns its place only
  in the agent-driven debugging loop.

**Evidence (two controlled trials).** To keep the pros/cons honest rather than assumed, we ran two
A/B experiments: the same model (Claude Sonnet) fixing the same real open bug in two isolated git
worktrees off the same commit, identical task, the *only* difference being whether the agent had
this MCP server (the no-MCP arm was hard-denied the tools). Both arms were ground-truth-verified
(the added regression test passes with the fix and fails without it).

* **When the problem is *not* API-heavy — negligible benefit.**
  [#42790](https://github.com/apache/airflow/issues/42790) (the `owners` filter substring-matches)
  is well-specified: the fix is clear from the issue and the source. Both arms produced a correct,
  tested fix; the MCP made no difference to correctness and did not make the agent faster. This is
  the "little effect" case above, demonstrated.
* **When the problem *is* API-heavy — a materially shorter path.**
  [#39801](https://github.com/apache/airflow/issues/39801) (a `none_failed_min_one_success` task
  skipped before its dynamic task group expands) had been open for over two years; its patch is
  tiny but *finding* it requires running a Dag and inspecting task-instance states. Both arms again
  reached a correct, tested fix — but the MCP arm did so in **~9 min / 46 turns / 8 test
  iterations**, versus the no-MCP arm's **~28 min / 86 turns / 24 test iterations** (roughly 3×
  faster, ~⅓ the cost). Notably the win did not come from saved `curl` plumbing (neither arm used
  `curl`); it came from **two** cheap typed observations that let the MCP arm see the failing state
  early and stop guessing.

The pattern matches the decision above: **the MCP server never changed *whether* a correct fix was
produced — but on a problem that needs to interrogate a running Airflow, it roughly halved the path
to it.** That is exactly the value we want from a development tool, and it is why the server is
opt-in: worth starting when you are debugging a running instance with an agent, not worth the
tokens or the extra Breeze pane otherwise.

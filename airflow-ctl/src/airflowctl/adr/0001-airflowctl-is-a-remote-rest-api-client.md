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

# 1. airflowctl is a remote client that reaches Airflow only through the public REST API

Date: 2026-07-19

## Status

Accepted

## Context

`airflowctl` is the standalone `apache-airflow-ctl` distribution for managing a
running Airflow deployment from the outside. It is deliberately **not** the
in-process `airflow` CLI that ships in airflow-core: it is a separate package with
its own dependency set, expected to run on an operator's laptop or a CI runner
with no co-located Airflow install and no metadata-database access.

Under the post-3.0 architecture, only the API server touches the metadata database
and remote clients interact through the **public REST API v2**. `airflowctl` is
such a client: transport is an `httpx`-based `api/client.py`, endpoint wrappers
live in `api/operations.py`, and it authenticates as an ordinary API client — a
JWT bearer token stored per environment in the system `keyring` — with no special
privilege. The temptation, when data is awkward to get over the API, is to
`import airflow.*`, open a SQLAlchemy session, or read a server-side config file;
any of those re-couples the client to a co-located server, drags in airflow-core's
heavy dependency tree, and bypasses the API server's authorization. Because the
client also runs on untrusted machines, locally-sourced input (environment names,
paths from env vars) is part of its attack surface and must be validated.

## Decision

`airflowctl` is treated as a remote REST API client and nothing more:

- **It reaches Airflow only through the public REST API v2.** Command handlers
  call `api/operations.py` over `api/client.py`; they do not import airflow-core
  modules, open a database session, or read server-owned config/state.
- **It is a separate distribution.** `apache-airflow-ctl` does not take an
  airflow-core (or provider) runtime dependency to make a command work; if the
  data a command needs is not on the API, the fix is a server-side endpoint, not
  a local import.
- **It authenticates as an API client.** Access is a JWT/token stored per
  environment (`AIRFLOW_CLI_ENVIRONMENT`) in `keyring`; the client carries no
  server-side privilege and never assumes DB or filesystem co-location.
- **Locally-sourced input is untrusted.** Environment names and paths from env
  vars / flags are validated (e.g. against path traversal) before use.

## Consequences

`airflowctl` stays deployable anywhere a REST client can run, works unchanged
against a remote deployment, and cannot become a side door into the database. The
package stays lightweight. The cost is that some capabilities require a server-side
API endpoint to exist first — added on the server, then consumed here, rather than
shortcut through a local import.

A change **violates** this decision when it:

- imports an `airflow.*` core module, opens a DB session, or reads server-side
  config/state from a command handler instead of calling the API;
- adds an airflow-core (or provider) runtime dependency to `apache-airflow-ctl`
  so a command can reach data locally rather than over the API;
- assumes co-location with the server (local filesystem, local DB, shared
  config) instead of treating Airflow as a remote endpoint;
- consumes a locally-sourced value (environment name, path) without validating
  it, or mishandles/prints the stored token/credential.

## Evidence

- #62843 — `auth token` prints a JWT: the CLI authenticates as an API client, not
  a DB session.
- #64618 — path-traversal fix: `AIRFLOW_CLI_ENVIRONMENT` is untrusted input,
  validated before building a keyring/token path.
- #63772 — version command must not prompt for keyring credentials.
- #65099 — a command that legitimately needs no token must not force a prompt.
- #62549 — `auth login` interactive prompt: logging in as an API client is the
  entry point to every authenticated command.

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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Dag parsing through executors: PoC work log

Checkpoint: September 25, 2026. Branch: `codex/dag-parsing-executor-poc`.
Base: `61d99c0374e77cefdc8f4bad495cb5d6c0bfc43b`.

The initial implementation checkpoint is commit `9706237a63`. The
[SDK importer follow-up](#sdk-importer-follow-up) records the subsequent integration.

This work tests whether parsing can reuse executor placement and lifecycle management
without sharing task capacity. LocalExecutor and an isolated Celery worker now run
bounded parsing batches, publish authenticated results, and recover unfinished work
under one trusted coordinator. A separate metadata experiment takes a remote result
through Airflow's persistence code into scheduler consumption.

This is an M0 experiment, not a production feature or completion of M0. The existing
Dag processor remains required for normal deployments. Scheduler hosting, Kubernetes,
production API integration, and comparative performance measurements remain open.

## Scope and organization

The implementation began with three agents responsible for executor transport,
supervised importing, and authenticated result acceptance. The coordinator integrated
the driver and ran process, HTTP, and container experiments. Subsequent independent
reviews drove the acknowledgment, admission, recovery, and persistence fixes below.

The original checkpoints have these outcomes:

| Checkpoint | Outcome |
| --- | --- |
| P0: contracts and baseline | Workload/result contracts and a legacy importer bridge implemented; a small serial-parser baseline recorded. Full baseline and related-AIP agreement remain open. |
| P1: LocalExecutor slice | Passed real process/HTTP execution, per-definition errors and timeouts, and a 100-definition batch under `spawn`. |
| P2: remote execution and recovery | Celery transport, isolated deployment, duplicate delivery, worker loss, admission restoration, and metadata consumption demonstrated. Kubernetes and HA adoption remain open. |
| P3: measurements and AIP feedback | No throughput or performance conclusion. Comparable load tests and agreed acceptance thresholds remain open. |

No task executor default, normal callback route, or scheduler parsing loop was changed.
The development drivers explicitly enable parsing on dedicated executor instances.

## Implementation map

Paths below are relative to the repository root.

| Area | Main implementation |
| --- | --- |
| Workload schema and identity | [parsing.py](../../airflow-core/src/airflow/executors/workloads/parsing.py), workload registry, key/state types |
| Executor dispatch | [BaseExecutor](../../airflow-core/src/airflow/executors/base_executor.py), [LocalExecutor](../../airflow-core/src/airflow/executors/local_executor.py), Celery executor and task entry point |
| Importing and publication | [executor_worker.py](../../airflow-core/src/airflow/dag_processing/executor_worker.py) |
| Development authentication and receipts | [api.py](../../dev/dag_parsing_poc/api.py), [store.py](../../dev/dag_parsing_poc/store.py) |
| Durable single-owner recovery | [coordinator.py](../../dev/dag_parsing_poc/coordinator.py), [recovery_checkpoint.py](../../dev/dag_parsing_poc/recovery_checkpoint.py) |
| Metadata acceptance | [metadata.py](../../dev/dag_parsing_poc/metadata.py), [collection.py](../../airflow-core/src/airflow/dag_processing/collection.py), DagCode and SerializedDagModel |
| Experiments | [Local driver](../../dev/dag_parsing_poc/run.py), [Celery driver](../../dev/dag_parsing_poc/run_celery.py), [recovery driver](../../dev/dag_parsing_poc/run_celery_recovery.py), [metadata driver](../../dev/dag_parsing_poc/run_metadata.py), [isolated worker](../../dev/dag_parsing_poc/celery_worker.py) |
| Regression coverage | [PoC tests](../../dev/dag_parsing_poc/tests), core worker/executor/model tests, Celery parsing workload tests |

### Workloads and supervised importing

`ParseDagDefinitions` carries 1–100 definition attempts, bundle/version, relative source
paths, SHA-256 revisions, per-definition timeouts, start/stop deadlines, queue, and a
workload token. Separate workload keys and states allow executor accounting without
using task-instance identity. Celery uses a stable workload UUID for submission and a
fresh execution UUID for each delivery.

The worker resolves the bundle against its own configured root, checks containment
and source revisions, and invokes the current core `DagFileProcessorProcess` bridge.
Live SDK Dag objects stay inside the importing runtime; serialized per-definition
results cross HTTP. The initial checkpoint did not exercise the SDK AIP-85
definition/importer interfaces; the follow-up below does.

Each batch member has its own result and supervised import timeout. Source hashes
are checked around importing. Attempt-ID JSONL logs are written locally or to stdout;
there is no production remote log lookup service. Receipt-only mode accepts a whole
archive as a source reference, but the experiments exercise Python files, not
independent archive members.

LocalExecutor now starts a consumer before a potentially blocking queue write.
Unread accounting precedes delivery and rolls back on write failure. This fixes the
large-batch deadlock exposed by `spawn` and `forkserver` tests. Running work retains
its executor slot through its terminal event. Worker exceptions carry transport-safe
status/type information across process queues instead of HTTP response objects.

Celery uses an explicit parsing queue and a Redis result backend. Provider imports
remain usable with core versions that lack the experimental parsing module. Logging
uses redacted workload summaries rather than token-bearing arguments.

### Authentication, receipts, and acknowledgment recovery

The development FastAPI app is explicitly launched and is not mounted in the production
Execution API. It verifies Ed25519 workload tokens, including issuer, audience, scope,
subject, and registered attempt identity. The API receives only the public verification
key. Registration persists immutable manifests without tokens.

SQLite transactions serialize claims and acceptance. A claim binds an attempt to one
execution; accepted per-definition results are immutable. Identical authorized retries
return the existing receipt, while conflicting identities or payloads are rejected.
Results, rather than executor success alone, establish parsing outcomes.

Worker and API share an 8 MiB request limit measured over the actual UTF-8 envelope.
Oversized or unencodable output becomes a small `worker_error` receipt so subsequent
definitions can run. A definitive first HTTP 413 allows that fallback. A 413 after an
uncertain publication cannot replace the original body, which may already be committed.

An initial claim or publication starts before its execution deadline. After a transport
error or server error, one byte-identical replay may start within a separate five-second
monotonic recovery window. The API decides whether replay is still valid: a committed
result can be acknowledged after its stop deadline; recovering a claim does not permit
a new import after stop. This bounds retry start time, not total HTTP response time.

Celery treats another active execution's claim and an unresolved claim acknowledgment
as `Ignore`, preserving shared backend state and capacity. Definitive rejection and
ordinary import/publication failures remain failures. This is conservative accounting,
not proof that an uncertain delivery has stopped.

### Startup accounting and atomic recovery

The coordinator persists admissions as reserved, submitted, or released. Registration
and reservation are atomic; submission is recorded before provider I/O. Startup restores
outstanding charges, including when backend history is missing. Reserved work stays out
of executor heartbeat queues until explicitly dispatched.

An indexed pending-admission table avoids scanning all completed history on each
admission; older receipt stores receive a one-time backfill. Lowering capacity does not
discard existing reservations. Dispatch reports published, uncertain, or expired
outcomes rather than treating attempted publication as confirmed delivery.

Never-submitted reservations with no claim can expire and retire atomically, including
expiry during signing. Submitted or claimed work requires trusted termination evidence.
Missing backend records, a terminal provider state, cancellation requests, or elapsed
time alone do not release the durable charge.

Once termination is established, one transaction rechecks accepted results, retires
unfinished attempts, creates replacement identities, transfers capacity, and records an
immutable recovery decision. Accepted siblings are preserved. Repeated recovery returns
the same replacement identities and deadlines. Metadata mode replaces only definitions
that still have current authority.

The live recovery harness correlates a run UUID, worker startup UUID, and hostname with
Docker inspection. It requires distinct original and replacement workers, verifies the
original exited with PID zero, and verifies a previously observed Redis `STARTED` record
was actually deleted. Loopback API probes ignore ambient HTTP proxy settings.

This remains a single-coordinator experiment. A crash after recording submission but
before broker publication can leave an unresolved reservation. Unknown termination can
retain capacity indefinitely; the harness does not provide a general termination oracle,
HA leases, or safe adoption of arbitrary in-flight work.

### Remote results into metadata and scheduler consumption

The opt-in metadata API mode shares one physical SQLite connection and transaction
between receipts and Airflow ORM writes. `update_dag_parsing_results_in_db` has an
optional atomic mode that makes one attempt and propagates persistence and diagnostic
failures. The caller can retry the entire acceptance transaction with authority checks;
legacy callers retain their existing retry behavior.

The worker can include UTF-8 Python source. Acceptance verifies its SHA-256 and supplies
it to DagCode persistence, so the API need not read worker-local files. Source text counts
toward the request limit. Explicit empty source is distinguished from missing source.
Archives and other encodings were outside the initial metadata checkpoint.

First acceptance validates the claim, deadline, bundle/version, registered source, and
current attempt. Logical paths are normalized, version data is preserved, and a definition
cannot overwrite a Dag ID owned by another definition. Registration order is a trusted
coordinator decision; opaque hashes do not imply ordering. Accepted replay bypasses new
metadata writes even after expiry or a newer registration.

Failure-injection tests cover metadata, diagnostics, warnings, and receipt insertion:
acceptance and database changes roll back together. Existing listener side effects are
not transactional. The adapter requires a fresh schema and does not provide production
migrations or PostgreSQL/MySQL concurrency evidence.

The final live run used the real scheduler methods `_create_dag_runs`,
`_start_queued_dagruns`, and `_schedule_dag_run` against the persisted result. They
created a running Dag run and a scheduled task without importing source in the scheduler.
The experiment did not execute that task or host parse orchestration in the scheduler.

## Reviews and validation

Review rounds found and fixed large-message queue blocking, unsafe exception transport,
retry-deadline coupling, oversized batch-member failure, uncertain claim handling,
inadequate termination evidence, missing startup accounting, non-atomic retirement,
reservation dispatch on restart, signing-time expiry, publication ambiguity, repeated
history scans, and stale metadata acceptance. Claim-contention tests were also checked
with deliberate mutations: removing the transaction or using an unprotected read made
the tests fail as intended.

The combined regression run exposed an autouse frozen-clock fixture registered globally
through `pytest_plugins`. Local fixture imports removed five unrelated timestamp
failures. Tests use Breeze with worktree sources, not host Python.

| Evidence | Recorded outcome |
| --- | --- |
| Initial Local/HTTP experiment | Two serialized Dags, one import error, one timeout; expected Dag IDs verified |
| Large Local batch | 100 accepted definition results in one workload under `spawn` |
| Initial isolated Celery experiment | Dedicated Redis/provider transport, accepted replay, and active duplicate delivery passed |
| Final worker-loss/restart experiment | Accepted sibling imported once; interrupted definition imported twice; old claim/result requests rejected with 409; capacity restored after confirmed replacement termination |
| Final metadata experiment | One import across two successful Celery deliveries; replay preserved metadata; Dag run running and task scheduled |
| Combined regression suite | 578 passed, 2 skipped |
| Final persistence rerun | 85 passed, 2 skipped; overlaps the combined suite and covers the final one-attempt atomic path |
| Post-hook regression rerun | 185 passed across worker, API, metadata, and coordinator tests after the type fixes; overlaps earlier suites |
| Earlier executor regression checkpoint | 172 passed, 1 skipped, including existing BaseExecutor and LocalExecutor tests |
| Formatting and whitespace | Ruff lint/format and `git diff --check` passed during implementation |
| Commit preparation | All applicable pre-commit hooks passed, including mypy for core, dev, and devel-common; SDK-import checks; Markdown links; and Bandit |

Counts from successive checkpoints overlap and must not be added. The cached Python
3.10 image reports an unknown pytest-configuration warning. Full CI, the complete
typing matrix, other Python versions, and all database backends have not been validated.

Commit preparation corrected type annotations for HTTP error narrowing, the SQLite
connection factory, receipt-store selection, process startup, and terminal-event
records. The worker's intentional SDK client and logging imports were recorded with
Airflow's scoped allowlist generator. Hooks added the required full Markdown license
header; the Local driver gained inline script metadata. The link checker passed after
retrying its exact Docker image download. These steps did not expand the PoC's scope.

The original four-definition smoke measurement was approximately 1.295 seconds for
serial supervised parsing and 1.316 seconds through LocalExecutor. It excludes discovery
and the existing manager pool and supports no performance claim.

The isolated Celery worker ran as UID 50000 with a read-only root, runtime, and bundle,
temporary writable storage, dropped capabilities, and no deployment database or signing
credentials configured. Its metadata engine remained uninitialized; neither the metadata
artifact directory nor Docker socket was mounted. These are same-host deployment checks,
not proof of a hostile-code sandbox or multi-host isolation. Experiment containers and
networks were removed without removing pre-existing user containers.

## Reproduction and retained evidence

Run from this worktree through Breeze. The combined suite was executed as:

```bash
breeze run --github-repository local-dag-diff-review/airflow \
  --skip-image-upgrade-check --answer no --python 3.10 \
  pytest dev/dag_parsing_poc/tests \
  airflow-core/tests/unit/dag_processing/test_executor_worker.py \
  airflow-core/tests/unit/executors/test_parsing_workload.py \
  airflow-core/tests/unit/executors/test_workloads.py \
  providers/celery/tests/unit/celery/executors/test_parsing_workload.py \
  airflow-core/tests/unit/dag_processing/test_collection.py \
  airflow-core/tests/unit/models/test_dagcode.py \
  airflow-core/tests/unit/models/test_serialized_dag.py -q --override-ini addopts=
```

The image override selects the cached CI image used for these experiments. Other
environments need a working Breeze image with the required dependencies. Its image ID
was `sha256:26fdbe3c4650d22ce559392edbcce080c470368a95fc65a2641b94054b35b777`.

The following are the actual Python invocations executed inside Breeze, recorded as
experiment history. Do not run them with host Python.

```bash
breeze run python -m dev.dag_parsing_poc.run --baseline
breeze run python dev/dag_parsing_poc/run_metadata.py \
  --output /files/dag-parsing-aip/poc-runs/metadata-NEW
breeze run python -m dev.dag_parsing_poc.run_celery_recovery \
  --output /files/dag-parsing-aip/poc-runs/recovery-NEW \
  --broker-url redis://poc-broker:6379/0 \
  --result-backend redis://poc-broker:6379/1 \
  --deadline-seconds 120 --recover
```

The Local driver is self-contained. Remote drivers also need a dedicated Redis container,
an isolated network with broker alias `poc-broker` and driver alias `poc-api`, and a
separately launched `celery_worker.py`. They are coordinated experiments, not one-command
deployment tooling. Use a new output directory and wait for `driver-ready.json` before
launching the worker with that run's identities, queue, API URL, bundle/version, and
bundle root. Metadata mode requires `--include-source`; the API defaults to port 8799.

Give the worker a clean environment and fresh home, a DB-less result backend, read-only
runtime/bundle mounts, and a separate writable evidence directory. Do not mount the
driver's metadata database or signing credentials. Recovery uses `kill-request.json`,
`replacement-ready.json`, and completion-stop evidence to coordinate externally
confirmed worker termination. Preserve the run and worker identity checks when adapting
the harness; do not substitute a missing backend result for termination evidence.

The Redis image was `redis:7.4-alpine`, digest
`sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499`.

Raw evidence is retained locally under this worktree's ignored
`files/dag-parsing-aip/` directory. It is deliberately excluded from the commit:

| Local artifact | Purpose |
| --- | --- |
| `metadata-final-tests.log` | Combined 578-test result |
| `metadata-atomic-final-tests.log` | Final persistence rerun |
| `commit-hooks-final.log` | Successful pre-commit hook run after type fixes |
| `commit-final-tests.log` | Post-hook 185-test regression run |
| `poc-runs/metadata-20260925-02/` | Final metadata summary, source hashes, worker launch/inspection, delivery markers |
| `poc-runs/admission-recovery-20260925-03/` | Final restart observations, retirement/replacement, termination and capacity evidence |
| `poc-runs/c0c2648d78cf/` | 100-definition Local `spawn` run |
| Earlier review and validation Markdown files | Historical findings and checkpoint-specific evidence |

Final metadata run ID: `7fc03a12-a0c8-4ffb-ad62-ed79f4b3c7c8`. The Dag was
`remote_metadata_checkpoint`, its run was `scheduled__2026-01-01T00:00:00+00:00`,
and task `sample` reached `scheduled`. Runtime source hashes matched before and after
the final experiments. Artifact scans found no JWT strings or private-key markers.
Runtime databases, logs, copied fixtures, and container inspections are not source files.

## SDK importer follow-up

The worker now uses `DagImporterRegistry`, `FilesystemDagDefinition`, and
`ZipMemberDagDefinition` from the Task SDK. Importer selection, user code, validation,
policy checks, and serialization run inside the supervised child. The executor and
orchestrator receive serialized results. Core still supplies validation, policies,
and serialization; this is not a runtime with only the SDK installed. The PoC driver
retains an explicit legacy path for baseline comparisons.

The new [importing adapter](../../airflow-core/src/airflow/dag_processing/executor_importer.py)
retains cycle checks, executor-field validation, default team pools, cluster-policy
skip/rejection, and stability checks. SDK import warnings become diagnostics; ordinary
import failures keep the registered definition's relative identity. Tests cover normal
return, timeout, and abrupt process exit; successful importing is also exercised under
fresh-interpreter launch.

An archive member has its own attempt and content hash, plus `archive_path` and
`archive_revision`. The archive hash covers changes to sibling dependencies.
References reject traversal, incomplete archive identities, and ambiguous member paths.
The worker reconstructs the SDK definition after importing to avoid accepting cached
member bytes after a source change. A whole archive is not one SDK definition.
Transport remains limited to files and archive members; arbitrary Python objects or
importer class names are not accepted in workload payloads.

Source publication uses the SDK importer's `get_source_code()` inside the child and
checks the returned UTF-8 source against the registered member hash. The metadata API
can therefore persist archive-member source without opening the worker's archive.
Claims, immutable receipts, acknowledgment recovery, and durable admission semantics
retain the preceding checkpoint's contracts.

| Live experiment | Outcome and local evidence |
| --- | --- |
| SDK Python files through LocalExecutor | Two successes, one import error, one timeout; legacy baseline still runs separately. `poc-runs/75549eb2814b/` |
| SDK archive members through LocalExecutor | Same four outcomes; two batch dispatches and separate per-member results. `poc-runs/9e6d71979106/` |
| SDK archive members through isolated Celery | Same outcomes; accepted redelivery preserved receipts/import counts, and an active duplicate did not replace the original execution. `poc-runs/sdk-celery-20260925-01/` |
| SDK archive member into metadata and scheduler | One import across two accepted deliveries; metadata unchanged on replay; Dag run running and task `sample` scheduled. `poc-runs/sdk-metadata-20260925-01/` |

The Celery runs retained the credential and filesystem restrictions from the earlier
experiment. Their runtime source hashes matched before and after execution. Neither
experiment executed the scheduled task. The previous live worker-loss/restart experiment
has not been repeated with the SDK adapter.

The affected regression suite passed **446 tests**, covering SDK importing, worker
publication, Local/Celery dispatch, API acceptance, metadata, and admission/recovery.
Core/dev type checks, Ruff, SDK-import checks, and the documentation links also passed.
The cached Breeze image still reports an unknown pytest-configuration warning.
Evidence is retained in `sdk-regression-final.log`, `sdk-types-final.log`, and
`sdk-hooks-final.log`.

One success-case test initially exceeded its ten-second budget during cold interpreter
startup. Its fixture now allows thirty seconds; explicit termination tests retain
one-second timeouts. Production timeout behavior was not relaxed. This is another
reason to measure startup cost before drawing a performance conclusion.

Both remote workers, their dedicated broker, and the test network were removed.
The pre-existing container inventory was unchanged. Scans of the retained remote-run
JSON and logs found no JWT strings or private-key markers.

The development drivers accept `--archive-members` for the archive variant. The Local
driver's `--baseline` remains a comparison against the legacy file importer and cannot
be combined with that option. Actual invocations used inside Breeze were:

```bash
breeze run python -m dev.dag_parsing_poc.run --baseline
breeze run python -m dev.dag_parsing_poc.run --archive-members
breeze run python -m dev.dag_parsing_poc.run_celery \
  --output /files/dag-parsing-aip/poc-runs/sdk-celery-NEW \
  --broker-url redis://poc-broker:6379/0 \
  --result-backend redis://poc-broker:6379/1 --queue poc-sdk --archive-members
breeze run python dev/dag_parsing_poc/run_metadata.py \
  --output /files/dag-parsing-aip/poc-runs/sdk-metadata-NEW --archive-members
```

Use the cached-image options recorded above when reproducing this environment.
Remote runs require the same isolated network and worker setup described earlier;
their exact launch arguments and inspections are retained with the local evidence.

Custom definition codecs, general discovery/dependency transport, parse-time API
reads, and an independent SDK serialization runtime remain outside this checkpoint.
These experiments establish correctness for the tested Python and archive-member
paths, not comparative performance or completion of M0.

## Remaining work

1. Agree AIP-85 importer/discovery boundaries and AIP-92 production API/token contracts.
   Extend SDK coverage beyond Python files/archive members to custom definitions,
   discovery, and dependency transport.
2. Validate Kubernetes placement, termination, recovery, and dispatch cardinality.
   Extend Celery evidence to automatic broker redelivery and uncertain submission.
3. Design production schema, authorization, source ownership, and transactional
   acceptance across supported databases. Address nontransactional listener effects.
4. Resolve distributed ownership/adoption and bounded recovery of uncertain capacity
   before claiming HA or scheduler-safe orchestration.
5. Add periodic discovery, source deletion/deactivation, parse-time API reads, and
   remote log retrieval. Callback and priority-routing changes are not implemented here.
6. Measure comparable throughput, freshness, resource usage, request rates, and recovery
   under load; agree acceptance thresholds before deciding on default rollout or pool
   retirement.

This work log consolidates the implementation history for the local checkpoint commit.
No production rollout or publication is implied.

Generated-by: Codex (GPT-6), with implementation and review assistance from delegated agents.

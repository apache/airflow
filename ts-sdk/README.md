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

# Airflow TypeScript SDK

Public TypeScript interfaces for writing Apache Airflow task handlers.

**Status:** 0.1.0-beta1 · API may change · Node 22+ · ESM-only

This package defines the user-facing task handler contract and the coordinator
runtime used to execute registered TypeScript handlers from Airflow.

## Installation

```bash
npm install apache-airflow-ts-sdk@0.1.0-beta1
```

## Task Handlers

```ts
import { Dag, DagRegistry, serveDags, type TaskHandlerArgs } from "apache-airflow-ts-sdk";

export async function sayHello({ ctx, client }: TaskHandlerArgs) {
  const greeting = await client.getVariable("greeting");
  return { message: `Hello from ${ctx.taskId}: ${greeting}` };
}

const dag = new Dag("example_dag");
dag.task("say_hello", sayHello);

await serveDags(new DagRegistry(dag));
```

Non-`undefined` return values are pushed to XCom under the `"return_value"`
key by the active runtime, matching Python `@task` behavior.

## Coordinator Usage

Airflow runs TypeScript task bundles through the Python-side
`airflow.sdk.coordinators.node.NodeCoordinator`. Declaring Airflow Dags in
TypeScript is not supported yet; the Dag is still declared in Python. The
intended authoring shape matches the other non-Python SDKs: a Python Dag
declares the scheduling shape with stub tasks, and the TypeScript module
registers handlers with matching task IDs.

Python Dag:

```python
from airflow.sdk import dag, task


@dag
def sales_pipeline():
    @task.stub(queue="typescript")
    def extract(): ...

    @task.stub(queue="typescript")
    def transform(extracted): ...

    transform(extract())


sales_pipeline()
```

Airflow coordinator config:

```ini
[sdk]
coordinators = {
  "ts": {
    "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
    "kwargs": {"bundles_root": ["/opt/airflow/ts-bundles"]}
  }
}
queue_to_coordinator = {"typescript": "ts"}
```

Each configured bundle directory must contain a `bundle.mjs` built with
`airflow-ts-pack` (see [Packing bundles](#packing-bundles)), which embeds the
Airflow metadata in the bundle itself.

TypeScript entrypoint:

```ts
import { Dag, DagRegistry, serveDags, type TaskHandlerArgs } from "apache-airflow-ts-sdk";

export async function extract({ client }: TaskHandlerArgs) {
  const connection = await client.getConnection("sales_db");
  const rowCount = Number((await client.getVariable("daily_row_count")) ?? "0");

  return {
    connectionId: connection?.id ?? null,
    rowCount,
  };
}

export async function transform({ client }: TaskHandlerArgs) {
  const extracted = await client.getXCom<{ rowCount: number }>({
    key: "return_value",
    taskId: "extract",
  });

  return {
    transformedRows: extracted?.rowCount ?? 0,
  };
}

const salesPipeline = new Dag("sales_pipeline");
salesPipeline.task("extract", extract);
salesPipeline.task("transform", transform);

await serveDags(new DagRegistry(salesPipeline));
```

The Python stub defines the Dag dependency graph. The TypeScript handler does
the work and uses `TaskClient` for task-time Airflow data access. Create a
`Dag` with the Python Dag's `dag_id` and attach each handler with the stub
task's `task_id`. The handler function is the reusable task implementation;
`dag.task` binds that handler to a Python stub task identity, a `DagRegistry`
collects the Dags this bundle can execute, and `serveDags` serves them to
Airflow.

`serveDags` is the entrypoint, and the registry it is given is the whole bundle:
a Dag left out of the registry is not part of the bundle, and its tasks are
marked removed at runtime. The registry itself holds no sockets and starts
nothing, so a unit test can build one and dispatch through
`registry.getTaskHandler(dagId, taskId)` without any runtime involved.

`new Dag` and `dag.task` take a trailing options object — `spec` on both, plus
`inputs` on a task. These are not used yet; do not set them.

For larger projects, declare each Dag in its own module and keep one Airflow
entrypoint that serves them all:

```ts
import { salesDag } from "./sales/dag";
import { billingDag } from "./billing/dag";
import { DagRegistry, serveDags } from "apache-airflow-ts-sdk";

await serveDags(new DagRegistry(salesDag, billingDag));
```

A bundle that collects its Dags across several modules can add them
incrementally with `registry.register(...)` instead of passing them all to the
constructor.

Airflow launches the bundled entrypoint with `--comm=host:port` and
`--logs=host:port`. `serveDags()` connects to those sockets, receives the task
startup message, finds the registered handler for the Dag/task pair, and
reports the terminal task state back to Airflow.

See [`example/`](example/) for a coordinator-runtime example that packs a
bundle with `airflow-ts-pack` and uses a Python stub Dag.

## Packing bundles

`airflow-ts-pack` produces everything `NodeCoordinator` needs in one command.
Packing is build-time only, so `esbuild` is an optional peer dependency the
runtime install skips:

```bash
npm install --save-dev esbuild
airflow-ts-pack src/main.ts --outdir dist
```

It bundles the entrypoint into `dist/bundle.mjs` with esbuild, runs the
bundle with `--airflow-metadata` so the bundle reports its own registered
Dag/task pairs and supervisor schema version, and embeds that manifest in the
bundle as a leading `//# airflowMetadata=<base64>` comment. The result is a
single deployable file whose metadata cannot drift from its code; no
hand-written sidecar is needed.

Options:

- `--outdir <dir>` — output directory (default `dist`)
- `--source <name>` — display name of the primary source file shown in the
  Airflow UI (default: entry basename)

## TaskClient

Every task handler receives a `TaskClient` for task-time Airflow data access:

| Method                                           | Description         |
| ------------------------------------------------ | ------------------- |
| `getVariable(key)` / `getVariableOrThrow`        | Airflow Variables   |
| `getXCom(opts)` / `setXCom(opts)`                | XCom read/write     |
| `getConnection(connId)` / `getConnectionOrThrow` | Airflow Connections |

Locator fields such as `dagId`, `runId`, and `taskId` default to the
current task context when omitted.

## Cancellation

`ctx.signal` is an `AbortSignal` controlled by the active runtime. Pass it to
`fetch()`, timers, database clients, child processes, or any other API that
accepts an abort signal so tasks can clean up cooperatively when Airflow
terminates the task subprocess with SIGTERM or SIGINT.

## Development

```bash
pnpm install
pnpm test
pnpm run typecheck
pnpm run build
```

The committed lockfile and `pnpm-workspace.yaml` define the dependency security
policy. Newly released dependency versions must age for 14 days before they
can enter the lockfile, transitive dependencies cannot use Git or arbitrary
tarball sources, and only explicitly approved dependencies can run lifecycle
build scripts. Review changes to both files together when updating dependencies.

Without a local pnpm install, [prek](https://prek.j178.dev) can compile the SDK
with its own managed node + pnpm toolchain:

```bash
prek run compile-ts-sdk
```

## API reference

The public API reference is generated from the TypeScript sources with
[TypeDoc](https://typedoc.org/) and published to
<https://airflow.apache.org/docs/ts-sdk/stable/>.

Build it locally (runs the pinned toolchain in a Node container, so no local
Node install is needed):

```bash
breeze build-docs --sdk-docs-only --sdk=typescript
```

The rendered site is staged at `generated/_build/docs/ts-sdk/stable/`, alongside
a `stable.txt` holding the version from `ts-sdk/package.json`. To iterate on the
docs directly instead, `npm ci && npm run build` inside `ts-sdk/docs/` writes to
`ts-sdk/docs/_build/html/`, and `npm start` rebuilds on change.

CI builds the reference on every change under `ts-sdk/src/` or `ts-sdk/docs/`,
so a broken docs build fails the PR rather than the release.

### Publishing the API docs

Publishing is a separate, deliberate step — a providers-only publish wave will
not refresh the SDK docs as a side effect. Trigger the *Publish Docs to S3*
workflow for the release ref:

```bash
gh workflow run "Publish Docs to S3" --repo apache/airflow --ref main \
  -f ref=<RELEASE_REF> \
  -f include-docs=ts-sdk \
  -f destination=live
```

Use `destination=staging` first to check the output, then `live`. Confirm that
`https://airflow.apache.org/docs/ts-sdk/stable/` resolves (allow time for cache
invalidation) and that `/docs/ts-sdk/` redirects to it.

## Publishing

The manually dispatched `Release TypeScript SDK` workflow first builds, tests,
and hashes one package tarball without OIDC permissions. Its protected publish
job then either uses [npm's staged-publishing flow](https://docs.npmjs.com/staged-publishing/)
or publishes the formal release directly.

npm's registry supports only one trusted-publisher configuration per package,
so both release paths share a single configuration pinned to a single GitHub
environment:

```bash
npm trust github apache-airflow-ts-sdk \
  --repo apache/airflow \
  --file ts-sdk-release.yml \
  --environment ts-sdk-npm-release \
  --allow-stage-publish \
  --allow-publish
```

Confirm it with `npm trust list apache-airflow-ts-sdk`. A second `npm trust
github` for the same package is rejected, so replace an outdated configuration
with `npm trust revoke` first.

Because the registry cannot express "stage only" and "publish only" as separate
relationships, what separates the two paths is the GitHub environment gate
rather than an npm-side permission split. Require reviewers and prevent
self-review on `ts-sdk-npm-release`, restrict its deployment tags to
`ts-sdk/*`, and protect those tags from updates and deletion with a repository
ruleset. The pinned `--environment` claim means a workflow edit that drops the
gate loses the ability to publish at all.

Create and push a `ts-sdk/<version>` tag whose version exactly matches
`package.json`, then dispatch the workflow on that same tag so npm provenance
names the source commit that produced the tarball. The tag must already exist
on `apache/airflow` before the dispatch below can reference it as `--ref`:

```bash
git tag ts-sdk/1.0.0-beta1 <commit-sha>
git push upstream ts-sdk/1.0.0-beta1
```

To submit the package to npm's private staging area for review, run:

```bash
gh workflow run ts-sdk-release.yml --repo apache/airflow --ref ts-sdk/1.0.0-beta1 \
  -f release_type=staged \
  -f tag=ts-sdk/1.0.0-beta1 \
  -f npm_tag=beta
```

The publish job calls `npm stage publish` only after the unprivileged build job
has uploaded a checksummed tarball. The version is not publicly installable
until a maintainer reviews and approves it with 2FA. The following commands
require npm 11.15 or later:

```bash
npm stage list apache-airflow-ts-sdk
npm stage view <stage-id>
npm stage download <stage-id>
npm stage approve <stage-id>
```

The approval cannot run through the trusted-publisher workflow because npm
requires interactive proof of presence. Reject an unsuitable staged version
with `npm stage reject <stage-id>`. Do not run the formal workflow for a version
that is already staged; approve or reject that staged version instead.

To publish directly without npm's staging review, trigger the formal path:

```bash
gh workflow run ts-sdk-release.yml --repo apache/airflow --ref ts-sdk/1.0.0-beta1 \
  -f release_type=formal \
  -f tag=ts-sdk/1.0.0-beta1 \
  -f npm_tag=beta
```

Use `latest` for stable releases and a non-`latest` tag such as `alpha`,
`beta`, or `rc` for prereleases. The workflow rejects a dispatch that would
move the selected npm dist-tag backward as of when it runs. For a staged
release, the dist-tag only actually moves later, at `npm stage approve` time,
which is not re-validated — approve staged versions in the order they were
requested so the dist-tag does not regress. Both publication paths use
short-lived npm OIDC credentials and automatically publish provenance. After
verifying the trusted-publisher setup, disable token-based publishing and
revoke obsolete npm automation tokens.

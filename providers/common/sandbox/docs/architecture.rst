 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Sandbox executor architecture
=============================

The Common Sandbox provider follows the ``KubernetesExecutor`` boundary: the
executor owns Airflow scheduling state, while a small service-specific layer
translates lifecycle operations to a remote compute API. The package exposes
:class:`~airflow.providers.common.sandbox.executor.BaseSandboxExecutor`, but no
user-selectable generic executor or dynamic driver registry.

.. code-block:: text

    Airflow scheduler
        -> vendor executor
            -> BaseSandboxExecutor
                -> typed command queue
                    -> _SandboxExecutorRunner
                        -> vendor SandboxDriver
                            -> sandbox service
                <- typed result queue

``BaseSandboxExecutor`` owns the scheduler-thread state. It keeps exactly one
``_TaskGeneration`` for each ``TaskInstanceKey`` and never replaces a live
generation. When another command arrives for the same key, the executor first
waits for the earlier generation to reach a conclusive terminal state or be
fenced. Results carry their generation identity, so a late result cannot mutate
the state of a successor.

A malformed or foreign persisted reference is quarantined rather than reset.
Quarantine is intentionally fail-closed and does not self-heal. To recover, a
Deployment Manager must use the provider control plane to identify and remove
every workload that could belong to the task attempt, verify its absence, and
only then clear or reset the task through Airflow's supported UI or CLI. Never
edit ``external_executor_id`` merely to bypass quarantine.

``_SandboxExecutorRunner`` owns the driver, its event loop, and the typed
command and result queues. It runs bounded asynchronous launch, observation,
and cleanup operations without sharing an event-loop-bound client with the
scheduler thread. The common engine also owns polling backoff, status-error
limits, terminal reporting, cancellation, shutdown, and adoption.

The vendor driver is deliberately narrow. It resolves scheduler-side
credentials and translates typed launch, observe, terminate, fence, recovery,
and health operations to one service API. It does not own Airflow task state or
queue policy.

Each task attempt receives a preassigned request UUID. The driver must use it
as a durable correlation key. Once a launch is accepted, the executor persists
a versioned reference containing the stable driver ID and an opaque,
provider-owned handle. A replacement scheduler may observe that handle. If the
handle is incomplete, the driver must recover the exact workload or fence every
resource correlated with the request before Airflow may run a successor.
Handles are persistence metadata, not secret storage: they enter
``external_executor_id`` and may be logged by scheduler state handling.

Users select a concrete vendor executor through Airflow's executor setting or
a multiple-executor alias. Adding another service means adding a driver and
thin executor to that service's provider package; it does not require a change
to Airflow core, this provider, or another vendor provider.

The Docker provider supplies an independently implemented binding for
conformance and end-to-end testing. It exercises the same command queues,
generation identity, adoption, fencing, and terminal-state paths as a remote
driver. Its explicit non-production classification is load-bearing: a second
driver demonstrates extensibility, while the production contract still
requires provider-enforced TTL and durable lifecycle guarantees.

Configuration
-------------

All engine tuning is in ``[common.sandbox]``. It controls queue batch sizes,
polling, bounded launch, observation and cleanup concurrency, error thresholds,
adoption and shutdown deadlines, the maximum provider TTL, retained-sandbox
permission, and the optional startup health check. Revocation starts bounded
asynchronous termination or fencing and returns immediately; the scheduler
leaves the task queued until a later heartbeat observes conclusive cleanup. It
never resets an ambiguous lifecycle result. Vendor sections contain only
connection and sandbox creation defaults. Dag code cannot request a TTL above
``max_ttl_seconds`` or use ``keep=True`` unless ``allow_keep`` is enabled by the
deployment.

Security boundary
-----------------

The common engine protects Airflow runtime environment variables from task
overrides. Drivers resolve control-plane credentials in the scheduler and send
only the Task SDK workload, its task-scoped Execution API token, and explicitly
allowed task environment to the sandbox.

Sandbox images must not contain long-lived secrets. Restrict outbound network
access to the Airflow Execution API, required task services, and the configured
remote log store. Egress controls belong to deployment configuration and must
not be weakened by per-task executor configuration. Retained sandboxes may
preserve task data and should be used only for controlled debugging.

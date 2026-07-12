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

Implement a sandbox driver
==========================

A vendor provider implements a
:class:`~airflow.providers.common.sandbox.driver.SandboxDriver` and a thin
:class:`~airflow.providers.common.sandbox.executor.BaseSandboxExecutor`
subclass. The common engine owns task generations, typed queues, concurrency,
backoff, and Airflow state changes. The driver owns only validation and API
translation.

Required contract
-----------------

``launch``
    Asynchronously start the exact argv and allowlisted environment in one
    isolated resource. Correlate every created resource with the supplied
    request UUID and return a small, versioned, JSON-serializable handle. The
    handle is persisted in ``TaskInstance.external_executor_id`` and may appear
    in scheduler logs, so it must contain identifiers only, never credentials,
    signed URLs, or bearer tokens.

``validate_handle``
    Validate the handle schema and request UUID without performing I/O. Raise
    :class:`~airflow.providers.common.sandbox.exceptions.SandboxInvalidHandleError`
    for an unsupported version or inconsistent identity.

``get_status``
    Asynchronously translate a provider state to ``PENDING``, ``RUNNING``,
    ``SUCCEEDED``, ``FAILED``, or conclusive ``GONE``. A missing command record
    alone is not conclusive if its enclosing sandbox may still run. Raise on
    transport errors, malformed responses, and unknown states; uncertainty must
    never become ``GONE`` or a synthetic task failure.

``terminate``
    Asynchronously and idempotently stop and remove the resource represented by
    a complete handle. An already absent resource is success.

``fence``
    Asynchronously and idempotently stop every resource correlated with the
    request UUID. Return only after the service conclusively guarantees that no
    correlated workload can still run.

``recover``
    Optionally recover an exact handle after a scheduler crash. Return
    :class:`~airflow.providers.common.sandbox.models.RecoveredSandbox`, including
    the original retention policy. Without conclusive recovery, fence the
    request and return ``None``.

``health_check`` and ``close``
    Asynchronously validate the control plane and release driver-owned clients.

Output retrieval is optional and diagnostic only. Task logs must use Airflow
remote logging because a sandbox may disappear immediately after completion.

Onboarding requirements
-----------------------

A service can be onboarded only when its driver can provide all of these safety
properties:

* Deterministic request correlation for every resource created by one task
  attempt.
* Non-blocking launch and observation suitable for bounded asynchronous
  concurrency.
* A conclusive ``GONE`` decision; an inconclusive lookup must raise an error.
* Idempotent termination by complete handle and idempotent fencing by request
  UUID.
* A provider-enforced hard TTL independent of the scheduler process.
* Versioned handles that remain readable across supported provider versions.
* Finite client deadlines for every launch, observation, termination, fencing,
  recovery, and health operation. An awaitable call without a deadline is not
  bounded and can permanently consume common-engine capacity.

Exact-handle recovery and provider-captured output are optional. If recovery is
not possible, fencing is mandatory. If a service cannot satisfy correlation,
conclusive absence, fencing, or hard-TTL requirements, it is not safe to bind to
this engine.

Daytona, Tensorlake, E2B, exe.dev, and other sandbox services can each be
evaluated against this contract. Their mention here is not a claim that their
current APIs satisfy it.

Non-production adapters
-----------------------

A provider may also use the engine as a conformance or end-to-end test adapter
without claiming the production contract. Such an executor must set
``is_production = False``, require an explicit non-production opt-in, document
every missing guarantee, and remain disabled by default. An adapter without a
provider TTL must also set ``requires_terminal_cleanup = True`` so ordinary
task completion cannot be reported until teardown is confirmed or fenced. It
must reject ``keep=True`` because it cannot bound retained-resource lifetime.
It does not lower the onboarding bar above.

The Docker provider's ``DockerSandboxExecutor`` is the reference example. It
proves that a second implementation can bind to the same typed driver boundary,
but Docker Sandboxes currently lack a provider-enforced hard TTL and a durable
execution record. The adapter is therefore suitable only for development,
driver conformance, and end-to-end tests, not production task execution.

The driver must also keep control-plane credentials in the scheduler, validate
all vendor ``executor_config`` fields through typed value objects, and never
forward arbitrary task mappings to a provider API. A launch with an ambiguous
outcome must be fenced rather than blindly retried.

Concrete executor binding
-------------------------

The vendor provider depends on ``apache-airflow-providers-common-sandbox`` and
defines one thin
:class:`~airflow.providers.common.sandbox.executor.BaseSandboxExecutor`
subclass. Set ``driver_id`` to a stable identifier. Implement:

``get_driver_factory``
    Resolve the Airflow connection in the scheduler process and return a
    zero-argument factory. ``_SandboxExecutorRunner`` calls it on its own thread,
    so capture immutable client configuration, not an event-loop-bound client.

``build_launch_config``
    Allowlist and validate portable and vendor task configuration, then return
    :class:`~airflow.providers.common.sandbox.models.SandboxLaunchConfig`.
    ``provider_config`` deliberately crosses the runner boundary as a
    JSON object; build a typed vendor value object first and serialize only its
    validated fields.

The common engine applies deployment retention policy after the vendor builds
the launch: ``ttl_seconds`` is capped by ``[common.sandbox] max_ttl_seconds`` and
``keep=True`` is rejected unless the deployment enables ``allow_keep``.

The base reads all engine options from ``[common.sandbox]``. The vendor section
contains only its connection, resource, image, and policy defaults. Register
only the concrete executor in the vendor's ``provider.yaml``. The Common
Sandbox provider does not register an executor.

Users can then select the vendor executor through Airflow's normal executor
configuration or a multiple-executor alias. A second vendor adds a second
provider package, driver, and concrete executor; it does not modify Airflow core
or the Islo provider.

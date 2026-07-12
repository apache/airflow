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

A vendor provider implements
:class:`~airflow.providers.common.sandbox.driver.SandboxDriver` and a thin
subclass of
:class:`~airflow.providers.common.sandbox.executor.BaseSandboxExecutor`. Only
the concrete executor is registered in the vendor's ``provider.yaml``.

Required operations
-------------------

``launch``
    Start the exact argv and environment in one isolated resource. Return a
    small JSON-serializable :class:`~airflow.providers.common.sandbox.models.SandboxHandle`.

``validate_handle``
    Validate the provider handle and its request UUID without performing I/O.
    Raise
    :class:`~airflow.providers.common.sandbox.exceptions.SandboxInvalidHandleError`
    for an unsupported schema or inconsistent identity. This lets adoption fence
    corrupt or stale references instead of polling them forever.

``get_status``
    Return only ``PENDING``, ``RUNNING``, ``SUCCEEDED``, ``FAILED``, or confirmed
    ``GONE``. Raise on transport errors and unknown provider states; uncertainty
    must not become a synthetic Airflow task failure.

``terminate``
    Idempotently stop and remove the workload represented by a complete handle.
    A missing resource is success.

``fence``
    Idempotently stop every possible workload associated with the Airflow
    request UUID. It must return only when no such workload can still run.

``recover``
    Optionally recover the exact handle after a scheduler crash. Return
    :class:`~airflow.providers.common.sandbox.models.RecoveredSandbox`, including
    the original ``keep`` policy. The safe default fences the request and returns
    ``None``.

``health_check`` and ``close``
    Validate the control plane and release driver-owned transports.

Provider-captured output is optional diagnostics. Task logs must use Airflow
remote logging because sandbox resources are ephemeral.

Safety requirements
-------------------

* Use the Airflow request UUID as a provider request ID, deterministic name,
  label, or equivalent lookup key.
* Never blindly retry a non-idempotent command submission. Fence the whole
  sandbox when its outcome is ambiguous.
* Keep provider control-plane credentials in the scheduler. Never inject them
  into the sandbox workload.
* Validate provider-specific ``executor_config`` keys in the concrete provider.
  Do not forward arbitrary user mappings to a provider API.
* Supply a hard resource TTL whenever the provider supports one.
* Version the provider-owned JSON handle. The common envelope has its own
  version, but vendor providers release independently and must be able to decode
  handles written by versions they still support.

Concrete executor binding
-------------------------

The vendor provider depends on ``apache-airflow-providers-common-sandbox`` and
defines one thin
:class:`~airflow.providers.common.sandbox.executor.BaseSandboxExecutor`
subclass. Set ``driver_id`` to the driver's stable identifier and
``config_section`` to the provider's Airflow configuration section. Implement:

``get_driver_factory``
    Resolve the Airflow connection in the scheduler process and return a
    zero-argument factory. The factory is called on the executor's asynchronous
    manager thread, so capture only immutable client configuration, never an
    event-loop-bound client.

``build_launch_config``
    Allowlist and validate portable and vendor task configuration, then return
    :class:`~airflow.providers.common.sandbox.models.SandboxLaunchConfig`.
    ``provider_config`` deliberately crosses the manager-thread boundary as a
    JSON object; build a typed vendor value object first and serialize only its
    validated fields.

The base reads polling, adoption, shutdown, batch-size, and concurrency options
from ``config_section``. A vendor should expose the options used by the base in
its ``provider.yaml`` together with its own launch defaults. Register only the
concrete executor in that provider's ``executors:`` metadata. The Common Sandbox
provider does not register an executor or maintain a central vendor registry.

Users can then select the vendor executor through Airflow's normal executor
configuration or a multiple-executor alias. A second vendor adds a second
provider package, driver, and concrete executor; it does not modify Airflow core
or the Islo provider.

Images, snapshots, file transfer, pause/resume, bulk status streams, and
process-specific cancellation are intentionally outside the first driver
contract. Concrete providers may expose them in their own launch configuration.

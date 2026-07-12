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

Sandbox isolation is an executor concern. The common package therefore exposes
:class:`~airflow.providers.common.sandbox.executor.BaseSandboxExecutor`, but
does not register a user-selectable ``SandboxExecutor``. Each service provider
owns a concrete executor and driver in its own provider package. The Islo
provider is the first implementation.

This is analogous to ``KubernetesExecutor`` separating the Airflow scheduler
from its worker-pod API. The common engine replaces pods with an opaque sandbox
workload handle while preserving the lifecycle that matters to Airflow:

.. code-block:: text

    Airflow scheduler
        -> provider executor (for example, IsloExecutor)
            -> BaseSandboxExecutor state machine
                -> provider-owned SandboxDriver
                    -> isolated sandbox service

The common engine owns workload queueing, the standard Task SDK command,
bounded asynchronous launch and polling, retry backoff, terminal state
reporting, cleanup, revocation, and scheduler adoption. A driver owns
credentials, provider API calls, deterministic resource lookup, and status
translation.

Airflow pre-assigns a UUID to each task try. Drivers use it as their durable
correlation key. After launch, the executor persists a versioned reference that
contains the driver ID and an opaque JSON handle. A replacement scheduler can
poll a complete handle. If only the UUID was persisted, the driver must recover
the exact workload or fence it before Airflow may reschedule the task.

Users select concrete executors through Airflow's existing executor setting or
multiple-executor aliases. There is no sandbox operator, decorator, task type,
backend registry, or arbitrary driver import path.

Security boundary
-----------------

The common engine protects Airflow runtime environment variables from task
overrides and never handles provider credentials. Concrete drivers resolve
control-plane credentials in the scheduler and send only the Task SDK workload,
its task-scoped Execution API token, and explicitly configured task environment
to the sandbox.

Sandbox images must not contain long-lived secrets. Restrict outbound network
access to the Airflow Execution API, required task services, and the configured
remote log store. Retained sandboxes may preserve task data and should be used
only for controlled debugging.

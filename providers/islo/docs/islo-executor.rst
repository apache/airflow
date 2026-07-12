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

Islo Executor
=============

The :class:`~airflow.providers.islo.executors.islo_executor.IsloExecutor`
creates one Islo sandbox for each task attempt and runs the standard Airflow Task
SDK command in it. It is a thin binding between the shared Common Sandbox
executor engine and the Islo API, analogous to ``KubernetesExecutor`` using a
provider-specific API behind the Airflow executor lifecycle.

Requirements
------------

* Apache Airflow 3.3 or newer.
* An :doc:`Islo connection <connections>`.
* Remote task logging. Sandboxes are deleted after terminal state, so their local
  files are not a durable log store.
* An OCI image or Islo snapshot containing the same Airflow Task SDK version as
  the scheduler, the Dag bundle dependencies, and the task dependencies.
* Network reachability from the sandbox to Airflow's Execution API and the remote
  log store. Configure an Islo gateway profile when egress must be restricted.

Configure Airflow
-----------------

.. code-block:: ini

    [core]
    executor = airflow.providers.islo.executors.islo_executor.IsloExecutor

    [logging]
    remote_logging = True
    remote_base_log_folder = s3://my-airflow-logs

    [islo]
    conn_id = islo_default
    default_snapshot_name = airflow-runtime

    [common.sandbox]
    launch_concurrency = 32
    status_concurrency = 128

Use ``default_image`` instead of ``default_snapshot_name`` to start from an OCI
image. Configure exactly one default source. The ``[islo]`` section contains
only the Islo connection, image or snapshot, resource, workdir, timeout, TTL,
gateway, and network defaults. Polling, batching, error limits, concurrency,
health checks, adoption, and shutdown are configured once in
``[common.sandbox]`` for every sandbox driver.

To keep a lower-latency executor as the default and route only selected tasks to
Islo, configure an executor alias:

.. code-block:: ini

    [core]
    executor = LocalExecutor,islo:airflow.providers.islo.executors.islo_executor.IsloExecutor

Then select the alias with Airflow's standard task-level executor field:

.. code-block:: python

    from airflow.sdk import task


    @task(executor="islo")
    def generated_code() -> None:
        run_generated_code()

Per-task overrides
------------------

Use Airflow's standard ``executor_config`` field; no Islo-specific task type is
required:

.. code-block:: python

    from airflow.sdk import task


    @task(
        executor_config={
            "sandbox": {
                "timeout_seconds": 21600,
                "ttl_seconds": 21600,
            },
            "islo": {
                "snapshot_name": "genomics-runtime",
                "vcpus": 8,
                "memory_mb": 32768,
            },
        }
    )
    def simulate(initial_condition: float) -> float:
        return run_simulation(initial_condition)

Portable ``sandbox`` keys are ``timeout_seconds``, ``ttl_seconds``, ``env``,
``workdir``, and ``keep``. Islo-specific keys are ``image``, ``snapshot_name``,
``vcpus``, ``memory_mb``, and ``disk_gb``. Both namespaces are allowlisted. Task
overrides cannot replace Airflow runtime environment variables or alter
``default_gateway_profile`` and ``internet_enabled``. Egress is deployment
policy, not Dag author policy. Setting ``keep`` skips terminal deletion for
debugging, but the provider-enforced ``ttl_seconds`` still applies. Deployments
bound task-requested TTLs with ``[common.sandbox] max_ttl_seconds`` and must
explicitly enable ``allow_keep`` before Dag code can retain a sandbox.

Islo treats the command timeout as a compatibility hint rather than a hard
deadline. Airflow cancellation fences the sandbox, and ``ttl_seconds`` remains
the provider-side cleanup bound. Configure both for the workload and quota
policy; do not rely on the command timeout alone to stop a hung process. The
client accepts only the API's documented ``201 Created`` response as proof that
the create request, including ``lifecycle.delete_after``, was accepted.

Security considerations
-----------------------

The Islo API key is resolved from an Airflow connection by the scheduler and
is never sent to a task sandbox. Store the connection in an Airflow secrets
backend and scope the Islo tenant credentials to the required project.

Each sandbox receives a short-lived Airflow Execution API token as part of the
standard Task SDK workload. The Islo control plane can therefore observe the
submitted command. Use TLS endpoints, limit Execution API token lifetime, and
restrict Islo administrative access.

Generated or untrusted code can still make outbound requests with credentials
explicitly supplied to the task. Set ``default_gateway_profile`` and
``internet_enabled`` in deployment configuration, and use an image or snapshot
without embedded secrets. Dag code cannot weaken those controls. The executor
also rejects attempts to override ``AIRFLOW_*`` runtime variables.

Scheduling, recovery, and scale
-------------------------------

Airflow preassigns a request UUID before dispatch. Islo uses it as the request ID
and deterministic sandbox name. After command acceptance, the executor persists
a versioned reference containing the sandbox and command IDs. A replacement
scheduler can adopt that exact generation. An incomplete launch is fenced by
request ID before a successor may start.

The shared runner performs bounded asynchronous launches, observations, and
cleanup. A terminal Islo status or conclusive absence may finish a task attempt;
transport errors, malformed responses, and an execution lookup that leaves the
sandbox state uncertain may not. Tune ``[common.sandbox]`` concurrency and batch
sizes against Islo quotas and measured latency. The connection's
``max_response_bytes`` setting also bounds the command-result response buffered
by each observation.

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
creates one Islo sandbox for each task try, starts the standard Airflow Task SDK
workload entrypoint in that sandbox, reports the terminal exit state, and deletes
the sandbox. This is the same executor-level separation used by
``KubernetesExecutor`` for worker pods, with Islo sandboxes as the compute unit.
It binds the provider-neutral state machine from the Common Sandbox provider to
the Islo API; no dynamic provider selector is involved.

Requirements
------------

* Apache Airflow 3.3 or newer.
* An :doc:`Islo connection <connections>`.
* Remote task logging. Sandboxes are deleted after terminal state, so their local
  files are not a durable log store.
* An OCI image or Islo snapshot containing the same Airflow Task SDK version as
  the scheduler, the Dag bundle dependencies, and the task's Python dependencies.
* Network reachability from the sandbox to Airflow's Execution API and the remote
  log store. Use an Islo gateway profile when egress must be restricted.

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
    launch_concurrency = 32
    status_concurrency = 128

Use ``default_image`` instead of ``default_snapshot_name`` to start from an OCI
image. Configure exactly one of ``default_image``, ``default_snapshot_name``, or
``default_snapshot_url``. Snapshots are useful when a prepared filesystem and
dependencies should be forked for many task variations.

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
``snapshot_url``, ``vcpus``, ``memory_mb``, ``disk_gb``, ``gateway_profile``,
and ``internet_enabled``. Both namespaces are allowlisted. Task overrides cannot
replace Airflow runtime environment variables. Setting ``keep`` skips deletion
at terminal state for debugging, but the hard ``ttl_seconds`` lifecycle policy
still applies.

Islo currently documents ``timeout_secs`` as an API compatibility hint rather
than a server-enforced command deadline. Airflow task cancellation fences the
whole sandbox, and ``ttl_seconds`` remains the hard provider-side cleanup bound.
Set both values for the workload and quota policy you need; do not rely on the
Islo timeout hint alone to stop a hung process.

Security considerations
-----------------------

The Islo access key is resolved from an Airflow connection by the scheduler and
is never sent to a task sandbox. Store the connection in an Airflow secrets
backend and scope the Islo tenant credentials to the required project.

Each sandbox receives a short-lived Airflow Execution API token as part of the
standard Task SDK workload. The Islo control plane can therefore observe the
submitted command. Use TLS endpoints, limit Execution API token lifetime, and
restrict Islo administrative access.

Generated or untrusted code can still make outbound requests with credentials
explicitly supplied to the task. Prefer a gateway profile with an allowlisted
egress policy, and use an image or snapshot without embedded secrets. The
executor rejects task attempts to override ``AIRFLOW_*`` runtime variables.

Scheduling, recovery, and scale
-------------------------------

Airflow pre-assigns a UUID before dispatch; the executor uses it as Islo's
request ID and deterministic sandbox name. After command acceptance, a versioned
Common Sandbox reference containing the Islo sandbox and command IDs is persisted
in ``external_executor_id`` so a replacement scheduler can adopt the task. If a
scheduler dies during command submission, the Islo driver fences the whole named
sandbox before allowing a retry. This avoids two copies of generated or untrusted
code running concurrently even though Islo does not currently expose idempotent
command submission.

Sandbox creation and status requests run on a dedicated asynchronous I/O loop with
separate concurrency bounds. Throttling and transient server failures use bounded
backoff. Status polling errors do not become a synthetic task failure; only a
confirmed missing sandbox or a terminal Islo execution state changes a task that
has already launched.

Islo currently exposes per-execution status requests rather than a batch status
stream. Size ``status_concurrency``, ``status_batch_size``, and Airflow
``parallelism`` for the service quota and measured polling latency.

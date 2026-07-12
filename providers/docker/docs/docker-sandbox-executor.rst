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

Docker Sandboxes Executor
=========================

The :class:`~airflow.providers.docker.sandbox.executor.DockerSandboxExecutor`
runs an Airflow task attempt in a local Docker Sandbox. It is the second binding
for the Common Sandbox executor engine and an end-to-end test target for the
provider-neutral driver contract.

.. warning::

   This executor is experimental and deliberately marked
   ``is_production = False``. Docker Sandboxes do not provide a
   provider-enforced hard TTL or a durable execution API. A scheduler, host, or
   local daemon failure can leave a workload behind. The task also shares the
   scratch mount that carries status, so task code can alter its local status
   record. Use this adapter only for development, conformance, and end-to-end
   testing. It is not a production executor or a multi-tenant security boundary.

Requirements
------------

* Apache Airflow 3.3 or newer.
* A Linux ``amd64`` host with KVM available to the scheduler host. A virtualized
  host must expose nested virtualization.
* A local Docker Sandboxes daemon and the standalone ``sbx`` CLI version 0.35.0
  or newer, installed and authenticated for the scheduler service account.
* ``apache-airflow-providers-docker`` installed with the Common Sandbox extra:

  .. code-block:: console

      pip install 'apache-airflow-providers-docker[common.sandbox]'

* Remote task logging, because sandbox and scratch files are not durable logs.
* A dedicated absolute scratch root owned only by the scheduler service account.
* A non-interactive, deployment-controlled network policy that permits the
  Airflow Execution API, remote log store, and any task services.

Follow the Docker Sandboxes platform documentation for host installation and
KVM checks. Verify ``sbx version`` reports at least 0.35.0 and ``sbx ls --json``
works as the same operating-system user that runs the scheduler.

Build a task template
---------------------

The template must extend ``docker/sandbox-templates:shell`` and contain Python,
the same Airflow task runtime and provider versions as the scheduler, the Dag
bundle dependencies, and the dependencies imported by tasks. For example:

.. code-block:: dockerfile

    FROM docker/sandbox-templates:shell

    ARG AIRFLOW_VERSION
    ARG DOCKER_PROVIDER_VERSION
    RUN python -m pip install \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "apache-airflow-providers-docker[common.sandbox]==${DOCKER_PROVIDER_VERSION}"

Register or publish the template through the installed ``sbx`` workflow and use
its identifier as ``default_template``. Pin real versions when building the
template. Do not bake Airflow connections, cloud credentials, or other
long-lived secrets into it.

Configure the host
------------------

Create a private root on a local filesystem before starting the scheduler:

.. code-block:: console

    install -d -m 0700 -o airflow -g airflow /var/lib/airflow/docker-sandbox

Configure the local daemon's network policy during host provisioning; it is not
a per-Dag setting. Then configure Airflow:

.. code-block:: ini

    [core]
    executor = airflow.providers.docker.sandbox.executor.DockerSandboxExecutor

    [logging]
    remote_logging = True
    remote_base_log_folder = s3://my-airflow-test-logs

    [docker_sandbox]
    allow_non_production = True
    workspace_root = /var/lib/airflow/docker-sandbox
    sbx_binary = /usr/local/bin/sbx
    default_template = airflow-sandbox
    default_cpus = 2
    default_memory = 4g
    default_timeout_seconds = 3600

    [common.sandbox]
    launch_concurrency = 4
    status_concurrency = 16

``allow_non_production`` is a required, explicit acknowledgement; the executor
refuses to start when it is false. ``workspace_root`` and ``default_template``
are also required. See :doc:`configurations-ref` for every option.

Per-task overrides
------------------

The executor accepts the portable ``sandbox`` namespace and a narrow
``docker_sandbox`` namespace:

.. code-block:: python

    from airflow.sdk import task


    @task(
        executor_config={
            "sandbox": {
                "env": {"EXPERIMENT": "candidate-a"},
                "workdir": "/opt/airflow",
                "timeout_seconds": 900,
                "ttl_seconds": 900,
            },
            "docker_sandbox": {
                "template": "airflow-sandbox-science",
                "cpus": 4,
                "memory": "8g",
            },
        }
    )
    def validate_candidate() -> str:
        return run_validation()

Portable keys are ``env``, ``workdir``, ``timeout_seconds``, and
``ttl_seconds``. Docker-specific keys are only ``template``, ``cpus``, and
``memory``. Dag code cannot select the CLI binary, scratch root, or network
policy. ``AIRFLOW_*`` environment keys are reserved by the executor. The common
deployment policy caps requested TTL values. This adapter rejects ``keep=True``
unconditionally because Docker Sandboxes cannot enforce a retained-resource
deadline.

For this adapter, a missing ``ttl_seconds`` defaults to ``timeout_seconds`` to
form a valid portable request. The in-sandbox supervisor enforces the command
deadline, but ``ttl_seconds`` does not bound the lifetime of the sandbox itself.
It cannot protect against scheduler, host, or daemon loss.

Lifecycle and scratch protocol
------------------------------

Airflow assigns a request UUID before launch. The driver uses the deterministic
name ``airflow-<UUID>`` and creates ``<workspace_root>/<UUID>`` with mode
``0700``. It writes strict, versioned ``launch.json``, ``metadata.json``, and
``status.json`` files with mode ``0600``. The launch specification carries task
environment and the short-lived Execution API token in the file, never in CLI
arguments.

The driver starts the in-sandbox supervisor with ``sbx exec -d`` and does not
accept the launch until the supervisor publishes matching state. Scheduler
restart recovery can check the deterministic sandbox identity while its scratch
metadata still exists. Ambiguous generations are fenced before a successor can
run. Because there is no provider TTL, the executor also requires confirmed
teardown before it reports an ordinary terminal task state; cleanup errors are
fenced rather than dropped.

These checks make the adapter valuable for end-to-end validation, but the local
files and ``sbx ls`` are not a durable provider execution record. Because the
task can write to its mounted workspace, the scratch status is a coordination
mechanism, not a trusted attestation of task outcome. A scheduler crash after
local teardown but before the terminal Airflow state is persisted can also lose
the terminal record and cause the attempt to be failed or retried.

See :doc:`security` before enabling the executor.

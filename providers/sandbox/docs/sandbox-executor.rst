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

Sandbox Executor
================

The ``SandboxExecutor`` runs each Airflow task instance in an ephemeral cloud
sandbox behind a pluggable provider layer. It implements the public
``BaseExecutor`` interface (AIP-51) and follows the Airflow 3 Task SDK topology:
the in-sandbox supervisor heartbeats and ships logs to the api-server, while the
executor reconciles terminal exit state from a polling watcher.

Configuration
-------------

.. code-block:: ini

    [core]
    executor = LocalExecutor,sandbox:airflow.providers.sandbox.executors.sandbox_executor.SandboxExecutor

    [logging]
    remote_logging = True
    remote_base_log_folder = s3://my-airflow-logs/

    [sandbox]
    provider = daytona       # local | daytona | e2b | modal | islo | module:Class
    poll_interval = 5
    creation_batch_size = 8

Backends
--------

==========  ==============  ===========  ==========  ====  ===============
Backend     kind            file upload  async exec  kill  reattach/adopt
==========  ==============  ===========  ==========  ====  ===============
local       delegated-run   yes          yes         yes   no
daytona     delegated-run   yes          yes         yes   yes (labelled)
e2b         delegated-run   yes          yes         yes   no (opaque)
modal       delegated-run   no           yes         yes   no
islo        delegated-run   yes          yes         no    yes (named)
==========  ==============  ===========  ==========  ====  ===============

Per-task sizing/image mirrors the Kubernetes executor's ``pod_override``:

.. code-block:: python

    BashOperator(
        task_id="train",
        bash_command="python train.py",
        executor="sandbox",
        executor_config={"sandbox_override": {"image": "daytona-medium", "cpu": 4, "memory_mb": 8192}},
    )

Limitations
-----------

* One fresh sandbox per task try — seconds of cold start; best for
  long-running, heavyweight, isolation-sensitive tasks, not high-volume short
  tasks.
* ``remote_logging`` is mandatory; ``get_task_log`` is a best-effort fallback.
* Adoption is clean for named/labelled backends (Daytona, islo) and best-effort
  for opaque-handle backends (E2B, Modal).

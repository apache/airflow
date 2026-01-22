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



Web Stack
=========


Configuration
-------------

Sometimes you want to deploy the backend and frontend behind a
variable url path prefix. To do so, you can configure the url :ref:`config:api__base_url`
for instance, set it to ``http://localhost:28080/d12345``. All the APIs routes will
now be available through that additional ``d12345`` prefix. Without rebuilding
the frontend, XHR requests and static file queries should be directed to the prefixed url
and served successfully.

You will also need to update the execution API server url
:ref:`config:core__execution_api_server_url` for tasks to be able to reach the API
with the new prefix.

Separating API Servers
-----------------------

By default, both the Core API Server and the Execution API Server are served together:

.. code-block:: bash

   airflow api-server
   # same as
   airflow api-server --apps all
   # or
   airflow api-server --apps core,execution

If you want to separate the Core API Server and the Execution API Server, you can run them
separately. This might be useful for scaling them independently or for deploying them on different machines.

.. code-block:: bash

   # serve only the Core API Server
   airflow api-server --apps core
   # serve only the Execution API Server
   airflow api-server --apps execution

Worker Recycling
----------------

Long-running API server workers can accumulate memory over time. To address this,
Airflow supports rolling worker restarts that periodically recycle workers while
maintaining zero downtime.

When enabled via :ref:`config:api__worker_refresh_interval`, the API server spawns
new workers and health-checks them *before* killing old workers, ensuring continuous
availability.

.. code-block:: ini

   [api]
   # Recycle workers every 30 minutes (1800 seconds)
   worker_refresh_interval = 1800
   # Number of workers to refresh at a time
   worker_refresh_batch_size = 1

Or via environment variables:

.. code-block:: bash

   export AIRFLOW__API__WORKER_REFRESH_INTERVAL=1800
   airflow api-server --workers 2

The rolling restart pattern works as follows:

1. Spawn ``worker_refresh_batch_size`` new workers (via ``SIGTTIN`` signal)
2. Wait for new workers to pass health checks
3. Kill the same number of old workers (via ``SIGTTOU`` signal)
4. If new workers fail health checks, roll back by killing the new workers

This ensures at least the expected number of healthy workers are always available.

.. note::

   When running with ``--workers 1``, uvicorn operates in single-process mode without a
   process supervisor. Rolling restarts still work but briefly scale up to 2 workers
   during the refresh cycle to enable signal-based worker management. For true zero-downtime
   with no capacity changes, use ``--workers 2`` or more.

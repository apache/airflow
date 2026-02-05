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

Server Types
------------

The API server supports two server types: ``uvicorn`` (default) and ``gunicorn``.

Uvicorn (Default)
~~~~~~~~~~~~~~~~~

Uvicorn is the default server type. It's simple to set up and works on all platforms including Windows.

.. code-block:: bash

   airflow api-server

Gunicorn
~~~~~~~~

Gunicorn provides additional features for production deployments:

- **Memory sharing**: Workers share memory via copy-on-write after fork, reducing total memory usage
- **Rolling worker restarts**: Zero-downtime worker recycling to prevent memory accumulation
- **Proper signal handling**: SIGTTOU kills the oldest worker (FIFO), enabling true rolling restarts

.. note::

   Gunicorn requires the ``gunicorn`` extra: ``pip install 'apache-airflow-core[gunicorn]'``

   Gunicorn is Unix-only and does not work on Windows.

To enable gunicorn mode:

.. code-block:: bash

   export AIRFLOW__API__SERVER_TYPE=gunicorn
   airflow api-server

Rolling Worker Restarts
^^^^^^^^^^^^^^^^^^^^^^^

To enable periodic worker recycling (useful for long-running processes to prevent memory accumulation):

.. code-block:: bash

   export AIRFLOW__API__SERVER_TYPE=gunicorn
   export AIRFLOW__API__WORKER_REFRESH_INTERVAL=43200  # Restart workers every 12 hours
   export AIRFLOW__API__WORKER_REFRESH_BATCH_SIZE=1   # Restart one worker at a time
   airflow api-server

The rolling restart process:

1. Spawns new workers before killing old ones (zero downtime)
2. Waits for new workers to be ready (process title check)
3. Performs HTTP health check to verify workers can serve requests
4. Kills old workers (oldest first)
5. Repeats until all original workers are replaced

Configuration Options
^^^^^^^^^^^^^^^^^^^^^

The following configuration options are available in the ``[api]`` section:

- ``server_type``: ``uvicorn`` (default) or ``gunicorn``
- ``worker_refresh_interval``: Seconds between worker refresh cycles (0 = disabled, default)
- ``worker_refresh_batch_size``: Number of workers to refresh per cycle (default: 1)
- ``reload_on_plugin_change``: Reload when plugin files change (default: False)

When to Use Gunicorn
^^^^^^^^^^^^^^^^^^^^

Use gunicorn when you need:

- Long-running API server processes where memory accumulation is a concern
- Multi-worker deployments where memory sharing matters
- Production environments requiring zero-downtime worker recycling

Use the default uvicorn when:

- Running on Windows
- Running in development or testing environments
- Running short-lived containers (e.g., Kubernetes pods that get recycled)

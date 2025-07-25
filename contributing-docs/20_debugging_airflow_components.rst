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

Debugging Airflow Components with Breeze
========================================

Breeze provides debugging support for Airflow components using the ``--debug`` and ``--debugger`` flags
in the ``breeze start-airflow`` command.

Starting Airflow with Debug Support
----------------------------------

To start Airflow with debugging enabled, use the ``--debug`` flag to specify which components you want to debug:

.. code-block:: bash

    # Debug the scheduler
    breeze start-airflow --debug scheduler

    # Debug multiple components
    breeze start-airflow --debug scheduler --debug triggerer

    # Debug all components
    breeze start-airflow --debug scheduler --debug triggerer --debug api-server --debug dag-processor

    # Debug with CeleryExecutor
    breeze start-airflow -b postgres -P 17 --executor CeleryExecutor  --debug scheduler --debug dag-processor --debug api-server --debug triggerer --debug celery-worker

    # Debug Webserver for Airflow 2.x
    breeze start-airflow --debug webserver

Available Components for Debugging
----------------------------------

* **scheduler** - The Airflow scheduler that monitors DAGs and triggers task instances
* **triggerer** - The triggerer service that handles deferred tasks and triggers
* **api-server** - The Airflow REST API server
* **dag-processor** - The DAG processor service (when using standalone DAG processor)
* **edge-worker** - The edge worker service (when using EdgeExecutor)
* **celery-worker** - Celery worker processes (when using CeleryExecutor)

Debugger Options
----------------

Breeze supports two debugger options:

* **debugpy** (default)
* **pydevd-pycharm**

.. code-block:: bash

    # Use debugpy (default)
    breeze start-airflow --debug scheduler --debugger debugpy

    # Use PyCharm debugger
    breeze start-airflow --debug scheduler --debugger pydevd-pycharm

Setting up VSCode for Remote Debugging
--------------------------------------

1. **Install Required Extensions**

   Install the following VSCode extensions:
   * Python (ms-python.python)
   * Python Debugger (ms-python.debugpy)

2. **Create Launch Configuration**

   Create or update your ``.vscode/launch.json`` file. The easiest way is to run the setup script:

   .. code-block:: bash

       python setup_vscode.py

   This will create debug configurations for all Airflow components. Here's an example configuration for the scheduler:

   .. code-block:: json

       {
           "name": "Debug Airflow Scheduler",
           "type": "debugpy",
           "request": "attach",
           "justMyCode": false,
           "connect": {
               "host": "localhost",
               "port": 50231
           },
           "pathMappings": [
               {
                   "localRoot": "${workspaceFolder}",
                   "remoteRoot": "/opt/airflow"
               }
           ]
       }

3. **Port Mapping**

   Each component uses a different debug port. These ports are automatically assigned by Breeze
   when you start Airflow with debugging enabled:

   * **Scheduler**: 50231
   * **DAG Processor**: 50232
   * **Triggerer**: 50233
   * **API Server**: 50234
   * **Celery Worker**: 50235
   * **Edge Worker**: 50236
   * **Web Server**: 50237

   These ports are exposed from the Breeze container to your host machine, allowing your IDE
   to connect to the debugger running inside the container.


Debugging Workflow
------------------

1. **Start Airflow with Debug Support**

   .. code-block:: bash

       breeze start-airflow --debug scheduler --debugger debugpy

2. **Set Breakpoints**

   In VSCode, set breakpoints in your Airflow code by clicking in the gutter next to line numbers.

3. **Attach Debugger**

   - Open the Debug panel in VSCode (Ctrl+Shift+D / Cmd+Shift+D)
   - Select the appropriate debug configuration (e.g., "Debug Airflow Scheduler")
   - Click the green play button or press F5

4. **Trigger Debugging**

   Perform an action that will trigger the code path with your breakpoint:

   - For scheduler: Trigger a DAG or wait for scheduled execution
   - For API server: Make an API call
   - For triggerer: Create a deferred task
   - For DAG processor: Parse a DAG file

5. **Debug Session**

   Once the breakpoint is hit:

   - Inspect variables in the Variables panel
   - Use the Debug Console to evaluate expressions
   - Step through code using F10 (step over), F11 (step into), F12 (step out)
   - Continue execution with F5

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

Install Edge Worker on Windows
==============================

.. note::

    Usage of Edge Worker on Windows is experimental. The Edge Worker has only been manually tested on Windows,
    and the setup is not validated in CI. It is recommended to use Linux for Edge Worker. The
    Windows-based setup is intended solely for testing at your own risk. It is technically limited
    due to Python OS restrictions and if currently of Proof-of-Concept quality.


The setup was tested on Windows 10 with Python 3.12.8, 64-bit.
To setup a instance of Edge Worker on Windows, you need to follow the steps below:

1. Install Python 3.9 or higher.
2. Create an empty folder as base to start with. In our example it is ``C:\\Airflow``.
3. Start Shell/Command Line in ``C:\\Airflow`` and create a new virtual environment via: ``python -m venv venv``
4. Activate the virtual environment via: ``venv\\Scripts\\activate.bat``
5. Install Edge provider using the Airflow constraints as of your airflow version via
   ``pip install apache-airflow-providers-edge3 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt``.
   (or alternative build and copy the wheel of the edge provider to the folder ``C:\\Airflow``.
   This document used ``apache_airflow_providers_edge-0.9.7rc0-py3-none-any.whl``, install the wheel file with the
   Airflow constraints matching your Airflow and Python version:
   ``pip install apache_airflow_providers_edge-0.9.7rc0-py3-none-any.whl apache-airflow==2.10.5 virtualenv --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt``)
6. Create a new folder ``dags`` in ``C:\\Airflow`` and copy the relevant DAG files in it.
   (At least the DAG files which should be executed on the edge alongside the dependencies. For testing purposes
   the DAGs from the ``apache-airflow`` repository can be used located in
   <https://github.com/apache/airflow/tree/main/providers/edge3/src/airflow/providers/edge3/example_dags>.)
7. Collect needed parameters from your running Airflow backend, at least the following:

  - ``edge`` / ``api_url``: The HTTP(s) endpoint where the Edge Worker connects to
  - ``core`` / ``internal_api_secret_key``: The shared secret key between the webserver and the Edge Worker
  - Any proxy details if applicable for your environment.

8. Create a worker start script to prevent repeated typing. Create a new file ``start_worker.bat`` in
   ``C:\\Airflow`` with the following content - replace with your settings:

.. code-block:: bash

    @echo off
    set AIRFLOW__CORE__DAGS_FOLDER=dags
    set AIRFLOW__LOGGING__BASE_LOG_FOLDER=edge_logs
    set AIRFLOW__EDGE__API_URL=https://your-hostname-and-port/edge_worker/v1/rpcapi
    set AIRFLOW__CORE__EXECUTOR=airflow.providers.edge3.executors.edge_executor.EdgeExecutor
    set AIRFLOW__CORE__INTERNAL_API_SECRET_KEY=<steal this from your deployment...>
    set AIRFLOW__CORE__LOAD_EXAMPLES=False
    set AIRFLOW_ENABLE_AIP_44=true
    @REM Add if needed: set http_proxy=http://my-company-proxy.com:3128
    @REM Add if needed: set https_proxy=http://my-company-proxy.com:3128
    airflow edge worker --concurrency 4 --queues windows

9. Note on logs: Per default the DAG Run ID is used as path in the log structure and per default the date and time
   is contained in the Run ID. Windows fails with a colon (":") in a file or folder name and this also
   the Edge Worker fails.
   Therefore you might consider changing the config ``AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE`` to avoid the colon.
   For example you could add the Jinja2 template replacement ``| replace(":", "-")`` to use other characters.
   Note that the log filename template is resolved on server side and not on the worker side. So you need to make
   this as a global change.
   Alternatively for testing purposes only you must use Run IDs without a colon, e.g. set the Run ID manually when
   starting a DAG run.
10. Start the worker via: ``start_worker.bat``
    Watch the console for errors.
11. Run a DAG as test and see if the result is as expected.

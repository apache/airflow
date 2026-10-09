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


Quick Start
-----------

Install Airflow on your machine and start it with ``airflow standalone``, which runs the scheduler, Dag
processor, triggerer and API server together on a SQLite database. Use it to try Airflow and to develop
Dags locally, not in production. For other ways to install and run Airflow, see :doc:`/installation/index`.

Before you start
''''''''''''''''

You need a Python version that this Airflow release supports; see :doc:`/installation/prerequisites`.

On Windows, set up WSL2 first: run ``wsl --install`` in PowerShell as administrator and restart, as described
in `Install WSL <https://learn.microsoft.com/en-us/windows/wsl/install>`__. Then run every command below in
the Ubuntu terminal that WSL2 installs.

Install Airflow
'''''''''''''''

Create a virtual environment and install Airflow into it with the
:ref:`constraints file <installation:constraints>`, which pins every dependency to the version this release
was tested with. A later release of a dependency then does not change what you install.

.. tab-set::

    .. tab-item:: uv
        :sync: uv

        If you do not have uv yet, install it from the
        `uv installation guide <https://docs.astral.sh/uv/getting-started/installation/>`__. Then create the
        virtual environment and install Airflow:

        .. code-block:: bash
            :substitutions:

            uv venv
            source .venv/bin/activate

            AIRFLOW_VERSION=|version|
            PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
            CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
            uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

        ``uv venv`` uses a Python it finds on your machine. To choose a supported version instead, pass it
        with ``--python``, and uv downloads it if it is missing.

    .. tab-item:: pip
        :sync: pip

        On Debian and Ubuntu, including WSL2, install the ``venv`` module first with
        ``sudo apt update && sudo apt install python3-venv``. Without it, ``python3 -m venv`` fails with
        ``The virtual environment was not created successfully because ensurepip is not available``.

        Then create the virtual environment and install Airflow:

        .. code-block:: bash
            :substitutions:

            python3 -m venv .venv
            source .venv/bin/activate
            pip install --upgrade pip

            AIRFLOW_VERSION=|version|
            PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
            CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
            pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

If the virtual environment's Python is a version this release does not support, no constraints file exists for
it and the install stops at the constraints URL. pip reports ``ERROR: 404 Client Error: Not Found for url:``
and uv reports ``error: Error while accessing remote requirements file:``, each followed by the URL. Recreate
the virtual environment with a supported Python.

Start Airflow
'''''''''''''

With the virtual environment activated, start every component with one command:

.. code-block:: bash

    airflow standalone

On the first run, Airflow creates ``~/airflow`` with its configuration file ``airflow.cfg`` and a SQLite
database, and generates a password for the ``admin`` user. The password appears near the start of the
output. Once the API server, scheduler, Dag processor and triggerer are all running, Airflow prints a ready
banner. The components' logs fill the lines in between:

.. code-block:: text

    standalone | Starting Airflow Standalone
    2026-10-09T00:16:49.108917Z [warning  ] SimpleAuthManager is active but the deployment shape looks like production (non-sqlite backend, non-local API host, or a distributed executor). ...
    Simple auth manager | Password for user 'admin': KTpeNaPsEqknZGPm
    ...
    standalone | Airflow is ready
    standalone | Airflow Standalone is for development purposes only. Do not use this in production!

The ``SimpleAuthManager is active but the deployment shape looks like production`` warning is expected for a
local standalone run: the API server listens on ``0.0.0.0`` by default, which the warning treats as a
non-local host.

Press ``Ctrl+C`` to stop every component.

Airflow keeps its files in ``~/airflow`` unless you set ``AIRFLOW_HOME``. If you set it, export the same value
in every terminal before you run ``airflow``: a terminal without it uses ``~/airflow``, with a separate
database and password. To start Airflow again later, open a terminal in the directory where you created the
virtual environment, run ``source .venv/bin/activate``, then ``airflow standalone``.

Log in and run a Dag
''''''''''''''''''''

Open ``http://localhost:8080`` and log in as ``admin`` with the password from the
``Password for user 'admin'`` line. Airflow prints that line only on the first run; later runs print a line
saying the password was previously generated, with the path of the file that holds it. To read the file:

.. code-block:: bash

    cat "${AIRFLOW_HOME:-$HOME/airflow}/simple_auth_manager_passwords.json.generated"

It maps each user to their password:

.. code-block:: text

    {"admin": "KTpeNaPsEqknZGPm"}

Airflow loads a set of example Dags, all paused. Open **Dags**, find ``example_bash_operator``, and switch on
its toggle to unpause it. The scheduler starts a run within a few seconds; click the Dag name to follow its
tasks.

What's next
'''''''''''

* :doc:`/tutorial/index` to write your first Dag
* :doc:`/installation/installing-from-pypi` for extras, providers, and how constraints files work
* :doc:`/configurations-ref` for the settings in ``airflow.cfg``
* :ref:`starting-components-separately` to run the API server, scheduler, Dag processor,
  and triggerer as separate processes
* :doc:`/howto/docker-compose/index` or :doc:`helm-chart:index` for container and Kubernetes setups
* :doc:`/administration-and-deployment/production-deployment` when you move beyond standalone
* :doc:`/howto/index` for common configuration tasks

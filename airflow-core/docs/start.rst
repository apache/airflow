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

You need a Python version that this Airflow release supports; see :doc:`/installation/prerequisites`. The
uv tab below uses Python 3.13 and downloads it if needed.

On Windows, set up WSL2 first: run ``wsl --install`` in PowerShell as administrator and restart, as described
in `Install WSL <https://learn.microsoft.com/en-us/windows/wsl/install>`__. Then run every command below in
the Ubuntu terminal that WSL2 installs.

Install Airflow
'''''''''''''''

Create a virtual environment and install Airflow into it with the constraints file. The constraints file pins
every dependency to the version this release was tested with, so a later release of a dependency does not
change what you install.

.. tab-set::

    .. tab-item:: uv
        :sync: uv

        If you do not have uv yet, install it from the
        `uv installation guide <https://docs.astral.sh/uv/getting-started/installation/>`__. Then create a
        virtual environment with Python 3.13, which uv downloads if it is missing, and install Airflow:

        .. code-block:: bash
            :substitutions:

            uv venv --python 3.13
            source .venv/bin/activate
            uv pip install "apache-airflow==|version|" \
                --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.13.txt"

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

        If ``python3`` is a version this release does not support, no constraints file exists for it and the
        install stops with ``ERROR: 404 Client Error: Not Found for url:`` followed by the constraints URL.
        Install a supported Python, or use the uv tab.

If you have uv and only want a quick look, this command downloads Airflow into a
`temporary environment <https://docs.astral.sh/uv/guides/tools/>`__ and starts it, with no virtual environment
to create. It leaves no ``airflow`` command behind, so use the steps above if you plan to follow the tutorials:

.. code-block:: bash
    :substitutions:

    uvx --python 3.13 \
        --constraints "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.13.txt" \
        apache-airflow@|version| standalone

Start Airflow
'''''''''''''

With the virtual environment activated, start every component with one command:

.. code-block:: bash

    airflow standalone

On the first run, Airflow creates ``~/airflow`` with its configuration file ``airflow.cfg`` and a SQLite
database, and generates a password for the ``admin`` user. The output shows the password near the start and
the ready banner once the API server, scheduler, Dag processor and triggerer are all running, with the
components' logs in between:

.. code-block:: text

    standalone | Starting Airflow Standalone
    ...
    Simple auth manager | Password for user 'admin': pxae2mVfufkCzUv6
    ...
    standalone | Airflow is ready
    standalone | Airflow Standalone is for development purposes only. Do not use this in production!

Press ``Ctrl+C`` to stop every component.

To keep Airflow's files somewhere other than ``~/airflow``, export ``AIRFLOW_HOME`` in every terminal before
you run ``airflow``. A terminal without it uses ``~/airflow``, with a separate database and password. To start
Airflow again later, open a terminal in the directory where you created the virtual environment, run
``source .venv/bin/activate``, then ``airflow standalone``.

Log in and run a Dag
''''''''''''''''''''

Open ``http://localhost:8080`` and log in as ``admin`` with the password from the
``Password for user 'admin'`` line. Airflow prints that line only on the first run. Later runs print a line
saying the password was previously generated, with the path of the file that holds it. To read the file,
which is in ``~/airflow`` unless you set ``AIRFLOW_HOME``:

.. code-block:: bash

    cat ~/airflow/simple_auth_manager_passwords.json.generated

It maps each user to their password:

.. code-block:: text

    {"admin": "pxae2mVfufkCzUv6"}

Airflow loads a set of example Dags, all paused. Open **Dags**, find ``example_bash_operator``, and switch on
its toggle to unpause it. The scheduler starts a run within a few seconds; click the Dag name to follow its
tasks.

What's next
'''''''''''

* :doc:`/tutorial/index` to write your first Dag
* :doc:`/installation/installing-from-pypi` for extras, providers, and other install tools
* :doc:`/configurations-ref` for the settings in ``airflow.cfg``
* :ref:`starting-components-separately` to run the API server, scheduler, Dag processor,
  and triggerer as separate processes
* :doc:`/howto/docker-compose/index` or :doc:`helm-chart:index` for container and Kubernetes setups
* :doc:`/administration-and-deployment/production-deployment` when you move beyond standalone
* :doc:`/howto/index` for common configuration tasks

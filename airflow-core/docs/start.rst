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

This guide boots a local Airflow standalone instance on your machine. It uses SQLite and
``SimpleAuthManager``, and is not a production deployment. For Docker Compose, the Helm chart,
or other options, see :doc:`/installation/index`.

Before you start
''''''''''''''''

Airflow requires a supported Python 3 environment. See :doc:`/installation/prerequisites`
for the versions tested with this release.

On Windows, run Airflow in WSL2:

.. code-block:: bash

   wsl --install

On Debian/Ubuntu, Python may enforce externally managed environments (PEP 668). Create and
activate a virtual environment before using ``pip``.

Fastest path
''''''''''''

If you have ``uv`` or ``pipx``, you can start Airflow without a persistent install:

.. tab-set::

    .. tab-item:: uv
        :sync: uv

        Install uv from the `uv installation guide <https://docs.astral.sh/uv/getting-started/installation/>`_.

        .. code-block:: bash

            uvx apache-airflow standalone

    .. tab-item:: pipx
        :sync: pipx

        .. code-block:: bash

            pipx run apache-airflow standalone

This starts a minimal local system with SQLite and an auto-generated admin password.

Install into a virtual environment
''''''''''''''''''''''''''''''''''

Use this path if you want a persistent ``airflow`` command in a virtual environment.

1. **Set Airflow Home (optional)**

   Airflow uses ``~/airflow`` by default. Set ``AIRFLOW_HOME`` before installing if you want a
   different location:

   .. code-block:: bash

      export AIRFLOW_HOME=~/airflow

2. **Create a virtual environment and install Airflow**

   Officially supported tools are ``pip`` and ``uv``. The constraint file pins a tested set of
   dependencies for this Airflow version. For extras, other tools, and more install scenarios,
   see :doc:`/installation/installing-from-pypi`.

   .. tab-set::

       .. tab-item:: uv
           :sync: uv

           .. code-block:: bash
               :substitutions:

               uv venv
               source .venv/bin/activate

               AIRFLOW_VERSION=|version|
               PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
               CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
               uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

       .. tab-item:: pip
           :sync: pip

           .. code-block:: bash
               :substitutions:

               python3 -m venv airflow_venv
               source airflow_venv/bin/activate
               pip install --upgrade pip

               AIRFLOW_VERSION=|version|
               PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
               CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
               pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

3. **Run Airflow standalone**

   This initializes the database, creates a user, and starts all components:

   .. code-block:: bash

      airflow standalone

Open the UI
'''''''''''

Visit ``http://localhost:8080`` and log in with the admin account.

The generated password is stored in ``$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated``.
If it is not printed in the terminal:

.. code-block:: bash

   cat ~/airflow/simple_auth_manager_passwords.json.generated

On the home page, enable the ``example_bash_operator`` Dag.

Optional check
''''''''''''''

Run a single task instance:

.. code-block:: bash

   airflow tasks test example_bash_operator runme_0 2015-01-01

What's next
'''''''''''

Airflow creates ``$AIRFLOW_HOME`` and writes ``airflow.cfg`` with defaults you can override.
See :doc:`/configurations-ref`.

* :doc:`/tutorial/index` for examples
* :doc:`/installation/installing-from-pypi` for constraints, extras, and other tools
* :doc:`/howto/docker-compose/index` or :doc:`helm-chart:index` for container and Kubernetes setups
* :ref:`starting-components-separately` if you want to run the API server, scheduler, Dag processor,
  and triggerer as separate processes
* :doc:`/administration-and-deployment/production-deployment` when you move beyond standalone
* :doc:`/howto/index` for common configuration tasks

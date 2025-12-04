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

This quick start guide will help you bootstrap an Airflow standalone instance on your local machine.

.. note::

   Successful installation requires a Python 3 environment. Starting with Airflow 3.1.0, Airflow supports Python 3.10, 3.11, 3.12, 3.13.

   Officially supported installation methods is with``pip`` or ``uv``.

   Run ``pip install apache-airflow[EXTRAS]==AIRFLOW_VERSION --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-AIRFLOW_VERSION/constraints-PYTHON_VERSION.txt"``, for example ``pip install "apache-airflow[celery]==3.0.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.10.txt"`` to install Airflow in a reproducible way. You can also use - much faster - ``uv`` - by adding ``uv`` before the command.



   While there have been successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` or ``uv`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported.

   If you wish to install Airflow using those tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.

   This guide will help you quickly set up Apache Airflow using ``uv``, a fast and modern tool for managing Python environments and dependencies. ``uv`` makes the installation process easy and provides a
   smooth setup experience.

   If you are on Windows, you have to use WSL2 (Linux environment for Windows).

   .. code-block:: bash

      wsl --install

1. **Set Airflow Home (optional)**:

   Airflow requires a home directory, and uses ``~/airflow`` by default, but you can set a different location if you prefer. The ``AIRFLOW_HOME`` environment variable is used to inform Airflow of the desired location. This step of setting the environment variable should be done before installing Airflow so that the installation process knows where to store the necessary files.

   .. code-block:: bash

      export AIRFLOW_HOME=~/airflow

2.  Install Airflow in a virtual environment using ``uv`` since it is faster alternative that creates the ``venv`` automatically implicitly for you. It is efficient alternative to using ``pip`` and ``venv``.

    .. rst-class:: centered

        Install uv: `uv Installation Guide <https://docs.astral.sh/uv/getting-started/installation/>`_


    For creating virtual environment with ``uv``, refer to the documentation here:
    `Creating and Maintaining Local virtual environment with uv <https://github.com/apache/airflow/blob/main/contributing-docs/07_local_virtualenv.rst#creating-and-maintaining-local-virtualenv-with-uv-recommended>`_

For installation using ``pip`` and ``venv``, carry out following steps:
.. code-block:: bash

   # For Windows after WSL2 install, restart computer, then in WSL Ubuntu terminal
   sudo apt update
   sudo apt install python3-pip python3-venv

   bash # Go to Linux home directory (not Windows mount)
   cd ~

   # Create airflow directory
   mkdir -p ~/airflow
   cd ~airflow

   # Create virtual environment
   python3 -m venv airflow_venv

   # Activate
   source airflow_venv/bin/activate

   # Upgrade pip
   pip install --upgrade pip

   # Install Airflow with correct Python version constraints
   pip install apache-airflow[celery]==3.1.0 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt

   # Verify installation
   airflow version

3. Install Airflow using the constraints file, which is determined based on the URL we pass:

   .. code-block:: bash
      :substitutions:


      AIRFLOW_VERSION=3.1.1

      # Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
      # See above for supported versions.
      PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

      CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
      # For example this would install 3.0.0 with python 3.10: https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.10.txt

      uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

4. Run Airflow Standalone:

   The ``airflow standalone`` command initializes the database, creates a user, and starts all components.

   .. code-block:: bash

      airflow standalone

5. Access the Airflow UI:

   Visit ``localhost:8080`` in your browser and log in with the admin account details shown in the terminal. Enable the ``example_bash_operator`` Dag in the home page.

Upon running these commands, Airflow will create the ``$AIRFLOW_HOME`` folder
and create the "airflow.cfg" file with defaults that will get you going fast.
You can override defaults using environment variables, see :doc:`/configurations-ref`.
You can inspect the file either in ``$AIRFLOW_HOME/airflow.cfg``, or through the UI in
the ``Admin->Configuration`` menu. The PID file for the webserver will be stored
in ``$AIRFLOW_HOME/airflow-api-server.pid`` or in ``/run/airflow/airflow-webserver.pid``
if started by systemd.

As you grow and deploy Airflow to production, you will also want to move away
from the ``standalone`` command we use here to running the components
separately. You can read more in :doc:`/administration-and-deployment/production-deployment`.

Here are a few commands that will trigger a few task instances. You should
be able to see the status of the jobs change in the ``example_bash_operator`` Dag as you
run the commands below.

.. code-block:: bash

    # run your first task instance
    airflow tasks test example_bash_operator runme_0 2015-01-01
    # run a backfill over 2 days
    airflow backfill create --dag-id example_bash_operator \
        --from-date 2015-01-01 \
        --to-date 2015-01-02

If you want to run the individual parts of Airflow manually rather than using
the all-in-one ``standalone`` command, you can instead run:

.. code-block:: bash

    airflow db migrate

    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org

    airflow api-server --port 8080

    airflow scheduler

    airflow dag-processor

    airflow triggerer

.. note::
    ``airflow users`` command is only available when :doc:`apache-airflow-providers-fab:auth-manager/index` is enabled.

What's Next?
''''''''''''
From this point, you can head to the :doc:`/tutorial/index` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.

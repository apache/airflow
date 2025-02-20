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

   Successful installation requires a Python 3 environment. Starting with Airflow 2.7.0, Airflow supports Python 3.9, 3.10, 3.11, and 3.12.

   Officially supported installation methods include ``pip`` and ``uv``. Both tools provide a streamlined workflow for installing Airflow and managing dependencies.



   While there have been successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` or ``uv`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported.

   There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
   Airflow. Please switch to ``pip`` or ``uv`` if you encounter such problems. ``Bazel`` community works on fixing
   the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
   newer versions of ``bazel`` will handle it.

   If you wish to install Airflow using those tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.

   This guide will help you quickly set up Apache Airflow using ``uv``, a fast and modern tool for managing Python environments and dependencies. ``uv`` makes the installation process easy and provides a
   smooth setup experience.

1. **Set Airflow Home (optional)**:

   Airflow requires a home directory, and uses ``~/airflow`` by default, but you can set a different location if you prefer. The ``AIRFLOW_HOME`` environment variable is used to inform Airflow of the desired location. This step of setting the environment variable should be done before installing Airflow so that the installation process knows where to store the necessary files.

   .. code-block:: bash

      export AIRFLOW_HOME=~/airflow

2.  Install Airflow Using uv:

    .. rst-class:: centered

        Install uv: `uv Installation Guide <https://docs.astral.sh/uv/getting-started/installation/>`_


    For creating Virtualenv with uv, refer to the documentation here:
    `Creating and Maintaining Local Virtualenv with uv <https://github.com/apache/airflow/blob/main/contributing-docs/07_local_virtualenv.rst#creating-and-maintaining-local-virtualenv-with-uv>`_


3. Install Airflow using the constraints file, which is determined based on the URL we pass:

   .. code-block:: bash
      :substitutions:


      AIRFLOW_VERSION=2.10.5

      # Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
      # See above for supported versions.
      PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

      CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
      # For example this would install 2.10.5 with python 3.9: https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.9.txt

      uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

4. Run Airflow Standalone:

   The ``airflow standalone`` command initializes the database, creates a user, and starts all components.

   .. code-block:: bash

      airflow standalone

5. Access the Airflow UI:

   Visit ``localhost:8080`` in your browser and log in with the admin account details shown in the terminal. Enable the ``example_bash_operator`` DAG in the home page.

Upon running these commands, Airflow will create the ``$AIRFLOW_HOME`` folder
and create the "airflow.cfg" file with defaults that will get you going fast.
You can override defaults using environment variables, see :doc:`/configurations-ref`.
You can inspect the file either in ``$AIRFLOW_HOME/airflow.cfg``, or through the UI in
the ``Admin->Configuration`` menu. The PID file for the webserver will be stored
in ``$AIRFLOW_HOME/airflow-webserver.pid`` or in ``/run/airflow/webserver.pid``
if started by systemd.

Out of the box, Airflow uses a SQLite database, which you should outgrow
fairly quickly since no parallelization is possible using this database
backend. It works in conjunction with the
:class:`~airflow.executors.sequential_executor.SequentialExecutor` which will
only run task instances sequentially. While this is very limiting, it allows
you to get up and running quickly and take a tour of the UI and the
command line utilities.

As you grow and deploy Airflow to production, you will also want to move away
from the ``standalone`` command we use here to running the components
separately. You can read more in :doc:`/administration-and-deployment/production-deployment`.

Here are a few commands that will trigger a few task instances. You should
be able to see the status of the jobs change in the ``example_bash_operator`` DAG as you
run the commands below.

.. code-block:: bash

    # run your first task instance
    airflow tasks test example_bash_operator runme_0 2015-01-01
    # run a backfill over 2 days
    airflow backfill create --dag-id example_bash_operator \
        --start-date 2015-01-01 \
        --end-date 2015-01-02

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

    airflow webserver --port 8080

    airflow scheduler

    airflow dag-processor

    airflow triggerer

.. note::
    ``airflow users`` command is only available when FAB auth manager is enabled.

What's Next?
''''''''''''
From this point, you can head to the :doc:`/tutorial/index` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.

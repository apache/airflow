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

   Successful installation requires a Python 3 environment. Starting with Airflow 2.3.0, Airflow is tested with Python 3.7, 3.8, 3.9, 3.10. Note that
   Python 3.11 is not yet supported.

   Only ``pip`` installation is currently officially supported.

   While there have been successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported.

   If you wish to install Airflow using those tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.

The installation of Airflow is painless if you follow the instructions below. Airflow uses
constraint files to enable reproducible installation, so using ``pip`` and constraint files is recommended.

1. Set Airflow Home (optional):

   Airflow requires a home directory, and uses ``~/airflow`` by default, but you can set a different location if you prefer. The ``AIRFLOW_HOME`` environment variable is used to inform Airflow of the desired location. This step of setting the environment variable should be done before installing Airflow so that the installation process knows where to store the necessary files.

   .. code-block:: bash

      export AIRFLOW_HOME=~/airflow

2. Install Airflow using the constraints file, which is determined based on the URL we pass:

   .. code-block:: bash
      :substitutions:


      AIRFLOW_VERSION=|version|

      # Extract the version of Python you have installed. If you're currently using Python 3.11 you may want to set this manually as noted above, Python 3.11 is not yet supported.
      PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

      CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
      # For example this would install |version| with python 3.7: https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.7.txt

      pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

3. Run Airflow Standalone:

   The ``airflow standalone`` command initializes the database, creates a user, and starts all components.

   .. code-block:: bash

      airflow standalone

4. Access the Airflow UI:

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
    airflow dags backfill example_bash_operator \
        --start-date 2015-01-01 \
        --end-date 2015-01-02

If you want to run the individual parts of Airflow manually rather than using
the all-in-one ``standalone`` command, you can instead run:

.. code-block:: bash

    airflow db init

    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org

    airflow webserver --port 8080

    airflow scheduler

What's Next?
''''''''''''
From this point, you can head to the :doc:`/tutorial/index` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.

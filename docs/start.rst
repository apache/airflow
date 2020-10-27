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

The installation is quick and straightforward.

.. code-block:: bash

    # airflow needs a home, ~/airflow is the default,
    # but you can lay foundation somewhere else if you prefer
    # (optional)
    export AIRFLOW_HOME=~/airflow

    # install from pypi using pip
    pip install apache-airflow

    # initialize the database
    airflow db init

    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org

    # start the web server, default port is 8080
    airflow webserver --port 8080

    # start the scheduler
    # open a new terminal or else run webserver with ``-D`` option to run it as a deamon
    airflow scheduler

    # visit localhost:8080 in the browser and use the admin account you just
    # created to login. Enable the example_bash_operator dag in the home page

Upon running these commands, Airflow will create the ``$AIRFLOW_HOME`` folder
and create the "airflow.cfg" file with defaults that will get you going fast.
You can inspect the file either in ``$AIRFLOW_HOME/airflow.cfg``, or through the UI in
the ``Admin->Configuration`` menu. The PID file for the webserver will be stored
in ``$AIRFLOW_HOME/airflow-webserver.pid`` or in ``/run/airflow/webserver.pid``
if started by systemd.

Out of the box, Airflow uses a sqlite database, which you should outgrow
fairly quickly since no parallelization is possible using this database
backend. It works in conjunction with the :class:`airflow.executors.sequential_executor.SequentialExecutor` which will
only run task instances sequentially. While this is very limiting, it allows
you to get up and running quickly and take a tour of the UI and the
command line utilities.

Here are a few commands that will trigger a few task instances. You should
be able to see the status of the jobs change in the ``example_bash_operator`` DAG as you
run the commands below.

.. code-block:: bash

    # run your first task instance
    airflow tasks run example_bash_operator runme_0 2015-01-01
    # run a backfill over 2 days
    airflow dags backfill example_bash_operator \
        --start-date 2015-01-01 \
        --end-date 2015-01-02

Basic Airflow architecture
--------------------------

Primarily intended for development use, the basic Airflow architecture with the Local and Sequential executors is an
excellent starting point for understanding the architecture of Apache Airflow.

.. image:: img/arch-diag-basic.png


There are a few components to note:

* **Metadata Database**: Airflow uses a SQL database to store metadata about the data pipelines being run. In the
  diagram above, this is represented as Postgres which is extremely popular with Airflow.
  Alternate databases supported with Airflow include MySQL.

* **Web Server** and **Scheduler**: The Airflow web server and Scheduler are separate processes run (in this case)
  on the local machine and interact with the database mentioned above.

* The **Executor** is shown separately above, since it is commonly discussed within Airflow and in the documentation, but
  in reality it is NOT a separate process, but run within the Scheduler.

* The **Worker(s)** are separate processes which also interact with the other components of the Airflow architecture and
  the metadata repository.

* ``airflow.cfg`` is the Airflow configuration file which is accessed by the Web Server, Scheduler, and Workers.

* **DAGs** refers to the DAG files containing Python code, representing the data pipelines to be run by Airflow. The
  location of these files is specified in the Airflow configuration file, but they need to be accessible by the
  Web Server, Scheduler, and Workers.



What's Next?
''''''''''''
From this point, you can head to the :doc:`tutorial` section for further examples or the :doc:`howto/index` section if you're ready to get your hands dirty.

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

    # if you build with master
    airflow users create --username admin --firstname Peter --lastname Parker --role Admin --email spiderman@superhero.org

    # start the web server, default port is 8080
    airflow webserver -p 8080

    # start the scheduler
    airflow scheduler

    # visit localhost:8080 in the browser and use the admin account you just
    # created to login. Enable the example_bash_operator dag in the home page

Upon running these commands, Airflow will create the ``$AIRFLOW_HOME`` folder
and lay an "airflow.cfg" file with defaults that get you going fast. You can
inspect the file either in ``$AIRFLOW_HOME/airflow.cfg``, or through the UI in
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
    airflow dags backfill example_bash_operator -s 2015-01-01 -e 2015-01-02

What's Next?
''''''''''''
From this point, you can head to the :doc:`tutorial` section for further examples or the :doc:`howto/index` section if you're ready to get your hands dirty.

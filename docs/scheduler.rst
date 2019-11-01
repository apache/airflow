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

Scheduler
==========

The Airflow scheduler monitors all tasks and all DAGs, and triggers the
task instances whose dependencies have been met. Behind the scenes,
it spins up a subprocess, which monitors and stays in sync with a folder
for all DAG objects it may contain, and periodically (every minute or so)
collects DAG parsing results and inspects active tasks to see whether
they can be triggered.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is
execute ``airflow scheduler``. It will use the configuration specified in
``airflow.cfg``.

The scheduler starts an instance of an :doc:`Executor </executor/index>`. 

To start a scheduler, simply run the command:

.. code:: bash

    airflow scheduler

You can start scheduling a DAG once your scheduler has started running successfully.

.. important::
    You should refer :doc:`dag-run` for details on scheduling a DAG.

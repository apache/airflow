..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Setup Test Environment using MySQL
==================================

By default, Airflow uses SQLite as database backend
and ``SequentialExecutor`` to execute tasks as SQLite
does not support multiple connections. Since
``SequentialExecutor`` runs one instance at a time,
some parallel execution logic will not be exercised
in this default setup.

To test out the parallel execution setup, we can use
MySQL as database backend and ``LocalExecutor`` as
the executor. Checkout the following setups to launch
a MySQL database container:

.. code-block:: bash

  # Launch MySQL docker container
  docker-compose -f scripts/ci/docker-compose.yml run -p3306:3306 mysql

  # Open airflow.cfg and add the following:
  # executor = LocalExecutor
  vim $AIRFLOW_HOME/airflow.cfg

  export AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql://root@127.0.0.1:3306/AIRFLOW_HOME

  airflow initdb

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

Airflow 2.0 CLI Command Migration Guide
=======================================
This article provides guideline for migrating to Airflow 2.0 from previous Airflow versions.

Introduction
------------
The Airflow CLI has been organized so that related commands are grouped together as subcommands. You can
learn about the commands by running ``airflow --help```. For example to get help about the ``celery`` group command,
you have to run the help command: ``airflow celery --help``.

The table below shows the commands that have changed in Airflow 2.0.

.. list-table:: Airflow 2.0 CLI Migration Guide
   :widths: 40, 40, 20
   :header-rows: 1

   * - Old commands
     - New commands
     - Group

   * - ``airflow trigger_dag``
     - ``airflow dags trigger``
     - ``dags``

   * - ``airflow delete_dag``
     - ``airflow dags delete``
     - ``dags``

   * - ``airflow show_dag``
     - ``airflow dags show``
     - ``dags``

   * - ``airflow list_dag``
     - ``airflow dags list``
     - ``dags``

   * - ``airflow dag_status``
     - ``airflow dags status``
     - ``dags``

   * - ``airflow backfill``
     - ``airflow dags backfill``
     - ``dags``

   * - ``airflow list_dag_runs``
     - ``airflow dags list_runs``
     - ``dags``

   * - ``airflow pause``
     - ``airflow dags pause``
     - ``dags``

   * - ``airflow unpause``
     - ``airflow dags unpause``
     - ``dags``

   * - ``airflow test``
     - ``airflow tasks test``
     - ``tasks``

   * - ``airflow clear``
     - ``airflow tasks clear``
     - ``tasks``

   * - ``airflow list_tasks``
     - ``airflow tasks list``
     - ``tasks``

   * - ``airflow task_failed_deps``
     - ``airflow tasks failed_deps``
     - ``tasks``

   * - ``airflow task_state``
     - ``airflow tasks state``
     - ``tasks``

   * - ``airflow run``
     - ``airflow tasks run``
     - ``tasks``

   * - ``airflow render``
     - ``airflow tasks render``
     - ``tasks``

   * - ``airflow initdb``
     - ``airflow db init``
     - ``db``

   * - ``airflow resetdb``
     - ``airflow db reset``
     - ``db``

   * - ``airflow upgradedb``
     - ``airflow db upgrade``
     - ``db``

   * - ``airflow checkdb``
     - ``airflow db check``
     - ``db``

   * - ``airflow shell``
     - ``airflow db shell``
     - ``db``

   * - ``airflow pool``
     - ``airflow pools``
     - ``pools``

   * - ``airflow create_user``
     - ``airflow users create``
     - ``users``

   * - ``airflow delete_user``
     - ``airflow users delete``
     - ``users``

   * - ``airflow list_users``
     - ``airflow users list``
     - ``users``

   * - ``airflow worker``
     - ``airflow celery worker``
     - ``celery``

   * - ``airflow flower``
     - ``airflow celery flower``
     - ``celery``

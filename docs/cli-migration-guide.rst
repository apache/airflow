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

Use Exactly Single Character For Short Option Style Change
----------------------------------------------------------
For Airflow short option, use exactly one single character, New commands are available according to the following table:

.. list-table:: Single Character For Short Option Style Change
   :widths: 50, 50
   :header-rows: 1

   * - Old Commands
     - New Commands

   * - ``airflow (dags|tasks|scheduler) [-sd, --subdir]``
     - ``airflow (dags|tasks|scheduler) [-S, --subdir]``

   * - ``airflow tasks test [-dr, --dry_run]``
     - ``airflow tasks test [-n, --dry-run]``

   * - ``airflow dags backfill [-dr, --dry_run]``
     - ``airflow dags backfill [-n, --dry-run]``

   * - ``airflow tasks clear [-dx, --dag_regex]``
     - ``airflow tasks clear [-R, --dag-regex]``

   * - ``airflow kerberos [-kt, --keytab]``
     - ``airflow kerberos [-k, --keytab]``

   * - ``airflow tasks run [-int, --interactive]``
     - ``airflow tasks run [-N, --interactive]``

   * - ``airflow webserver [-hn, --hostname]``
     - ``airflow webserver [-H, --hostname]``

   * - ``airflow celery worker [-cn, --celery_hostname]``
     - ``airflow celery worker [-H, --celery-hostname]``

   * - ``airflow celery flower [-hn, --hostname]``
     - ``airflow celery flower [-H, --hostname]``

   * - ``airflow celery flower [-fc, --flower_conf]``
     - ``airflow celery flower [-c, --flower-conf]``

   * - ``airflow celery flower [-ba, --basic_auth]``
     - ``airflow celery flower [-A, --basic-auth]``

   * - ``airflow celery flower [-tp, --task_params]``
     - ``airflow celery flower [-t, --task-params]``

   * - ``airflow celery flower [-pm, --post_mortem]``
     - ``airflow celery flower [-m, --post-mortem]``

For Airflow long option, use `kebab-case <https://en.wikipedia.org/wiki/Letter_case>`_ instead of `snake_case <https://en.wikipedia.org/wiki/Snake_case>`_

.. list-table:: Airflow CLI long option
   :widths: 50, 50
   :header-rows: 1

   * - Old commands
     - New commands

   * - ``--task_regex``
     - ``--task-regex``

   * - ``--start_date``
     - ``--start-date``

   * - ``--end_date``
     - ``--end-date``

   * - ``--dry_run``
     - ``--dry-run``

   * - ``--no_backfill``
     - ``--no-backfill``

   * - ``--mark_success``
     - ``--mark-success``

   * - ``--donot_pickle``
     - ``--donot-pickle``

   * - ``--ignore_dependencies``
     - ``--ignore-dependencies``

   * - ``--ignore_first_depends_on_past``
     - ``--ignore-first-depends-on-past``

   * - ``--delay_on_limit``
     - ``--delay-on-limit``

   * - ``--reset_dagruns``
     - ``--reset-dagruns``

   * - ``--rerun_failed_tasks``
     - ``--rerun-failed-tasks``

   * - ``--run_backwards``
     - ``--run-backwards``

   * - ``--only_failed``
     - ``--only-failed``

   * - ``--only_running``
     - ``--only-running``

   * - ``--exclude_subdags``
     - ``--exclude-subdags``

   * - ``--exclude_parentdag``
     - ``--exclude-parentdag``

   * - ``--dag_regex``
     - ``--dag-regex``

   * - ``--run_id``
     - ``--run-id``

   * - ``--exec_date``
     - ``--exec-date``

   * - ``--ignore_all_dependencies``
     - ``--ignore-all-dependencies``

   * - ``--ignore_depends_on_past``
     - ``--ignore-depends-on-past``

   * - ``--ship_dag``
     - ``--ship-dag``

   * - ``--job_id``
     - ``--job-id``

   * - ``--cfg_path``
     - ``--cfg-path``

   * - ``--ssl_cert``
     - ``--ssl-cert``

   * - ``--ssl_key``
     - ``--ssl-key``

   * - ``--worker_timeout``
     - ``--worker-timeout``

   * - ``--access_logfile``
     - ``--access-logfile``

   * - ``--error_logfile``
     - ``--error-logfile``

   * - ``--dag_id``
     - ``--dag-id``

   * - ``--num_runs``
     - ``--num-runs``

   * - ``--do_pickle``
     - ``--do-pickle``

   * - ``--celery_hostname``
     - ``--celery-hostname``

   * - ``--broker_api``
     - ``--broker-api``

   * - ``--flower_conf``
     - ``--flower-conf``

   * - ``--url_prefix``
     - ``--url-prefix``

   * - ``--basic_auth``
     - ``--basic-auth``

   * - ``--task_params``
     - ``--task-params``

   * - ``--post_mortem``
     - ``--post-mortem``

   * - ``--conn_uri``
     - ``--conn-uri``

   * - ``--conn_type``
     - ``--conn-type``

   * - ``--conn_host``
     - ``--conn-host``

   * - ``--conn_login``
     - ``--conn-login``

   * - ``--conn_password``
     - ``--conn-password``

   * - ``--conn_schema``
     - ``--conn-schema``

   * - ``--conn_port``
     - ``--conn-port``

   * - ``--conn_extra``
     - ``--conn-extra``

   * - ``--use_random_password``
     - ``--use-random-password``

   * - ``--skip_serve_logs``
     - ``--skip-serve-logs``

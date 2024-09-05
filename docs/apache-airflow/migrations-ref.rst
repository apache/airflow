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

Reference for Database Migrations
'''''''''''''''''''''''''''''''''

Here's the list of all the Database Migrations that are executed via when you run ``airflow db migrate``.

.. warning::

   Those migration details are mostly used here to make the users aware when and what kind of migrations
   will be executed during migrations between specific Airflow versions. The intention here is that the
   "DB conscious" users might perform an analysis on the migrations and draw conclusions about the impact
   of the migrations on their Airflow database. Those users might also want to take a look at the
   :doc:`database-erd-ref` document to understand how the internal DB of Airflow structure looks like.
   However, you should be aware that the structure is internal and you should not access the DB directly
   to retrieve or modify any data - you should use the :doc:`REST API <stable-rest-api-ref>` to do that instead.



 .. This table is automatically updated by pre-commit by ``scripts/ci/pre_commit/migration_reference.py``
 .. All table elements are scraped from migration files
 .. Beginning of auto-generated table

+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| Revision ID             | Revises ID       | Airflow Version   | Description                                                  |
+=========================+==================+===================+==============================================================+
| ``1cdc775ca98f`` (head) | ``a2c32e6c7729`` | ``3.0.0``         | Drop ``execution_date`` unique constraint on DagRun.         |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``a2c32e6c7729``        | ``0bfc26bc256e`` | ``3.0.0``         | Add triggered_by field to DagRun.                            |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``0bfc26bc256e``        | ``d0f1c55954fa`` | ``3.0.0``         | Rename DagModel schedule_interval to timetable_summary.      |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``d0f1c55954fa``        | ``044f740568ec`` | ``3.0.0``         | Remove SubDAGs: ``is_subdag`` & ``root_dag_id`` columns from |
|                         |                  |                   | DAG table.                                                   |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``044f740568ec``        | ``22ed7efa9da2`` | ``3.0.0``         | Drop ab_user.id foreign key.                                 |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``22ed7efa9da2``        | ``8684e37832e6`` | ``2.10.0``        | Add dag_schedule_dataset_alias_reference table.              |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``8684e37832e6``        | ``41b3bc7c0272`` | ``2.10.0``        | Add dataset_alias_dataset association table.                 |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``41b3bc7c0272``        | ``ec3471c1e067`` | ``2.10.0``        | Add try_number to audit log.                                 |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``ec3471c1e067``        | ``05e19f3176be`` | ``2.10.0``        | Add dataset_alias_dataset_event.                             |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``05e19f3176be``        | ``d482b7261ff9`` | ``2.10.0``        | Add dataset_alias.                                           |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``d482b7261ff9``        | ``c4602ba06b4b`` | ``2.10.0``        | Add task_instance_history.                                   |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``c4602ba06b4b``        | ``677fdbb7fc54`` | ``2.10.0``        | Added DagPriorityParsingRequest table.                       |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``677fdbb7fc54``        | ``0fd0c178cbe8`` | ``2.10.0``        | add new executor field to db.                                |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``0fd0c178cbe8``        | ``686269002441`` | ``2.10.0``        | Add indexes on dag_id column in referencing tables.          |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``686269002441``        | ``bff083ad727d`` | ``2.9.2``         | Fix inconsistency between ORM and migration files.           |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``bff083ad727d``        | ``1949afb29106`` | ``2.9.2``         | Remove ``idx_last_scheduling_decision`` index on             |
|                         |                  |                   | last_scheduling_decision in dag_run table.                   |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``1949afb29106``        | ``ee1467d4aa35`` | ``2.9.0``         | update trigger kwargs type and encrypt.                      |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``ee1467d4aa35``        | ``b4078ac230a1`` | ``2.9.0``         | add display name for dag and task instance.                  |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``b4078ac230a1``        | ``8e1c784a4fc7`` | ``2.9.0``         | Change value column type to longblob in xcom table for       |
|                         |                  |                   | mysql.                                                       |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``8e1c784a4fc7``        | ``ab34f260b71c`` | ``2.9.0``         | Adding max_consecutive_failed_dag_runs column to dag_model   |
|                         |                  |                   | table.                                                       |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``ab34f260b71c``        | ``d75389605139`` | ``2.9.0``         | add dataset_expression in DagModel.                          |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``d75389605139``        | ``1fd565369930`` | ``2.9.0``         | Add run_id to (Audit) log table and increase event name      |
|                         |                  |                   | length.                                                      |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``1fd565369930``        | ``88344c1d9134`` | ``2.9.0``         | Add rendered_map_index to TaskInstance.                      |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``88344c1d9134``        | ``10b52ebd31f7`` | ``2.8.1``         | Drop unused TI index.                                        |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``10b52ebd31f7``        | ``bd5dfbe21f88`` | ``2.8.0``         | Add processor_subdir to ImportError.                         |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``bd5dfbe21f88``        | ``f7bf2a57d0a6`` | ``2.8.0``         | Make connection login/password TEXT.                         |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``f7bf2a57d0a6``        | ``375a816bbbf4`` | ``2.8.0``         | Add owner_display_name to (Audit) Log table.                 |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``375a816bbbf4``        | ``405de8318b3a`` | ``2.8.0``         | add new field 'clear_number' to dagrun.                      |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``405de8318b3a``        | ``788397e78828`` | ``2.7.0``         | add include_deferred column to pool.                         |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``788397e78828``        | ``937cbd173ca1`` | ``2.7.0``         | Add custom_operator_name column.                             |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``937cbd173ca1`` (base) | ``None``         | ``2.7.0``         | Add index to task_instance table.                            |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+

 .. End of auto-generated table

.. spelling:word-list::
    branchpoint
    mergepoint

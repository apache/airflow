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
| ``d0f1c55954fa`` (head) | ``044f740568ec`` | ``3.0.0``         | Remove SubDAGs: ``is_subdag`` & ``root_dag_id`` columns from |
|                         |                  |                   | Dag table.                                                   |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``044f740568ec``        | ``22ed7efa9da2`` | ``3.0.0``         | Drop ab_user.id foreign key.                                 |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+
| ``22ed7efa9da2`` (base) | ``None``         | ``2.10.0``        | Add dag_schedule_dataset_alias_reference table.              |
+-------------------------+------------------+-------------------+--------------------------------------------------------------+

 .. End of auto-generated table

.. spelling:word-list::
    branchpoint
    mergepoint

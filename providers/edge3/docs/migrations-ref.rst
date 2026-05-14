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
   will be executed during migrations between specific Edge3 provider versions. The intention here is that the
   "DB conscious" users might perform an analysis on the migrations and draw conclusions about the impact
   of the migrations on their Airflow database.

 .. This table is automatically updated by prek hook: ``scripts/ci/prek/migration_reference.py``
 .. All table elements are scraped from migration files
 .. Beginning of auto-generated table

+-------------------------+------------------+-----------------+----------------------------------------------------------+
| Revision ID             | Revises ID       | Edge3 Version   | Description                                              |
+=========================+==================+=================+==========================================================+
| ``c6b3c3d093fd`` (head) | ``a09c3ee8e1d3`` | ``3.5.0``       | Replace individual counters with extended JSON based     |
|                         |                  |                 | sysinfo.                                                 |
+-------------------------+------------------+-----------------+----------------------------------------------------------+
| ``a09c3ee8e1d3``        | ``8c275b6fbaa8`` | ``3.4.0``       | Add team_name column to edge_job and edge_worker tables. |
+-------------------------+------------------+-----------------+----------------------------------------------------------+
| ``8c275b6fbaa8``        | ``b3c4d5e6f7a8`` | ``3.2.0``       | Fix migration file/ORM inconsistencies.                  |
+-------------------------+------------------+-----------------+----------------------------------------------------------+
| ``b3c4d5e6f7a8``        | ``9d34dfc2de06`` | ``3.2.0``       | Add concurrency column to edge_worker table.             |
+-------------------------+------------------+-----------------+----------------------------------------------------------+
| ``9d34dfc2de06`` (base) | ``None``         | ``3.0.0``       | Create Edge tables if missing.                           |
+-------------------------+------------------+-----------------+----------------------------------------------------------+

 .. End of auto-generated table

.. spelling:word-list::
    branchpoint
    mergepoint

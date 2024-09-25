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

Upgrading FAB to a newer version
--------------------------------
Before reading this, make sure you have read the Airflow Upgrade Guide for how to prepare for an upgrade:
:doc:`apache-airflow:installation/upgrading`

Why you need to upgrade
=======================
The FAB provider is a separate package from Airflow and it is released independently. Starting from version 1.3.0, FAB
can now run its own migrations if you are on Airflow 3. Newer FAB versions can contain database migrations, so you
must run ``airflow fab-db migrate`` to migrate your database with the schema changes in the FAB version you are
upgrading to. If ``FABDBManager`` is included in the ``[core] external_db_managers`` configuration, the migrations will
be run automatically as part of the ``airflow db migrate`` command.

How to upgrade
==============
To upgrade the FAB provider, you need to install the new version of the package. You can do this using ``pip``.
After the installation, you can run the DB upgrade of the FAB provider by running the following command:
``airflow fab-db migrate``. This command is only available if you are in Airflow 3.0.0 or newer.

The command takes the same options as ``airflow db migrate`` command, you can learn more about the command by
running ``airflow fab-db migrate --help``.

How to downgrade
================
If you need to downgrade the FAB provider, you can do this by running the downgrade command to the version you want to
downgrade to, example ``airflow fab-db downgrade --to-version 1.2.0``. Afterwards, install the new FAB provider version
using ``pip``.

There are other options to this command, check it out by running ``airflow fab-db downgrade -help``.

Resetting the FAB database
==========================
If you need to reset the FAB database, you can do this by running the reset command, example ``airflow fab-db reset``.
This command will drop all tables in the FAB database and recreate them. This command is only available if you are in
Airflow 3.0.0 or newer. There are other options to this command, check it out by running ``airflow fab-db reset --help``.

Offline SQL migration scripts
=============================
If you want to run the upgrade script offline, you can use the ``-s`` or ``--show-sql-only`` flag
to get the SQL statements that would be executed. You may also specify the starting FAB version with the
``--from-version`` flag and the ending FAB version with the ``-n`` or ``--to-version`` flag.
This feature is supported in Postgres and MySQL.

Sample usage for Airflow version 2.7.0 or greater:
   ``airflow fab-db migrate -s --from-version "1.3.0" -n "1.4.0"``
   ``airflow fab-db migrate --show-sql-only --from-version "1.3.0" --to-version "1.4.0"``

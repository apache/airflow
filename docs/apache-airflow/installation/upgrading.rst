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

Upgrading Airflow to a newer version
------------------------------------

Why you need to upgrade
=======================

Newer Airflow versions can contain Database migrations so it is recommended that you run
``airflow db upgrade`` to Upgrade your Database with the schema changes in the Airflow version
you are upgrading to.

When you need to upgrade
========================

If you have a custom deployment based on virtualenv or Docker Containers, you usually need to run
the DB upgrade manually as part of the upgrade process.

In some cases the upgrade happens automatically - it depends if in your deployment, the upgrade is
built-in as post-install action. For example when you are using :doc:`helm-chart:index` with
post-upgrade hooks enabled, the database upgrade happens automatically right after the new software
is installed. Similarly all Airflow-As-A-Service solutions perform the upgrade automatically for you,
when you choose to upgrade airflow via their UI.

How to upgrade
==============

In order to manually upgrade the database you should run the ``airflow db upgrade`` command in your
environment. It can be run either in your virtual environment or in the containers that give
you access to Airflow ``CLI`` :doc:`/usage-cli` and the database.

Migration best practices
========================

Depending on the size of your database and the actual migration it might take quite some time to migrate it,
so if you have long history and big database, it is recommended to make a copy of the database first and
perform a test migration to assess how long the migration will take. Typically "Major" upgrades might take
longer as adding new features require sometimes restructuring of the database.

Post-upgrade warnings
=====================

Typically you just need to successfully run ``airflow db upgrade`` command and this is all. However in
some cases, the migration might find some old, stale and probably wrong data in your database and moves it
aside to a separate table. In this case you might get warning in your webserver UI about the data found.

Typical message that you might see:

  Airflow found incompatible data in the <original table> table in the
  metadatabase, and has moved them to <new table> during the database migration to upgrade.
  Please inspect the moved data to decide whether you need to keep them,
  and manually drop the <new table> table to dismiss this warning.

When you see such message, it means that some of your data was corrupted and you should inspect it
to determine whether you would like to keep or delete some of that data. Most likely the data was corrupted
and left-over from some bugs and can be safely deleted - because this data would not be anyhow visible
and useful in Airflow. However if you have particular need for auditing or historical reasons you might
choose to store it somewhere. Unless you have specific reasons to keep the data most likely deleting it
is your best option.

There are various ways you can inspect and delete the data - if you have direct access to the
database using your own tools (often graphical tools showing the database objects), you can drop such
table or rename it or move it to another database using those tools. If you don't have such tools you
can use the ``airflow db shell`` command - this will drop you in the db shell tool for your database and you
will be able to both inspect and delete the table.

Please replace ``<table>`` in the examples with the actual table name as printed in the warning message.

Inspecting a table:

.. code-block:: sql

   SELECT * FROM <table>;

Deleting a table:

.. code-block:: sql

   DROP TABLE <table>;

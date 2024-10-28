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

Upgrading Airflow® to a newer version
-------------------------------------

Why you need to upgrade
=======================

Newer Airflow versions can contain database migrations so you must run ``airflow db migrate``
to migrate your database with the schema changes in the Airflow version you are upgrading to.
Don't worry, it's safe to run even if there are no migrations to perform.

What are the changes between Airflow version x and y?
=====================================================

The :doc:`release notes <../release_notes>` lists the changes that were included in any given Airflow release.

Upgrade preparation - make a backup of DB
=========================================

It is highly recommended to make a backup of your metadata DB before any migration.
If you do not have a "hot backup" capability for your DB, you should
do it after shutting down your Airflow instances, so that the backup of your database will be consistent.
If you did not make a backup and your migration fails, you might end-up in
a half-migrated state and restoring DB from backup and repeating the
migration might be the only easy way out. This can for example be caused by a broken
network connection between your CLI and the database while the migration happens, so taking
a backup is an important precaution to avoid problems like this.

When you need to upgrade
========================

If you have a custom deployment based on virtualenv or Docker Containers, you usually need to run
the DB migrate manually as part of the upgrade process.

In some cases the upgrade happens automatically - it depends if in your deployment, the upgrade is
built-in as post-install action. For example when you are using :doc:`helm-chart:index` with
post-upgrade hooks enabled, the database upgrade happens automatically right after the new software
is installed. Similarly all Airflow-As-A-Service solutions perform the upgrade automatically for you,
when you choose to upgrade airflow via their UI.

How to upgrade
==============

Reinstall Apache Airflow®, specifying the desired new version.

To upgrade a bootstrapped local instance, you can set the ``AIRFLOW_VERSION`` environment variable to the
intended version prior to rerunning the installation command. Upgrade incrementally by patch version: e.g.,
if upgrading from version 2.8.2 to 2.8.4, upgrade first to 2.8.3. For more detailed guidance, see
:doc:`/start`.

To upgrade a PyPI package, rerun the ``pip install`` command in your environment using the desired version
as a constraint. For more detailed guidance, see :doc:`/installation/installing-from-pypi`.

In order to manually migrate the database you should run the ``airflow db migrate`` command in your
environment. It can be run either in your virtual environment or in the containers that give
you access to Airflow ``CLI`` :doc:`/howto/usage-cli` and the database.

Offline SQL migration scripts
=============================
If you want to run the upgrade script offline, you can use the ``-s`` or ``--show-sql-only`` flag
to get the SQL statements that would be executed. You may also specify the starting airflow version with the ``--from-version`` flag and the ending airflow version with the ``-n`` or ``--to-version`` flag. This feature is supported in Postgres and MySQL
from Airflow 2.0.0 onward.

Sample usage for Airflow version 2.7.0 or greater:
   ``airflow db migrate -s --from-version "2.4.3" -n "2.7.3"``
   ``airflow db migrate --show-sql-only --from-version "2.4.3" --to-version "2.7.3"``

.. note::
    ``airflow db upgrade`` has been replaced by ``airflow db migrate`` since Airflow version 2.7.0
    and former has been deprecated.


Handling migration problems
===========================


Wrong Encoding in MySQL database
................................

If you are using old Airflow 1.10 as a database created initially either manually or with previous version of MySQL,
depending on the original character set of your database, you might have problems with migrating to a newer
version of Airflow and your migration might fail with strange errors ("key size too big", "missing indexes" etc).
The next chapter describes how to fix the problem manually.


Why you might get the error? The recommended character set/collation for MySQL 8 database is
``utf8mb4`` and ``utf8mb4_bin`` respectively. However, this has been changing in different versions of
MySQL and you could have custom created database with a different character set. If your database
was created with an old version of Airflow or MySQL, the encoding could have been wrong when the database
was created or broken during migration.

Unfortunately, MySQL limits the index key size and with ``utf8mb4``, Airflow index key sizes might be
too big for MySQL to handle. Therefore in Airflow we force all the "ID" keys to use ``utf8`` character
set (which is equivalent to ``utf8mb3`` in MySQL 8). This limits the size of indexes so that MySQL
can handle them.

Here are the steps you can follow to fix it BEFORE you attempt to migrate
(but you might also choose to do it your way if you know what you are doing).

Get familiar with the internal Database structure of Airflow which you might find at
:doc:`/database-erd-ref` and list of migrations that you might find in :doc:`/migrations-ref`.


1. Make a backup of your database so that you can restore it in case of a mistake.


2. Check which of the tables of yours need fixing. Look at those tables:

.. code-block:: sql

    SHOW CREATE TABLE task_reschedule;
    SHOW CREATE TABLE xcom;
    SHOW CREATE TABLE task_fail;
    SHOW CREATE TABLE rendered_task_instance_fields;
    SHOW CREATE TABLE task_instance;

Make sure to copy the output. You will need it in the last step. Your
``dag_id``, ``run_id``, ``task_id`` and ``key`` columns should have ``utf8`` or ``utf8mb3`` character
set set explicitly, similar to:

.. code-block:: text

  ``task_id`` varchar(250) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,  # correct

or

.. code-block:: text

  ``task_id`` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,  # correct


The problem is if your fields have no encoding:

.. code-block:: text

  ``task_id`` varchar(250),  # wrong !!


or just collation set to utf8mb4:

.. code-block:: text

  ``task_id`` varchar(250) COLLATE utf8mb4_unicode_ci DEFAULT NULL,  # wrong !!


or character set and collation set to utf8mb4

.. code-block:: text

  ``task_id`` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,  # wrong !!


You need to fix those fields that have wrong character set/collation set.


3. Drop foreign key indexes for tables you need to modify (you do not need to drop all of them - do it just
for those tables that you need to modify). You will need to recreate them in the last step (that's why
you need to keep the ``SHOW CREATE TABLE`` output from step 2.

.. code-block:: sql

    ALTER TABLE task_reschedule DROP FOREIGN KEY task_reschedule_ti_fkey;
    ALTER TABLE xcom DROP FOREIGN KEY xcom_task_instance_fkey;
    ALTER TABLE task_fail DROP FOREIGN KEY task_fail_ti_fkey;
    ALTER TABLE rendered_task_instance_fields DROP FOREIGN KEY rtif_ti_fkey;


4. Modify your ``ID`` fields to have correct character set/encoding. Only do that for fields that have
wrong encoding (here are all potential commands you might need to use):

.. code-block:: sql

    ALTER TABLE task_instance MODIFY task_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE task_reschedule MODIFY task_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;

    ALTER TABLE rendered_task_instance_fields MODIFY task_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE rendered_task_instance_fields MODIFY dag_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;

    ALTER TABLE task_fail MODIFY task_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE task_fail MODIFY dag_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;

    ALTER TABLE sla_miss MODIFY task_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE sla_miss MODIFY dag_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;

    ALTER TABLE task_map MODIFY task_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE task_map MODIFY dag_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE task_map MODIFY run_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;

    ALTER TABLE xcom MODIFY task_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE xcom MODIFY dag_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE xcom MODIFY run_id VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;
    ALTER TABLE xcom MODIFY key VARCHAR(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;

5. Recreate the foreign keys dropped in step 3.

Repeat this one for all the indexes you dropped. Note that depending on the version of Airflow you
Have, the indexes might be slightly different (for example ``map_index`` was added in 2.3.0) but if you
keep the ``SHOW CREATE TABLE`` output prepared in step 2., you will find the right ``CONSTRAINT_NAME``
and ``CONSTRAINT`` to use.

.. code-block:: sql

    # Here you have to copy the statements from SHOW CREATE TABLE output
    ALTER TABLE <TABLE> ADD CONSTRAINT `<CONSTRAINT_NAME>` <CONSTRAINT>


This should bring the database to the state where you will be able to run the migration to the new
Airflow version.


Post-upgrade warnings
.....................

Typically you just need to successfully run ``airflow db migrate`` command and this is all. However, in
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
and useful in Airflow. However, if you have particular need for auditing or historical reasons you might
choose to store it somewhere. Unless you have specific reasons to keep the data most likely deleting it
is your best option.

There are various ways you can inspect and delete the data - if you have direct access to the
database using your own tools (often graphical tools showing the database objects), you can drop such
table or rename it or move it to another database using those tools. If you don't have such tools you
can use the ``airflow db shell`` command - this will drop you in the db shell tool for your database and you
will be able to both inspect and delete the table.

How to drop the table using Kubernetes:


1. Exec into any of the Airflow pods - webserver or scheduler: ``kubectl exec -it <your-webserver-pod> python``

2. Run the following commands in the python shell:

 .. code-block:: python

     from airflow.settings import Session

     session = Session()
     session.execute("DROP TABLE _airflow_moved__2_2__task_instance")
     session.commit()

Please replace ``<table>`` in the examples with the actual table name as printed in the warning message.

Inspecting a table:

.. code-block:: sql

   SELECT * FROM <table>;

Deleting a table:

.. code-block:: sql

   DROP TABLE <table>;


Migration best practices
========================

Depending on the size of your database and the actual migration it might take quite some time to migrate it,
so if you have long history and big database, it is recommended to make a copy of the database first and
perform a test migration to assess how long the migration will take. Typically "Major" upgrades might take
longer as adding new features require sometimes restructuring of the database.

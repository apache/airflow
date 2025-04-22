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

Upgrading to Airflow 3
=======================

Apache Airflow 3 is a major release and contains :ref:`breaking changes<breaking-changes>`. This guide walks you through the steps required to upgrade from Airflow 2.x to Airflow 3.0.

Step 1: Take care of prerequisites
----------------------------------

- Make sure that you are on Airflow 2.7 or later.
- Make sure that your Python version is in the supported list. Airflow 3.0.0 supports the following Python versions: Python 3.9, 3.10, 3.11 and 3.12.
- Ensure that you are not using any features or functionality that have been :ref:`removed in Airflow 3<breaking-changes>`.


Step 2: Clean and back up your existing Airflow Instance
---------------------------------------------------------

- It is highly recommended that you make a backup of your Airflow instance, specifically your Airflow metadata database before starting the migration process.

    - If you do not have a "hot backup" capability for your database, you should do it after shutting down your Airflow instances, so that the backup of your database will be consistent. For example, if you don't turn off your Airflow instance, the backup of the database will not include all TaskInstances or DagRuns.

    - If you did not make a backup and your migration fails, you might end up in a half-migrated state. This can be caused by, for example, a broken network connection between your Airflow CLI and the database during the migration. Having a backup is an important precaution to avoid problems like this.

- A long running Airflow instance can accumulate a substantial amount of data that are no longer required (for example, old XCom data). Schema changes will be a part of the Airflow 3
  upgrade process. These schema changes can take a long time if the database is large. For a faster, safer migration, we recommend that you clean up your Airflow meta-database before the upgrade.
  You can use the ``airflow db clean`` :ref:`Airflow CLI command<cli-db-clean>` to trim your Airflow database.


Step 3: DAG Authors - Check your Airflow DAGs for compatibility
----------------------------------------------------------------

To minimize friction for users upgrading from prior versions of Airflow, we have created a dag upgrade check utility using `Ruff <https://docs.astral.sh/ruff/>`_.

The latest available ``ruff`` version will have the most up-to-date rules, but be sure to use at least version ``0.11.6``. The below example demonstrates how to check
for dag incompatibilities that will need to be fixed before they will work as expected on Airflow 3.

.. code-block:: bash

    ruff check dag/ --select AIR301

To preview the recommended fixes, run the following command:

.. code-block:: bash

    ruff check dag/ --select AIR301 --show-fixes

Some changes can be automatically fixed. To do so, run the following command:

.. code-block:: bash

    ruff check dag/ --select AIR301 --fix

Step 4: Install the Standard Providers
--------------------------------------

- Some of the commonly used Operators which were bundled as part of the ``airflow-core`` package (for example ``BashOperator`` and ``PythonOperator``)
  have now been split out into a separate package: ``apache-airflow-providers-standard``.
- For convenience, this package can also be installed on Airflow 2.x versions, so that DAGs can be modified to reference these Operators from the standard provider
  package instead of Airflow Core.


Step 5: Deployment Managers - Upgrade your Airflow Instance
------------------------------------------------------------

For an easier and safer upgrade process, we have also created a utility to upgrade your Airflow instance configuration.

The first step is to run this configuration check utility as shown below:


.. code-block:: bash

    airflow config update


This configuration utility can also update your configuration to automatically be compatible with Airflow 3. This can be done as shown below:

.. code-block:: bash

    airflow config update --fix


The biggest part of an Airflow upgrade is the database upgrade. The database upgrade process for Airflow 3 is the same as for Airflow 2.7 or later:

.. code-block:: bash

    airflow db migrate


You should now be able to start up your Airflow 3 instance.


Step 6: Changes to your startup scripts
---------------------------------------

In Airflow 3, the Webserver has become a generic API server. The API server can be started up using the following command:

.. code-block:: bash

    airflow api-server

The dag processor must now be started independently, even for local or development setups:

.. code-block:: bash

    airflow dag-processor


.. _breaking-changes:

Breaking Changes
================

Some capabilities which were deprecated in Airflow 2.x are not available in Airflow 3.
These include:

- **SubDAGs**: Replaced by TaskGroups, Assets, and Data Aware Scheduling.
- **Sequential Executor**: Replaced by LocalExecutor, which can be used with SQLite for local development use cases.
- **SLAs**: Deprecated and removed; Will be replaced by forthcoming `Deadline Alerts <https://cwiki.apache.org/confluence/x/tglIEw>`_.
- **Subdir**: Used as an argument on many CLI commands, ``--subdir`` or ``-S`` has been superseded by :doc:`DAG bundles </administration-and-deployment/dag-bundles>`.
- **Some Airflow context variables**: The following keys are no longer available in a :ref:`task instance's context <templates:variables>`. If not replaced, will cause dag errors:
  - ``tomorrow_ds``
  - ``tomorrow_ds_nodash``
  - ``yesterday_ds``
  - ``yesterday_ds_nodash``
  - ``prev_ds``
  - ``prev_ds_nodash``
  - ``prev_execution_date``
  - ``prev_execution_date_success``
  - ``next_execution_date``
  - ``next_ds_nodash``
  - ``next_ds``
  - ``execution_date``
- The ``catchup_by_default`` dag parameter is now ``False`` by default.
- The ``create_cron_data_intervals`` configuration is now ``False`` by default. This means that the ``CronTriggerTimetable`` will be used by default instead of the ``CronDataIntervalTimetable``
- **Simple Auth** is now default ``auth_manager``. To continue using FAB as the Auth Manager, please install the FAB provider and set ``auth_manager`` to ``FabAuthManager``:

  .. code-block:: ini

      airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

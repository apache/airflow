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

Understanding Airflow 3.x Architecture Changes
-----------------------------------------------

Airflow 3.x introduces significant architectural changes that improve security, scalability, and maintainability. Understanding these changes helps you prepare for the upgrade and adapt your workflows accordingly.

Airflow 2.x Architecture
^^^^^^^^^^^^^^^^^^^^^^^^
.. image:: ../img/airflow-2-arch.png
   :alt: Airflow 2.x architecture diagram showing scheduler, metadata database, and worker
   :align: center

- All components communicate directly with the Airflow metadata database.
- Airflow 2 was designed to run all components within the same network space: task code and task execution code (airflow package code that runs user code) run in the same process.
- Workers communicate directly with the Airflow database and execute all user code.
- User code could import sessions and perform malicious actions on the Airflow metadata database.
- The number of connections to the database was excessive, leading to scaling challenges.

Airflow 3.x Architecture
^^^^^^^^^^^^^^^^^^^^^^^^
.. image:: ../img/airflow-3-arch.png
   :alt: Airflow 3.x architecture diagram showing the decoupled Execution API Server and worker subprocesses
   :align: center

- The API server is currently the sole access point for the metadata DB for tasks and workers.
- It supports several applications: the Airflow REST API, an internal API for the Airflow UI that hosts static JS, and an API for workers to interact with when executing TIs via the task execution interface.
- Workers communicate with the API server instead of directly with the database.
- Dag processor and Triggerer utilize the task execution mechanism for their tasks, especially when they require variables or connections.

Database Access Restrictions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In Airflow 3, direct metadata database access from task code is now restricted. This is a key security and architectural improvement that affects how Dag authors interact with Airflow resources:

- **No Direct Database Access**: Task code can no longer directly import and use Airflow database sessions or models.
- **API-Based Resource Access**: All runtime interactions (state transitions, heartbeats, XComs, and resource fetching) are handled through a dedicated Task Execution API.
- **Enhanced Security**: This ensures isolation and security by preventing malicious task code from accessing or modifying the Airflow metadata database.
- **Stable Interface**: The Task SDK provides a stable, forward-compatible interface for accessing Airflow resources without direct database dependencies.

Step 1: Take care of prerequisites
----------------------------------

- Make sure that you are on Airflow 2.7 or later. It is recommended to upgrade to latest 2.x and then to Airflow 3.
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

- Ensure that there are no errors related to Dag processing, such as ``AirflowDagDuplicatedIdException``.  You should
  be able to run ``airflow dags reserialize`` with no errors.  If you have to resolve errors from Dag processing,
  ensure you deploy your changes to your old instance prior to upgrade, and wait until your Dags have all been reprocessed
  (and all errors gone) before you proceed with upgrade.

Step 3: Dag authors - Check your Airflow Dags for compatibility
----------------------------------------------------------------

To minimize friction for users upgrading from prior versions of Airflow, we have created a Dag upgrade check utility using `Ruff <https://docs.astral.sh/ruff/>`_ combined with `AIR <https://docs.astral.sh/ruff/rules/#airflow-air>`_ rules.
The rules AIR301 and AIR302 indicate breaking changes in Airflow 3, while AIR311 and AIR312 highlight changes that are not currently breaking but are strongly recommended for updates.

The latest available ``ruff`` version will have the most up-to-date rules, but be sure to use at least version ``0.13.0``. The below example demonstrates how to check
for Dag incompatibilities that will need to be fixed before they will work as expected on Airflow 3.

.. code-block:: bash

    ruff check dags/ --select AIR301

To preview the recommended fixes, run the following command:

.. code-block:: bash

    ruff check dags/ --select AIR301 --show-fixes

Some changes can be automatically fixed. To do so, run the following command:

.. code-block:: bash

    ruff check dags/ --select AIR301 --fix


Some of the fixes are marked as unsafe. Unsafe fixes usually do not break Dag code. They're marked as unsafe as they may change some runtime behavior. For more information, see `Fix Safety <https://docs.astral.sh/ruff/linter/#fix-safety>`_.
To trigger these fixes, run the following command:

.. code-block:: bash

    ruff check dags/ --select AIR301 --fix --unsafe-fixes

.. note::

    In AIR rules, unsafe fixes involve changing import paths while keeping the name of the imported member the same. For instance, changing the import from ``from airflow.sensors.base_sensor_operator import BaseSensorOperator`` to ``from airflow.sdk.bases.sensor import BaseSensorOperator`` requires ruff to remove the original import before adding the new one. In contrast, safe fixes include changes to both the member name and the import path, such as changing ``from airflow.datasets import Dataset`` to `from airflow.sdk import Asset``. These adjustments do not require ruff to remove the old import. To remove unused legacy imports, it is necessary to enable the `unused-import` rule (F401) <https://docs.astral.sh/ruff/rules/unused-import/#unused-import-f401>.

You can also configure these flags through configuration files. See `Configuring Ruff <https://docs.astral.sh/ruff/configuration/>`_ for details.

Step 4: Install the Standard Provider
--------------------------------------

- Some of the commonly used Operators which were bundled as part of the ``airflow-core`` package (for example ``BashOperator`` and ``PythonOperator``)
  have now been split out into a separate package: ``apache-airflow-providers-standard``.
- For convenience, this package can also be installed on Airflow 2.x versions, so that Dags can be modified to reference these Operators from the standard provider
  package instead of Airflow Core.

Step 5: Review custom operators for direct db access
----------------------------------------------------

- In Airflow 3 operators can not access the Airflow metadata database directly using database sessions.
  If you have custom operators, review the code to make sure there are no direct db access.
  You can follow examples in https://github.com/apache/airflow/issues/49187 to find how to modify your code if needed.

Step 6: Deployment Managers - Upgrade your Airflow Instance
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


If you have plugins that use Flask-AppBuilder views ( ``appbuilder_views`` ), Flask-AppBuilder menu items ( ``appbuilder_menu_items`` ), or Flask blueprints ( ``flask_blueprints`` ), you will either need to convert
them to FastAPI apps or ensure you install the FAB provider which provides a backwards compatibility layer for Airflow 3.
Ideally, you should convert your plugins to the Airflow 3 Plugin interface i.e External Views (``external_views``), Fast API apps (``fastapi_apps``)
and FastAPI middlewares (``fastapi_root_middlewares``).

Step 7: Changes to your startup scripts
---------------------------------------

In Airflow 3, the Webserver has become a generic API server. The API server can be started up using the following command:

.. code-block:: bash

    airflow api-server

The Dag processor must now be started independently, even for local or development setups:

.. code-block:: bash

    airflow dag-processor

You should now be able to start up your Airflow 3 instance.

.. _breaking-changes:

Breaking Changes
================

Some capabilities which were deprecated in Airflow 2.x are not available in Airflow 3.
These include:

- **SubDAGs**: Replaced by TaskGroups, Assets, and Data Aware Scheduling.
- **Sequential Executor**: Replaced by LocalExecutor, which can be used with SQLite for local development use cases.
- **CeleryKubernetesExecutor and LocalKubernetesExecutor**: Replaced by `Multiple Executor Configuration <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html#using-multiple-executors-concurrently>`_
- **SLAs**: Deprecated and removed; Will be replaced by forthcoming `Deadline Alerts <https://cwiki.apache.org/confluence/x/tglIEw>`_.
- **Subdir**: Used as an argument on many CLI commands, ``--subdir`` or ``-S`` has been superseded by :doc:`Dag bundles </administration-and-deployment/dag-bundles>`.
- **REST API** (``/api/v1``) replaced: Use the modern FastAPI-based stable ``/api/v2`` instead; see :doc:`Airflow API v2 </stable-rest-api-ref>` for details.
- **Some Airflow context variables**: The following keys are no longer available in a :ref:`task instance's context <templates:variables>`. If not replaced, will cause Dag errors:
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
- The ``catchup_by_default`` Dag parameter is now ``False`` by default.
- The ``create_cron_data_intervals`` configuration is now ``False`` by default. This means that the ``CronTriggerTimetable`` will be used by default instead of the ``CronDataIntervalTimetable``
- **Simple Auth** is now default ``auth_manager``. To continue using FAB as the Auth Manager, please install the FAB provider and set ``auth_manager`` to ``FabAuthManager``:

  .. code-block:: ini

      airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
- **AUTH API** api routes defined in the auth manager are prefixed with the ``/auth`` route. Urls consumed outside of the application such as oauth redirect urls will have to updated accordingly. For example an oauth redirect url that was ``https://<your-airflow-url.com>/oauth-authorized/google`` in Airflow 2.x will be ``https://<your-airflow-url.com>/auth/oauth-authorized/google`` in Airflow 3.x

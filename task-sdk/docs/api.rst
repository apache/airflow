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

airflow.sdk API Reference
=========================

This page documents the full public API exposed in Airflow 3.0+ via the Task SDK python module.

If something is not on this page it is best to assume that it is not part of the public API and use of it is entirely at your own risk
-- we won't go out of our way break usage of them, but we make no promises either.

Defining Dags
-------------
.. autoapiclass:: airflow.sdk.DAG


Configuration
-------------

The ``conf`` object is available as part of the Task SDK. It provides an interface to the
configurations, allowing you to read and interact with Airflow configuration values.


Macros
------

The ``macros`` module is available as part of the Task SDK. It provides builtin utility functions
for date manipulation and other common operations in Jinja templates and task code.

Available functions include:

- ``ds_add(ds, days)`` - Add or subtract days from a date string
- ``ds_format(ds, input_format, output_format)`` - Format datetime strings
- ``ds_format_locale(ds, input_format, output_format, locale)`` - Format datetime strings with locale support
- ``datetime_diff_for_humans(dt, since)`` - Human-readable datetime differences

The module also provides direct access to commonly used standard library modules:
``json``, ``time``, ``uuid``, ``dateutil``, and ``random``.

Decorators
----------
.. autoapifunction:: airflow.sdk.dag
.. autoapifunction:: airflow.sdk.task

Task Decorators:

- ``@task.run_if(condition, skip_message=None)``
  Run the task only if the given condition is met; otherwise the task is skipped.  The condition is a callable
  that receives the task execution context and returns either a boolean or a tuple ``(bool, message)``.
- ``@task.skip_if(condition, skip_message=None)``
  Skip the task if the given condition is met, raising a skip exception with an optional message.
- Provider-specific task decorators under ``@task.<provider>``, e.g. ``@task.python``, ``@task.docker``, etc., dynamically loaded from registered providers.

.. autoapifunction:: airflow.sdk.task_group

.. autoapifunction:: airflow.sdk.setup

.. autoapifunction:: airflow.sdk.teardown

.. autofunction:: airflow.sdk.task
.. autofunction:: airflow.sdk.setup
.. autofunction:: airflow.sdk.teardown
.. autofunction:: airflow.sdk.asset


Bases
-----
.. autoapiclass:: airflow.sdk.BaseAsyncOperator

.. autoapiclass:: airflow.sdk.BaseOperator

.. autoapiclass:: airflow.sdk.BaseSensorOperator

.. autoapiclass:: airflow.sdk.BaseNotifier

.. autoapiclass:: airflow.sdk.BaseOperatorLink

.. autoapiclass:: airflow.sdk.BaseXCom

.. autoapiclass:: airflow.sdk.PokeReturnValue

.. autoapiclass:: airflow.sdk.BaseHook

Callbacks
---------
.. autoclass:: airflow.sdk.AsyncCallback

.. autoclass:: airflow.sdk.SyncCallback

Deadline Alerts
---------------
.. autoclass:: airflow.sdk.DeadlineAlert

.. autoclass:: airflow.sdk.DeadlineReference

Connections & Variables
-----------------------
.. autoapiclass:: airflow.sdk.Connection

.. autoapiclass:: airflow.sdk.Variable

Tasks & Operators
-----------------
.. autoapiclass:: airflow.sdk.TaskGroup

.. autoapiclass:: airflow.sdk.XComArg

.. autoapifunction:: airflow.sdk.literal

.. autoapiclass:: airflow.sdk.Param

.. autoclass:: airflow.sdk.ParamsDict

.. autoclass:: airflow.sdk.TriggerRule

.. autoapifunction:: airflow.sdk.get_current_context

.. autoapifunction:: airflow.sdk.get_parsing_context

State Enums
-----------
.. autoclass:: airflow.sdk.TaskInstanceState

.. autoclass:: airflow.sdk.DagRunState

.. autoclass:: airflow.sdk.WeightRule

Setting Dependencies
~~~~~~~~~~~~~~~~~~~~
.. autoapifunction:: airflow.sdk.chain

.. autoapifunction:: airflow.sdk.chain_linear

.. autoapifunction:: airflow.sdk.cross_downstream

Edges & Labels
~~~~~~~~~~~~~~
.. autoapiclass:: airflow.sdk.EdgeModifier

.. autoapiclass:: airflow.sdk.Label

Assets
------
.. autoapiclass:: airflow.sdk.Asset

.. autoapiclass:: airflow.sdk.AssetAlias

.. autoapiclass:: airflow.sdk.AssetAll

.. autoapiclass:: airflow.sdk.AssetAny

.. autoapiclass:: airflow.sdk.AssetWatcher

.. autoapiclass:: airflow.sdk.Metadata

Timetables
----------
.. autoapiclass:: airflow.sdk.AssetOrTimeSchedule

.. autoapiclass:: airflow.sdk.CronDataIntervalTimetable

.. autoapiclass:: airflow.sdk.CronTriggerTimetable

.. autoapiclass:: airflow.sdk.CronPartitionTimetable

.. autoapiclass:: airflow.sdk.DeltaDataIntervalTimetable

.. autoapiclass:: airflow.sdk.DeltaTriggerTimetable

.. autoapiclass:: airflow.sdk.EventsTimetable

.. autoapiclass:: airflow.sdk.MultipleCronTriggerTimetable

.. autoapiclass:: airflow.sdk.PartitionedAssetTimetable


Partition Mapper
----------------

.. autoapiclass:: airflow.sdk.PartitionMapper

.. autoapiclass:: airflow.sdk.IdentityMapper

.. autoapiclass:: airflow.sdk.HourlyMapper

.. autoapiclass:: airflow.sdk.DailyMapper

.. autoapiclass:: airflow.sdk.WeeklyMapper

.. autoapiclass:: airflow.sdk.MonthlyMapper

.. autoapiclass:: airflow.sdk.QuarterlyMapper

.. autoapiclass:: airflow.sdk.YearlyMapper

I/O Helpers
-----------
.. autoapiclass:: airflow.sdk.ObjectStoragePath

Execution Time Components
-------------------------
.. rubric:: Context

.. autoapiclass:: airflow.sdk.Context

The ``Context`` object represents the execution-time context available to tasks.
It corresponds to the same context that is exposed to Jinja templates during task execution.

For a complete list of available context variables (such as ``dag_run``,
``task_instance``, ``logical_date``, etc.), see the
:ref:`Templates reference <templates-ref>`.

.. rubric:: Logging

.. autofunction:: airflow.sdk.log.mask_secret

Observability
-------------
.. autoclass:: airflow.sdk.Trace

Everything else
---------------

.. autoapimodule:: airflow.sdk
  :members:
  :special-members: __version__
  :exclude-members: BaseAsyncOperator, BaseOperator, DAG, dag, asset, Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher, TaskGroup, XComArg, get_current_context, get_parsing_context
  :undoc-members:
  :imported-members:
  :no-index:

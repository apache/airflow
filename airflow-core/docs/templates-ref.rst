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

.. _templates-ref:

Templates reference
===================

Variables, macros and filters can be used in templates (see the :ref:`concepts:jinja-templating` section)

The following come for free out of the box with Airflow.
Additional custom macros can be added globally through :doc:`administration-and-deployment/plugins`, or at a Dag level through the
``DAG.user_defined_macros`` argument.

.. _templates:variables:

Variables
---------
The Airflow engine passes a few variables by default that are accessible
in all templates

=========================================== ===================== ===================================================================
Variable                                    Type                  Description
=========================================== ===================== ===================================================================
``{{ data_interval_start }}``               `pendulum.DateTime`_  Start of the data interval. Added in version 2.2.
``{{ data_interval_end }}``                 `pendulum.DateTime`_  End of the data interval. Added in version 2.2.
``{{ logical_date }}``                      `pendulum.DateTime`_  | A date-time that logically identifies the current Dag run. This value does not contain any semantics, but is simply a value for identification.
                                                                  | Use ``data_interval_start`` and ``data_interval_end`` instead if you want a value that has real-world semantics,
                                                                  | such as to get a slice of rows from the database based on timestamps.
``{{ exception }}``                         None | str |          | Error occurred while running task instance.
                                            Exception             |
                                            KeyboardInterrupt     |
``{{ prev_data_interval_start_success }}``  `pendulum.DateTime`_  | Start of the data interval of the prior successful :class:`~airflow.models.dagrun.DagRun`.
                                            | ``None``            | Added in version 2.2.
``{{ prev_data_interval_end_success }}``    `pendulum.DateTime`_  | End of the data interval of the prior successful :class:`~airflow.models.dagrun.DagRun`.
                                            | ``None``            | Added in version 2.2.
``{{ prev_start_date_success }}``           `pendulum.DateTime`_  Start date from prior successful :class:`~airflow.models.dagrun.DagRun` (if available).
                                            | ``None``
``{{ prev_end_date_success }}``             `pendulum.DateTime`_  End date from prior successful :class:`~airflow.models.dagrun.DagRun` (if available).
                                            | ``None``
``{{ start_date }}``                        `pendulum.DateTime`_  Datetime of when current task has been started.
``{{ inlets }}``                            list                  List of inlets declared on the task.
``{{ inlet_events }}``                      dict[str, ...]        Access past events of inlet assets. See :doc:`Assets <authoring-and-scheduling/asset-scheduling>`. Added in version 2.10.
``{{ outlets }}``                           list                  List of outlets declared on the task.
``{{ outlet_events }}``                     dict[str, ...]        | Accessors to attach information to asset events that will be emitted by the current task.
                                                                  | See :doc:`Assets <authoring-and-scheduling/asset-scheduling>`. Added in version 2.10.
``{{ dag }}``                               DAG                   The currently running :class:`~airflow.models.dag.DAG`. You can read more about Dags in :doc:`Dags <core-concepts/dags>`.
``{{ task }}``                              BaseOperator          | The currently running :class:`~airflow.models.baseoperator.BaseOperator`. You can read more about Tasks in :doc:`core-concepts/operators`
``{{ task_reschedule_count }}``             int                   How many times current task has been rescheduled. Relevant to ``mode="reschedule"`` sensors.
``{{ macros }}``                                                  | A reference to the macros package. See Macros_ below.
``{{ task_instance }}``                     TaskInstance          The currently running :class:`~airflow.models.taskinstance.TaskInstance`.
``{{ ti }}``                                TaskInstance          Same as ``{{ task_instance }}``.
``{{ params }}``                            dict[str, Any]        | The user-defined params. This can be overridden by the mapping
                                                                  | passed to ``trigger_dag -c`` if ``dag_run_conf_overrides_params``
                                                                  | is enabled in ``airflow.cfg``.
``{{ var.value }}``                                               Airflow variables. See `Airflow Variables in Templates`_ below.
``{{ var.json }}``                                                Airflow variables. See `Airflow Variables in Templates`_ below.
``{{ conn }}``                                                    Airflow connections. See `Airflow Connections in Templates`_ below.
``{{ task_instance_key_str }}``             str                   | A unique, human-readable key to the task instance. The format is
                                                                  | ``{dag_id}__{task_id}__{ds_nodash}``.
``{{ run_id }}``                            str                   The currently running :class:`~airflow.models.dagrun.DagRun` run ID.
``{{ dag_run }}``                           DagRun                The currently running :class:`~airflow.models.dagrun.DagRun`.
``{{ test_mode }}``                         bool                  Whether the task instance was run by the ``airflow test`` CLI.
``{{ map_index_template }}``                None | str            Template used to render the expanded task instance of a mapped task. Setting this value will be reflected in the rendered result.
``{{ expanded_ti_count }}``                 int | ``None``        | Number of task instances that a mapped task was expanded into. If
                                                                  | the current task is not mapped, this should be ``None``.
                                                                  | Added in version 2.5.
``{{ triggering_asset_events }}``           dict[str,             | If in an Asset Scheduled Dag, a map of Asset URI to a list of triggering :class:`~airflow.models.asset.AssetEvent`
                                            list[AssetEvent]]     | (there may be more than one, if there are multiple Assets with different frequencies).
                                                                  | Read more here :doc:`Assets <authoring-and-scheduling/asset-scheduling>`.
                                                                  | Added in version 2.4.
=========================================== ===================== ===================================================================

The following are only available when the DagRun has a ``logical_date``

=========================================== ===================== ===================================================================
Variable                                    Type                  Description
=========================================== ===================== ===================================================================
``{{ ds }}``                                str                   | The Dag run's logical date as ``YYYY-MM-DD``.
                                                                  | Same as ``{{ logical_date | ds }}``.
``{{ ds_nodash }}``                         str                   Same as ``{{ logical_date | ds_nodash }}``.
``{{ ts }}``                                str                   | Same as ``{{ logical_date | ts }}``.
                                                                  | Example: ``2018-01-01T00:00:00+00:00``.
``{{ ts_nodash_with_tz }}``                 str                   | Same as ``{{ logical_date | ts_nodash_with_tz }}``.
                                                                  | Example: ``20180101T000000+0000``.
``{{ ts_nodash }}``                         str                   | Same as ``{{ logical_date | ts_nodash }}``.
                                                                  | Example: ``20180101T000000``.
=========================================== ===================== ===================================================================

.. note::

    The Dag run's logical date, and values derived from it, such as ``ds`` and
    ``ts``, **should not** be considered unique in a Dag. Use ``run_id`` instead.

Accessing Airflow context variables from TaskFlow tasks
-------------------------------------------------------

While ``@task`` decorated tasks don't support rendering jinja templates passed as arguments,
all of the variables listed above can be accessed directly from tasks. The following code block
is an example of accessing a ``task_instance`` object from its task:

.. include:: /../../devel-common/src/docs/shared/template-examples/taskflow.rst

Note that you can access the object's attributes and methods with simple
dot notation. Here are some examples of what is possible:
``{{ task.owner }}``, ``{{ task.task_id }}``, ``{{ ti.hostname }}``, ...
Refer to the models documentation for more information on the objects'
attributes and methods.

Airflow Variables in Templates
------------------------------
The ``var`` template variable allows you to access Airflow Variables.
You can access them as either plain-text or JSON. If you use JSON, you are
also able to walk nested structures, such as dictionaries like:
``{{ var.json.my_dict_var.key1 }}``.

It is also possible to fetch a variable by string if needed (for example your variable key contains dots) with
``{{ var.value.get('my.var', 'fallback') }}`` or
``{{ var.json.get('my.dict.var', {'key1': 'val1'}) }}``. Defaults can be
supplied in case the variable does not exist.


Airflow Connections in Templates
---------------------------------
Similarly, Airflow Connections data can be accessed via the ``conn`` template variable. For example, you could use expressions in your templates like ``{{ conn.my_conn_id.login }}``,
``{{ conn.my_conn_id.password }}``, etc.

Just like with ``var`` it's possible to fetch a connection by string  (e.g. ``{{ conn.get('my_conn_id_'+index).host }}``
) or provide defaults (e.g ``{{ conn.get('my_conn_id', {"host": "host1", "login": "user1"}).host }}``).

Additionally, the ``extras`` field of a connection can be fetched as a Python Dictionary with the ``extra_dejson`` field, e.g.
``conn.my_aws_conn_id.extra_dejson.region_name`` would fetch ``region_name`` out of ``extras``.
This way, defaults in ``extras`` can be provided as well (e.g. ``{{ conn.my_aws_conn_id.extra_dejson.get('region_name', 'Europe (Frankfurt)') }}``).

Filters
-------

Airflow defines some Jinja filters that can be used to format values.

For example, using ``{{ logical_date | ds }}`` will output the logical_date in the ``YYYY-MM-DD`` format.

=====================  ============  ==================================================================
Filter                 Operates on   Description
=====================  ============  ==================================================================
``ds``                 datetime      Format the datetime as ``YYYY-MM-DD``
``ds_nodash``          datetime      Format the datetime as ``YYYYMMDD``
``ts``                 datetime      Same as ``.isoformat()``, Example: ``2018-01-01T00:00:00+00:00``
``ts_nodash``          datetime      Same as ``ts`` filter without ``-``, ``:`` or TimeZone info.
                                     Example: ``20180101T000000``
``ts_nodash_with_tz``  datetime      As ``ts`` filter without ``-`` or ``:``. Example
                                     ``20180101T000000+0000``
=====================  ============  ==================================================================


.. _templates:macros:

Macros
------
Macros are a way to expose objects to your templates and live under the
``macros`` namespace in your templates.

A few commonly used libraries and methods are made available.

=================================   ==============================================
Variable                            Description
=================================   ==============================================
``macros.datetime``                 The standard lib's :class:`datetime.datetime`
``macros.timedelta``                The standard lib's :class:`datetime.timedelta`
``macros.dateutil``                 A reference to the ``dateutil`` package
``macros.time``                     The standard lib's :mod:`time`
``macros.uuid``                     The standard lib's :mod:`uuid`
``macros.random``                   The standard lib's :class:`random.random`
=================================   ==============================================

Some Airflow specific macros are also defined:

.. automodule:: airflow.sdk.execution_time.macros
    :members:

.. _pendulum.DateTime: https://pendulum.eustace.io/docs/#introduction

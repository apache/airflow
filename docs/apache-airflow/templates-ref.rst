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
Additional custom macros can be added globally through :doc:`/plugins`, or at a DAG level through the
``DAG.user_defined_macros`` argument.

.. _templates:variables:

Variables
---------
The Airflow engine passes a few variables by default that are accessible
in all templates

==========================================  ====================================
Variable                                    Description
==========================================  ====================================
``{{ data_interval_start }}``               Start of the data interval (`pendulum.DateTime`_).
``{{ data_interval_end }}``                 End of the data interval (`pendulum.DateTime`_).
``{{ ds }}``                                The DAG run's logical date as ``YYYY-MM-DD``.
                                            Same as ``{{ dag_run.logical_date | ds }}``.
``{{ ds_nodash }}``                         Same as ``{{ dag_run.logical_date | ds_nodash }}``.
``{{ ts }}``                                Same as ``{{ dag_run.logical_date | ts }}``.
                                            Example: ``2018-01-01T00:00:00+00:00``.
``{{ ts_nodash_with_tz }}``                 Same as ``{{ dag_run.logical_date | ts_nodash_with_tz }}``.
                                            Example: ``20180101T000000+0000``.
``{{ ts_nodash }}``                         Same as ``{{ dag_run.logical_date | ts_nodash }}``.
                                            Example: ``20180101T000000``.
``{{ prev_data_interval_start_success }}``  Start of the data interval from prior successful DAG run
                                            (`pendulum.DateTime`_ or ``None``).
``{{ prev_data_interval_end_success }}``    End of the data interval from prior successful DAG run
                                            (`pendulum.DateTime`_ or ``None``).
``{{ prev_start_date_success }}``           Start date from prior successful dag run (if available)
                                            (`pendulum.DateTime`_ or ``None``).
``{{ dag }}``                               The DAG object.
``{{ task }}``                              The Task object.
``{{ macros }}``                            A reference to the macros package, described below.
``{{ task_instance }}``                     The task_instance object.
``{{ ti }}``                                Same as ``{{ task_instance }}``.
``{{ params }}``                            A reference to the user-defined params dictionary which can be
                                            overridden by the dictionary passed through ``trigger_dag -c`` if
                                            you enabled ``dag_run_conf_overrides_params`` in ``airflow.cfg``.
``{{ var.value.my_var }}``                  Global defined variables represented as a dictionary.
``{{ var.json.my_var.path }}``              Global defined variables represented as a dictionary.
                                            With deserialized JSON object, append the path to the key within
                                            the JSON object.
``{{ conn.my_conn_id }}``                   Connection represented as a dictionary.
``{{ task_instance_key_str }}``             A unique, human-readable key to the task instance formatted
                                            ``{dag_id}__{task_id}__{ds_nodash}``.
``{{ conf }}``                              The full configuration object located at
                                            ``airflow.configuration.conf`` which represents the content of
                                            your ``airflow.cfg``.
``{{ run_id }}``                            The ``run_id`` of the current DAG run.
``{{ dag_run }}``                           A reference to the DagRun object.
``{{ test_mode }}``                         Whether the task instance was called using the CLI's test
                                            subcommand.
==========================================  ====================================

.. note::

    The DAG run's logical date, and values derived from it, such as ``ds`` and
    ``ts``, **should not** be considered unique in a DAG. Use ``run_id`` instead.

The following variables are deprecated. They are kept for backward compatibility, but you should convert
existing code to use other variables instead.

=====================================   ====================================
Deprecated Variable                     Description
=====================================   ====================================
``{{ execution_date }}``                the execution date (logical date), same as ``dag_run.logical_date``
``{{ next_execution_date }}``           the logical date of the next scheduled run (if applicable);
                                        you may be able to use ``data_interval_end`` instead
``{{ next_ds }}``                       the next execution date as ``YYYY-MM-DD`` if exists, else ``None``
``{{ next_ds_nodash }}``                the next execution date as ``YYYYMMDD`` if exists, else ``None``
``{{ prev_execution_date }}``           the logical date of the previous scheduled run (if applicable)
``{{ prev_ds }}``                       the previous execution date as ``YYYY-MM-DD`` if exists, else ``None``
``{{ prev_ds_nodash }}``                the previous execution date as ``YYYYMMDD`` if exists, else ``None``
``{{ yesterday_ds }}``                  the day before the execution date as ``YYYY-MM-DD``
``{{ yesterday_ds_nodash }}``           the day before the execution date as ``YYYYMMDD``
``{{ tomorrow_ds }}``                   the day after the execution date as ``YYYY-MM-DD``
``{{ tomorrow_ds_nodash }}``            the day after the execution date as ``YYYYMMDD``
``{{ prev_execution_date_success }}``   execution date from prior successful dag run

=====================================   ====================================

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

It is also possible to fetch a variable by string if needed with
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

Filters
-------

Airflow defines some Jinja filters that can be used to format values.

For example, using ``{{ execution_date | ds }}`` will output the execution_date in the ``YYYY-MM-DD`` format.

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

Some airflow specific macros are also defined:

.. automodule:: airflow.macros
    :members:

.. automodule:: airflow.macros.hive
    :members:

.. _pendulum.DateTime: https://pendulum.eustace.io/docs/#introduction

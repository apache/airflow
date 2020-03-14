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



.. _howto/operator:PythonOperator:

PythonOperator
==============

Use the :class:`~airflow.operators.python.PythonOperator` to execute
Python callables.

.. exampleinclude:: ../../../airflow/example_dags/example_python_operator.py
    :language: python
    :start-after: [START howto_operator_python]
    :end-before: [END howto_operator_python]


Default kwargs
-----------------


kwargs is a dictionary where the values are templates that will get templated by the Airflow engine.

Default kwargs is passed to the function (the example above is 'ds'). And it can bring in multiple arguments as well as one.


=====================================   ====================================
kwargs                                   Description
=====================================   ====================================
``conf``                                  the full configuration object located at ``airflow.configuration.conf`` which represents the content of your ``airflow.cfg``
``dag``                                   the DAG object
``dag_run``                               a reference to the DagRun object
``ds``                                    the execution date as ``YYYY-MM-DD``
``ds_nodash``                             the execution date as ``YYYYMMDD``
``execution_date``                        the execution_date (logical date)
``inlets``                                []
``macros``                                a reference to the macros package
``next_ds``                               the next execution date as ``YYYY-MM-DD`` if ``ds`` is ``2018-01-01`` and ``schedule_interval`` is ``@weekly``, ``next_ds`` will be ``2018-01-08``
``next_ds_nodash``                        the next execution date as ``YYYYMMDD`` if exists, else ``None``
``next_execution_date``                   the next execution date (`pendulum.Pendulum`_)
``outlets``                               []
``params``                                a reference to the user-defined params dictionary which can be overridden by the dictionary passed through ``trigger_dag -c`` if you enabled ``dag_run_conf_overrides_params`` in ``airflow.cfg``
``prev_ds``                               the previous execution date as ``YYYY-MM-DD`` if ``ds`` is ``2018-01-08`` and ``schedule_interval`` is ``@weekly``, ``prev_ds`` will be ``2018-01-01``
``prev_ds_nodash``                        the previous execution date as ``YYYYMMDD`` if exists, else ``None``
``prev_execution_date``                   the previous execution date (if available) (`pendulum.Pendulum`_)
``prev_execution_date_success``           execution date from prior successful dag run (if available) (`pendulum.Pendulum`_)
``prev_start_date_success``               start date from prior successful dag run (if available) (pendulum.Pendulum_)
``run_id``                                the ``run_id`` of the current DAG run
``task``                                  the Task object
``task_instance``                         the task_instance object
``task_instance_key_str``                 a unique, human-readable key to the task instance formatted ``{dag_id}_{task_id}_{ds}``
``templates_dict``                        a dictionary where the values are templates that will get templated by the Airflow engine
``test_mode``                             whether the task instance was called using the CLI's test subcommand
``ti``                                    same as ``task_instance``
``tomorrow_ds``                           the day after the execution date as ``YYYY-MM-DD``
``tomorrow_ds_nodash``                    the day after the execution date as ``YYYYMMDD``
``ts``                                    same as ``execution_date.isoformat()``. Example: ``2018-01-01T00:00:00+00:00``
``ts_nodash``                             same as ``ts`` without ``-``, ``:`` and TimeZone info. Example: ``20180101T000000``
``ts_nodash_with_tz``                     same as ``ts`` without ``-`` and ``:``. Example: ``20180101T000000+0000``
``var``                                   global defined variables represented as a dictionary (json, value)
``yesterday_ds``                          the day before the execution date as ``YYYY-MM-DD``
``yesterday_ds_nodash``                   the day before the execution date as ``YYYYMMDD``
=====================================   ====================================



Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Use the ``op_args`` and ``op_kwargs`` arguments to pass additional arguments
to the Python callable.

.. exampleinclude:: ../../../airflow/example_dags/example_python_operator.py
    :language: python
    :start-after: [START howto_operator_python_kwargs]
    :end-before: [END howto_operator_python_kwargs]

Templating
^^^^^^^^^^

Airflow passes in an additional set of keyword arguments: one for each of the
:doc:`Jinja template variables <../../macros-ref>` and a ``templates_dict``
argument.

The ``templates_dict`` argument is templated, so each value in the dictionary
is evaluated as a :ref:`Jinja template <jinja-templating>`.

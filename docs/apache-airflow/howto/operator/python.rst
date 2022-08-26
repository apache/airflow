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

Use the ``@task`` decorator to execute Python callables.

.. warning::
    The ``@task`` decorator is recommended over the classic :class:`~airflow.operators.python.PythonOperator`
    to execute Python callables.

.. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_python]
    :end-before: [END howto_operator_python]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Pass extra arguments to the ``@task`` decorated function as you would with
a normal Python function.

.. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_python_kwargs]
    :end-before: [END howto_operator_python_kwargs]

Templating
^^^^^^^^^^

Airflow passes in an additional set of keyword arguments: one for each of the
:ref:`Jinja template variables <templates:variables>` and a ``templates_dict``
argument.

The ``templates_dict`` argument is templated, so each value in the dictionary
is evaluated as a :ref:`Jinja template <concepts:jinja-templating>`.



.. _howto/operator:PythonVirtualenvOperator:

PythonVirtualenvOperator
========================

Use the ``@task.virtualenv`` decorator to execute Python callables inside a new Python virtual environment.
The ``virtualenv`` package needs to be installed in the environment that runs Airflow (as optional dependency ``pip install airflow[virtualenv] --constraint ...``).

.. warning::
    The ``@task.virtualenv`` decorator is recommended over the classic :class:`~airflow.operators.python.PythonVirtualenvOperator`
    to execute Python callables inside new Python virtual environments.

.. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_python_venv]
    :end-before: [END howto_operator_python_venv]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Pass extra arguments to the ``@task.virtualenv`` decorated function as you would with a normal Python function.
Unfortunately, Airflow does not support serializing ``var``, ``ti`` and ``task_instance`` due to incompatibilities
with the underlying library. For Airflow context variables make sure that you either have access to Airflow through
setting ``system_site_packages`` to ``True`` or add ``apache-airflow`` to the ``requirements`` argument.
Otherwise you won't have access to the most context variables of Airflow in ``op_kwargs``.
If you want the context related to datetime objects like ``data_interval_start`` you can add ``pendulum`` and
``lazy_object_proxy``.

If additional parameters for package installation are needed pass them in ``requirements.txt`` as in the example below:

.. code-block::

  SomePackage==0.2.1 --pre --index-url http://some.archives.com/archives
  AnotherPackage==1.4.3 --no-index --find-links /my/local/archives

All supported options are listed in the `requirements file format <https://pip.pypa.io/en/stable/reference/requirements-file-format/#supported-options>`_.


.. _howto/operator:ShortCircuitOperator:

ShortCircuitOperator
========================

Use the :class:`~airflow.operators.python.ShortCircuitOperator` to control whether a pipeline continues
if a condition is satisfied or a truthy value is obtained. The evaluation of this condition and truthy value
is done via the output of a ``python_callable``. If the ``python_callable`` returns True or a truthy value,
the pipeline is allowed to continue and an :ref:`XCom <concepts:xcom>` of the output will be pushed. If the
output is False or a falsy value, the pipeline will be short-circuited based on the configured
short-circuiting (more on this later). In the example below, the tasks that follow the "condition_is_True"
ShortCircuitOperator will execute while the tasks downstream of the "condition_is_False" ShortCircuitOperator
will be skipped.


.. exampleinclude:: /../../airflow/example_dags/example_short_circuit_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_short_circuit]
    :end-before: [END howto_operator_short_circuit]


The "short-circuiting" can be configured to either respect or ignore the :ref:`trigger rule <concepts:trigger-rules>`
defined for downstream tasks. If ``ignore_downstream_trigger_rules`` is set to True, the default configuration, all
downstream tasks are skipped without considering the ``trigger_rule`` defined for tasks.  If this parameter is
set to False, the direct downstream tasks are skipped but the specified ``trigger_rule`` for other subsequent
downstream tasks are respected. In this short-circuiting configuration, the operator assumes the direct
downstream task(s) were purposely meant to be skipped but perhaps not other subsequent tasks. This
configuration is especially useful if only *part* of a pipeline should be short-circuited rather than all
tasks which follow the ShortCircuitOperator task.

In the example below, notice that the ShortCircuitOperator task is configured to respect downstream trigger
rules. This means while the tasks that follow the "short_circuit" ShortCircuitOperator task will be skipped
since the ``python_callable`` returns False, "task_7" will still execute as its set to execute when upstream
tasks have completed running regardless of status (i.e. the ``TriggerRule.ALL_DONE`` trigger rule).

.. exampleinclude:: /../../airflow/example_dags/example_short_circuit_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_short_circuit_trigger_rules]
    :end-before: [END howto_operator_short_circuit_trigger_rules]



Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Both the ``op_args`` and ``op_kwargs`` arguments can be used in same way as described for the PythonOperator.


Templating
^^^^^^^^^^

Jinja templating can be used in same way as described for the PythonOperator.

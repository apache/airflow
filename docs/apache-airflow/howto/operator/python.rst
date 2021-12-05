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

.. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_python]
    :end-before: [END howto_operator_python]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

Use the ``op_args`` and ``op_kwargs`` arguments to pass additional arguments
to the Python callable.

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

Use the :class:`~airflow.operators.python.PythonVirtualenvOperator` to execute
Python callables inside a new Python virtual environment.

.. exampleinclude:: /../../airflow/example_dags/example_python_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_python_venv]
    :end-before: [END howto_operator_python_venv]

Passing in arguments
^^^^^^^^^^^^^^^^^^^^

You can use the ``op_args`` and ``op_kwargs`` arguments the same way you use it in the PythonOperator.
Unfortunately we currently do not support to serialize ``var`` and ``ti`` / ``task_instance`` due to incompatibilities
with the underlying library. For airflow context variables make sure that you either have access to Airflow through
setting ``system_site_packages`` to ``True`` or add ``apache-airflow`` to the ``requirements`` argument.
Otherwise you won't have access to the most context variables of Airflow in ``op_kwargs``.
If you want the context related to datetime objects like ``data_interval_start`` you can add ``pendulum`` and
``lazy_object_proxy``.

If additional parameters for package installation are needed pass them in ``requirements.txt`` as in the example below:

.. code-block::

  SomePackage==0.2.1 --pre --index-url http://some.archives.com/archives
  AnotherPackage==1.4.3 --no-index --find-links /my/local/archives

All supported options are listed in the `requirements file format <https://pip.pypa.io/en/stable/reference/requirements-file-format/#supported-options>`_.


Templating
^^^^^^^^^^

You can use jinja Templating the same way you use it in PythonOperator.

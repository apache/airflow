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

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

.. _concepts:params:

Params
======

Params are how Airflow provides runtime configuration to tasks.
When you trigger a DAG manually, you can modify its Params before the dagrun starts.
If the user-supplied values don't pass validation, Airflow shows a warning instead of creating the dagrun.
(For scheduled runs, the default values are used.)

Adding Params to a DAG
----------------------

To add Params to a :class:`~airflow.models.dag.DAG`, initialize it with the ``params`` kwarg.
Use a dictionary that maps Param names to a either a :class:`~airflow.models.param.Param` or an object indicating the parameter's default value.

.. code-block::

    from airflow import DAG
    from airflow.models.param import Param

    with DAG(
        "the_dag",
        params={
            "x": Param(5, type="integer", minimum=3),
            "y": 6
        },
    ) as the_dag:

Referencing Params in a Task
----------------------------

Params are stored as ``params`` in the :ref:`template context <templates-ref>`.
So you can reference them in a template.

.. code-block::

    PythonOperator(
        task_id="from_template",
        op_args=[
            "{{ params.int_param + 10 }}",
        ],
        python_callable=(
            lambda x: print(x)
        ),
    )

Even though Params can use a variety of types, the default behavior of templates is to provide your task with a string.
You can change this by setting ``render_template_as_native_obj=True`` while initializing the :class:`~airflow.models.dag.DAG`.

.. code-block::

    with DAG(
        "the_dag",
        params={"x": Param(5, type="integer", minimum=3)},
        render_template_as_native_obj=True
    ) as the_dag:


This way, the Param's type is respected when its provided to your task.

.. code-block::

    # prints <class 'str'> by default
    # prints <class 'int'> if render_template_as_native_obj=True
    PythonOperator(
        task_id="template_type",
        op_args=[
            "{{ params.int_param }}",
        ],
        python_callable=(
            lambda x: print(type(x))
        ),
    )

Another way to access your param is via a task's ``context`` kwarg.

.. code-block::

    def print_x(**context):
        print(context["params"]["x"])

    PythonOperator(
        task_id="print_x",
        python_callable=print_it,
    )

Task-level Params
-----------------

You can also add Params to individual tasks.

.. code-block::

    PythonOperator(
        task_id="print_x",
        params={"x": 10},
        python_callable=print_it,
    )

If there's already a dag param with that name, the task-level default will take precedence over the dag-level default.
If a user supplies their own value when the DAG was triggered, Airflow ignores all defaults and uses the user's value.

JSON Schema Validation
----------------------

:class:`~airflow.modules.param.Param` makes use of ``json-schema <https://json-schema.org/>``, so you can use the full json-schema specifications mentioned at https://json-schema.org/draft/2020-12/json-schema-validation.html to define ``Param`` objects.

.. code-block::

    with DAG(
        "my_dag",
        params={
            # a int with a default value
            "int_param": Param(10, type="integer", minimum=0, maximum=20),

            # a required param which can be of multiple types
            "dummy": Param(type=["null", "number", "string"]),

            # a param which uses json-schema formatting
            "email": Param(
                default="example@example.com",
                type="string",
                format="idn-email",
                minLength=5,
                maxLength=255,
            ),
        },
    ) as my_dag:

.. note::
    As of now, for security reasons, one can not use Param objects derived out of custom classes. We are
    planning to have a registration system for custom Param classes, just like we've for Operator ExtraLinks.

Disabling Runtime Param Modification
------------------------------------

The ability to update params while triggering a DAG depends on the flag ``core.dag_run_conf_overrides_params``.
Setting this config to ``False`` will effectively turn your default params into constants.

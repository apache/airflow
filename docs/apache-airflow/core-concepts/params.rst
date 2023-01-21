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

Params enable you to provide runtime configuration to tasks. You can configure default Params in your DAG
code and supply additional Params, or overwrite Param values, at runtime when you trigger a DAG. Param values
are validated with JSON Schema. For scheduled DAG runs, default Param values are used.

DAG-level Params
----------------

To add Params to a :class:`~airflow.models.dag.DAG`, initialize it with the ``params`` kwarg.
Use a dictionary that maps Param names to either a :class:`~airflow.models.param.Param` or an object indicating the parameter's default value.

.. code-block::
   :emphasize-lines: 6-9

    from airflow import DAG
    from airflow.models.param import Param

    with DAG(
        "the_dag",
        params={
            "x": Param(5, type="integer", minimum=3),
            "y": 6
        },
    ):

Task-level Params
-----------------

You can also add Params to individual tasks.

.. code-block::

    PythonOperator(
        task_id="print_x",
        params={"x": 10},
        python_callable=print_it,
    )

Task-level params take precedence over DAG-level params, and user-supplied params (when triggering the DAG)
take precedence over task-level params.

Referencing Params in a Task
----------------------------

Params can be referenced in :ref:`templated strings <templates-ref>` under ``params``. For example:

.. code-block::
   :emphasize-lines: 4

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
   :emphasize-lines: 4

    with DAG(
        "the_dag",
        params={"x": Param(5, type="integer", minimum=3)},
        render_template_as_native_obj=True
    ):


This way, the Param's type is respected when it's provided to your task:

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
   :emphasize-lines: 1,2

    def print_x(**context):
        print(context["params"]["x"])

    PythonOperator(
        task_id="print_x",
        python_callable=print_x,
    )

JSON Schema Validation
----------------------

:class:`~airflow.modules.param.Param` makes use of `JSON Schema <https://json-schema.org/>`_, so you can use the full JSON Schema specifications mentioned at https://json-schema.org/draft/2020-12/json-schema-validation.html to define ``Param`` objects.

.. code-block::

    with DAG(
        "my_dag",
        params={
            # an int with a default value
            "int_param": Param(10, type="integer", minimum=0, maximum=20),

            # a required param which can be of multiple types
            # a param must have a default value
            "dummy": Param(5, type=["null", "number", "string"]),

            # an enum param, must be one of three values
            "enum_param": Param("foo", enum=["foo", "bar", 42]),

            # a param which uses json-schema formatting
            "email": Param(
                default="example@example.com",
                type="string",
                format="idn-email",
                minLength=5,
                maxLength=255,
            ),
        },
    ):

.. note::
    As of now, for security reasons, one can not use Param objects derived out of custom classes. We are
    planning to have a registration system for custom Param classes, just like we've for Operator ExtraLinks.

Disabling Runtime Param Modification
------------------------------------

The ability to update params while triggering a DAG depends on the flag ``core.dag_run_conf_overrides_params``.
Setting this config to ``False`` will effectively turn your default params into constants.

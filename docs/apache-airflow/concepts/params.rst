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

Params
======

Params are Airflow's way to provide runtime configuration to tasks when a DAG gets triggered manually.
To use them, initialize your DAG with a dictionary where the keys are strings with each param's name, and the values are ``Param`` objects.

``Param`` makes use of `json-schema <https://json-schema.org/>`, so one can use the full json-schema specifications mentioned at https://json-schema.org/draft/2020-12/json-schema-validation.html to define the construct of a ``Param`` objects.
Or, if you want a default value without any validation, you can use literals instead.

.. code-block::
   :caption a simple DAG with a parameter
        from airflow import DAG
        from airflow.models.param import Param
        from airflow.operators.python_operator import PythonOperator

        with DAG(
            "params",
            params={"x": Param(5, type="integer", minimum=3),
                    "y": 6},
        ) as the_dag:

            def print_x(**context):
                print(context["params"]["x"])

            # prints 5, or whatever the user provided at trigger time
            PythonOperator(
                task_id="print_x",
                python_callable=print_it,
            )

Params can also be added to individual tasks.
If there's already a dag param with that name, the task-level default will take precedence over the dag-level default.

.. code-block::
   :caption tasks can have parameters too

            # prints 10, or whatever the user provided at trigger time
            PythonOperator(
                task_id="print_x",
                params={"x": 10},
                python_callable=print_it,
            )

When a user manually triggers a dag, they can change the parameters that are provided to the dagrun.
This can be disabled by setting ``core.dag_run_conf_overrides_params = False``, which will prevent the user from changing the params.
If the user-supplied values don't pass validation, Airflow will show the user a warning it will not create the dagrun.


You can reference dag params via a templated task argument:

.. code-block::
   :caption use a template
        from airflow import DAG
        from airflow.models.param import Param
        from airflow.operators.python import PythonOperator

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

            # instead of getting strings from templates, get objects
            render_template_as_native_obj=True,

        ) as my_dag:

            PythonOperator(
                task_id="from_template",
                op_args=[
                    "{{ params.int_param + 10 }}",
                ],
                python_callable=(
                    lambda x: print(type(x), x)
                    # '<class 'str'> 20' by default
                    # '<class 'int'> 20' if render_template_as_native_obj=True
                ),
            )

By default, Jinja templates create strings.
So if you have parameters that aren't strings, and you want to use templated task arguments, you might be interested in the ``render_template_as_native_obj`` DAG kwarg.
It will allow you to preserve the type of the parameter, even if you manipulate it in a template.

If templates aren't your style, you can access params in via the context.

.. code-block::
   :caption use the context kwarg

            # or you can reference them through the context
            def from_context(**context):
                int_param = context["params"]["int_param"]
                print(type(int_param), int_param + 10)
                # <class 'int'> 20

            PythonOperator(
                task_id="from_context",
                python_callable=from_context,
            )


.. note::
    As of now, for security reasons, one can not use Param objects derived out of custom classes. We are
    planning to have a registration system for custom Param classes, just like we've for Operator ExtraLinks.

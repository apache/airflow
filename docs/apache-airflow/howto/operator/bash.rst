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



.. _howto/operator:BashOperator:

BashOperator
============

Use the :class:`~airflow.providers.standard.operators.bash.BashOperator` to execute
commands in a `Bash <https://www.gnu.org/software/bash/>`__ shell. The Bash command or script to execute is
determined by:

1. The ``bash_command`` argument when using ``BashOperator``, or

2. If using the TaskFlow decorator, ``@task.bash``, a non-empty string value returned from the decorated callable.


.. tip::

    The ``@task.bash`` decorator is recommended over the classic ``BashOperator`` to execute Bash commands.


.. tab-set::

    .. tab-item:: @task.bash
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_bash_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_decorator_bash]
            :end-before: [END howto_decorator_bash]

    .. tab-item:: BashOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_bash_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_bash]
            :end-before: [END howto_operator_bash]


Templating
----------

You can use :ref:`Jinja templates <concepts:jinja-templating>` to parameterize the Bash command.

.. tab-set::

    .. tab-item:: @task.bash
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_bash_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_decorator_bash_template]
            :end-before: [END howto_decorator_bash_template]

    .. tab-item:: BashOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_bash_operator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_operator_bash_template]
            :end-before: [END howto_operator_bash_template]

Using the ``@task.bash`` TaskFlow decorator allows you to return a formatted string and take advantage of
having all :ref:`execution context variables directly accessible to decorated tasks <taskflow/accessing_context_variables>`.

.. exampleinclude:: /../../airflow/example_dags/example_bash_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_decorator_bash_context_vars]
    :end-before: [END howto_decorator_bash_context_vars]

You are encouraged to take advantage of this approach as it fits nicely into the overall TaskFlow paradigm.

.. caution::

    Care should be taken with "user" input when using Jinja templates in the Bash command as escaping and
    sanitization of the Bash command is not performed.

    This applies mostly to using ``dag_run.conf``, as that can be submitted via users in the Web UI. Most of
    the default template variables are not at risk.

    For example, do **not** do:

    .. tab-set::

        .. tab-item:: @task.bash
            :sync: taskflow

            .. code-block:: python

                @task.bash
                def bash_task() -> str:
                    return 'echo "Here is the message: \'{{ dag_run.conf["message"] if dag_run.conf else "" }}\'"'


                # Or directly accessing `dag_run.conf`
                @task.bash
                def bash_task(dag_run) -> str:
                    message = dag_run.conf["message"] if dag_run.conf else ""
                    return f'echo "here is the message: {message}"'

        .. tab-item:: BashOperator
            :sync: operator

            .. code-block:: python

                bash_task = BashOperator(
                    task_id="bash_task",
                    bash_command='echo "Here is the message: \'{{ dag_run.conf["message"] if dag_run.conf else "" }}\'"',
                )


    Instead, you should pass this via the ``env`` kwarg and use double-quotes inside the Bash command.

    .. tab-set::

        .. tab-item:: @task.bash
            :sync: taskflow

            .. code-block:: python

                @task.bash(env={"message": '{{ dag_run.conf["message"] if dag_run.conf else "" }}'})
                def bash_task() -> str:
                    return "echo \"here is the message: '$message'\""

        .. tab-item:: BashOperator
            :sync: operator

            .. code-block:: python

                bash_task = BashOperator(
                    task_id="bash_task",
                    bash_command="echo \"here is the message: '$message'\"",
                    env={"message": '{{ dag_run.conf["message"] if dag_run.conf else "" }}'},
                )


Skipping
--------

In general a non-zero exit code produces an AirflowException and thus a task failure.  In cases where it is
desirable to instead have the task end in a ``skipped`` state, you can exit with code ``99`` (or with another
exit code if you pass ``skip_on_exit_code``).

.. tab-set::

    .. tab-item:: @task.bash
        :sync: taskflow

        .. exampleinclude:: /../../airflow/example_dags/example_bash_decorator.py
            :language: python
            :dedent: 4
            :start-after: [START howto_decorator_bash_skip]
            :end-before: [END howto_decorator_bash_skip]

    .. tab-item:: BashOperator
        :sync: operator

        .. exampleinclude:: /../../airflow/example_dags/example_bash_operator.py
            :language: python
            :start-after: [START howto_operator_bash_skip]
            :end-before: [END howto_operator_bash_skip]


Output processor
----------------

The ``output_processor`` parameter allows you to specify a lambda function that processes the output of the bash script
before it is pushed as an XCom. This feature is particularly useful for manipulating the script's output directly within
the BashOperator, without the need for additional operators or tasks.

For example, consider a scenario where the output of the bash script is a JSON string. With the ``output_processor``,
you can transform this string into a JSON object before storing it in XCom. This simplifies the workflow and ensures
that downstream tasks receive the processed data in the desired format.

Here's how you can use the result_processor with the BashOperator:

.. tab-set::

    .. tab-item:: @task.bash
        :sync: taskflow

        .. code-block:: python

            @task.bash(output_processor=lambda output: json.loads(output))
            def bash_task() -> str:
                return """
                    jq -c '.[] | select(.lastModified > "{{ data_interval_start | ts_zulu }}" or .created > "{{ data_interval_start | ts_zulu }}")' \\
                    example.json
                """

    .. tab-item:: BashOperator
        :sync: operator

        .. code-block:: python

            bash_task = BashOperator(
                task_id="filter_today_changes",
                bash_command="""
                    jq -c '.[] | select(.lastModified > "{{ data_interval_start | ts_zulu }}" or .created > "{{ data_interval_start | ts_zulu }}")' \\
                    example.json
                """,
                output_processor=lambda output: json.loads(output),
            )


Executing commands from files
-----------------------------
Both the ``BashOperator`` and ``@task.bash`` TaskFlow decorator enables you to execute Bash commands stored
in files. The files **must** have a ``.sh`` or ``.bash`` extension.

With Jinja template
"""""""""""""""""""

You can execute bash script which contains Jinja templates. When you do so, Airflow
loads the content of your file, render the templates, and write the rendered script
into a temporary file. By default, the file is placed in a temporary directory
(under ``/tmp``). You can change this location with the ``cwd`` parameter.

.. caution::

    Airflow must have write access to ``/tmp`` or the ``cwd`` directory, to be
    able to write the temporary file to the disk.


To execute a bash script, place it in a location relative to the directory containing
the DAG file. So if your DAG file is in ``/usr/local/airflow/dags/test_dag.py``, you can
move your ``test.sh`` file to any location under ``/usr/local/airflow/dags/`` (Example:
``/usr/local/airflow/dags/scripts/test.sh``) and pass the relative path to ``bash_command``
as shown below:

.. tab-set::

    .. tab-item:: @tash.bash
        :sync: taskflow

        .. code-block:: python

            @task.bash
            def bash_example():
                # "scripts" folder is under "/usr/local/airflow/dags"
                return "scripts/test.sh"

    .. tab-item:: BashOperator
        :sync: operator

        .. code-block:: python

            t2 = BashOperator(
                task_id="bash_example",
                # "scripts" folder is under "/usr/local/airflow/dags"
                bash_command="scripts/test.sh",
            )

Creating separate folder for Bash scripts may be desirable for many reasons, like
separating your script's logic and pipeline code, allowing for proper code highlighting
in files composed in different languages, and general flexibility in structuring
pipelines.

It is also possible to define your ``template_searchpath`` as pointing to any folder
locations in the DAG constructor call.

.. tab-set::

    .. tab-item:: @task.bash
        :sync: taskflow

        .. code-block:: python
            :emphasize-lines: 1

            @dag(..., template_searchpath="/opt/scripts")
            def example_bash_dag():
                @task.bash
                def bash_example():
                    return "test.sh "

    .. tab-item:: BashOperator
        :sync: operator

        .. code-block:: python
            :emphasize-lines: 1

            with DAG("example_bash_dag", ..., template_searchpath="/opt/scripts"):
                t2 = BashOperator(
                    task_id="bash_example",
                    bash_command="test.sh ",
                )

Without Jinja template
""""""""""""""""""""""

If your script doesn't contains any Jinja template, disable Airflow's rendering by
adding a space after the script name.

.. tab-set::

    .. tab-item:: @task.bash
        :sync: taskflow

        .. code-block:: python
            :emphasize-lines: 3

            @task.bash
            def run_command_from_script() -> str:
                return "$AIRFLOW_HOME/scripts/example.sh "


            run_script = run_command_from_script()

    .. tab-item:: BashOperator
        :sync: operator

        .. code-block:: python
            :emphasize-lines: 3

            run_script = BashOperator(
                task_id="run_command_from_script",
                bash_command="$AIRFLOW_HOME/scripts/example.sh ",
            )


Jinja template not found
""""""""""""""""""""""""

If you encounter a "Template not found" exception when trying to execute a Bash script, add a space after the
script name. This is because Airflow tries to apply a Jinja template to it, which will fail.

.. tab-set::

    .. tab-item:: @task.bash
        :sync: taskflow

        .. code-block:: python

            @task.bash
            def bash_example():
                # This fails with 'Jinja template not found' error
                # return "/home/batcher/test.sh",
                # This works (has a space after)
                return "/home/batcher/test.sh "

    .. tab-item:: BashOperator
        :sync: operator

        .. code-block:: python

            BashOperator(
                task_id="bash_example",
                # This fails with 'Jinja template not found' error
                # bash_command="/home/batcher/test.sh",
                # This works (has a space after)
                bash_command="/home/batcher/test.sh ",
            )

However, if you want to use templating in your Bash script, do not add the space
and instead check the `bash script with Jinja template <#with-jinja-template>`_ section.

Enriching Bash with Python
--------------------------

The ``@task.bash`` TaskFlow decorator allows you to combine both Bash and Python into a powerful combination
within a task.

Using Python conditionals, other function calls, etc. within a ``@task.bash`` task can help define, augment,
or even build the Bash command(s) to execute.

For example, use conditional logic to determine task behavior:

.. exampleinclude:: /../../airflow/example_dags/example_bash_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_decorator_bash_conditional]
    :end-before: [END howto_decorator_bash_conditional]

Or call a function to help build a Bash command:

.. exampleinclude:: /../../airflow/example_dags/example_bash_decorator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_decorator_bash_build_cmd]
    :end-before: [END howto_decorator_bash_build_cmd]

There are numerous possibilities with this type of pre-execution enrichment.


.. _howto/operator:BashSensor:

BashSensor
==========

Use the :class:`~airflow.providers.standard.sensors.bash.BashSensor` to use arbitrary command for sensing. The command
should return 0 when it succeeds, any other value otherwise.

.. exampleinclude:: /../../airflow/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_bash_sensors]
    :end-before: [END example_bash_sensors]

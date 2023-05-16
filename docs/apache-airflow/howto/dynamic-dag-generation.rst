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



Dynamic DAG Generation
======================

This document describes creation of DAGs that have a structure generated dynamically, but where the number of
tasks in the DAG does not change between DAG Runs. If you want to implement a DAG where number of Tasks (or
Task Groups as of Airflow 2.6) can change based on the output/result of previous tasks, see
:doc:`/authoring-and-scheduling/dynamic-task-mapping`.

.. note:: Consistent sequence of generating tasks and task groups

    In all cases where you generate DAGs dynamically, you should make sure that Tasks and Task Groups
    are generated with consistent sequence every time the DAG is generated, otherwise you might end up with
    Tasks and Task Groups changing their sequence in the Grid View every time you refresh the page.
    This can be achieved for example by using a stable sorting mechanism in your Database queries or by using
    ``sorted()`` function in Python.

Dynamic DAGs with environment variables
.......................................

If you want to use variables to configure your code, you should always use
`environment variables <https://wiki.archlinux.org/title/environment_variables>`_ in your
top-level code rather than :doc:`Airflow Variables </core-concepts/variables>`. Using Airflow Variables
in top-level code creates a connection to the metadata DB of Airflow to fetch the value, which can slow
down parsing and place extra load on the DB. See the `best practices on Airflow Variables <best_practice:airflow_variables>`_
to make the best use of Airflow Variables in your DAGs using Jinja templates.

For example you could set ``DEPLOYMENT`` variable differently for your production and development
environments. The variable ``DEPLOYMENT`` could be set to ``PROD`` in your production environment and to
``DEV`` in your development environment. Then you could build your DAG differently in production and
development environment, depending on the value of the environment variable.

.. code-block:: python

    deployment = os.environ.get("DEPLOYMENT", "PROD")
    if deployment == "PROD":
        task = Operator(param="prod-param")
    elif deployment == "DEV":
        task = Operator(param="dev-param")


Generating Python code with embedded meta-data
..............................................

You can externally generate Python code containing the meta-data as importable constants.
Such constant can then be imported directly by your DAG and used to construct the object and build
the dependencies. This makes it easy to import such code from multiple DAGs without the need to find,
load and parse the meta-data stored in the constant - this is done automatically by Python interpreter
when it processes the "import" statement. This sounds strange at first, but it is surprisingly easy
to generate such code and make sure this is a valid Python code that you can import from your DAGs.

For example assume you dynamically generate (in your DAG folder), the ``my_company_utils/common.py`` file:

.. code-block:: python

    # This file is generated automatically !
    ALL_TASKS = ["task1", "task2", "task3"]

Then you can import and use the ``ALL_TASKS`` constant in all your DAGs like that:

.. code-block:: python

    from my_company_utils.common import ALL_TASKS

    with DAG(
        dag_id="my_dag",
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
    ):
        for task in ALL_TASKS:
            # create your operators and relations here
            ...

Don't forget that in this case you need to add empty ``__init__.py`` file in the ``my_company_utils`` folder
and you should add the ``my_company_utils/.*`` line to ``.airflowignore`` file (if using the regexp ignore
syntax), so that the whole folder is ignored by the scheduler when it looks for DAGs.


Dynamic DAGs with external configuration from a structured data file
....................................................................

If you need to use a more complex meta-data to prepare your DAG structure and you would prefer to keep the
data in a structured non-python format, you should export the data to the DAG folder in a file and push
it to the DAG folder, rather than try to pull the data by the DAG's top-level code - for the reasons
explained in the parent :ref:`best_practices/top_level_code`.

The meta-data should be exported and stored together with the DAGs in a convenient file format (JSON, YAML
formats are good candidates) in DAG folder. Ideally, the meta-data should be published in the same
package/folder as the module of the DAG file you load it from, because then you can find location of
the meta-data file in your DAG easily. The location of the file to read can be found using the
``__file__`` attribute of the module containing the DAG:

.. code-block:: python

    my_dir = os.path.dirname(os.path.abspath(__file__))
    configuration_file_path = os.path.join(my_dir, "config.yaml")
    with open(configuration_file_path) as yaml_file:
        configuration = yaml.safe_load(yaml_file)
    # Configuration dict is available here


Registering dynamic DAGs
........................

You can dynamically generate DAGs when using the ``@dag`` decorator or the ``with DAG(..)`` context manager
and Airflow will automatically register them.

.. code-block:: python

    from datetime import datetime
    from airflow.decorators import dag, task

    configs = {
        "config1": {"message": "first DAG will receive this message"},
        "config2": {"message": "second DAG will receive this message"},
    }

    for config_name, config in configs.items():
        dag_id = f"dynamic_generated_dag_{config_name}"

        @dag(dag_id=dag_id, start_date=datetime(2022, 2, 1))
        def dynamic_generated_dag():
            @task
            def print_message(message):
                print(message)

            print_message(config["message"])

        dynamic_generated_dag()

The code below will generate a DAG for each config: ``dynamic_generated_dag_config1`` and ``dynamic_generated_dag_config2``.
Each of them can run separately with related configuration.

If you do not wish to have DAGs auto-registered, you can disable the behavior by setting ``auto_register=False`` on your DAG.

.. versionchanged:: 2.4

    As of version 2.4 DAGs that are created by calling a ``@dag`` decorated function (or that are used in the
    ``with DAG(...)`` context manager are automatically registered, and no longer need to be stored in a
    global variable.


Optimizing DAG parsing delays during execution
----------------------------------------------

.. versionadded:: 2.4

|experimental|

Sometimes when you generate a lot of Dynamic DAGs from a single DAG file, it might cause unnecessary delays
when the DAG file is parsed during task execution. The impact is a delay before a task starts.

Why is this happening? You might not be aware but just before your task is executed,
Airflow parses the Python file the DAG comes from.

The Airflow Scheduler (or rather DAG File Processor) requires loading of a complete DAG file to process
all metadata. However, task execution requires only a single DAG object to execute a task. Knowing this,
we can skip the generation of unnecessary DAG objects when a task is executed, shortening the parsing time.
This optimization is most effective when the number of generated DAGs is high.

There is an experimental approach that you can take to optimize this behaviour. Note that it is not always
possible to use (for example when generation of subsequent DAGs depends on the previous DAGs) or when
there are some side-effects of your DAGs generation. Also the code snippet below is pretty complex and while
we tested it and it works in most circumstances, there might be cases where detection of the currently
parsed DAG will fail and it will revert to creating all the DAGs or fail. Use this solution with care and
test it thoroughly.

A nice example of performance improvements you can gain is shown in the
`Airflow's Magic Loop <https://medium.com/apache-airflow/airflows-magic-loop-ec424b05b629>`_ blog post
that describes how parsing during task execution was reduced from 120 seconds to 200 ms. (The example was
written before Airflow 2.4 so it uses undocumented behaviour of Airflow.)

In Airflow 2.4 instead you can use :py:meth:`~airflow.utils.dag_parsing_context.get_parsing_context` method
to retrieve the current context in documented and predictable way.

Upon iterating over the collection of things to generate DAGs for, you can use the context to determine
whether you need to generate all DAG objects (when parsing in the DAG File processor), or to generate only
a single DAG object (when executing the task).

The :py:meth:`~airflow.utils.dag_parsing_context.get_parsing_context` return the current parsing
context. The context is of :py:class:`~airflow.utils.dag_parsing_context.AirflowParsingContext` and
in case only single DAG/task is needed, it contains ``dag_id`` and ``task_id`` fields set.
In case "full" parsing is needed (for example in DAG File Processor), ``dag_id`` and ``task_id``
of the context are set to ``None``.


.. code-block:: python
  :emphasize-lines: 4,8,9

  from airflow.models.dag import DAG
  from airflow.utils.dag_parsing_context import get_parsing_context

  current_dag_id = get_parsing_context().dag_id

  for thing in list_of_things:
      dag_id = f"generated_dag_{thing}"
      if current_dag_id is not None and current_dag_id != dag_id:
          continue  # skip generation of non-selected DAG

      with DAG(dag_id=dag_id, ...):
          ...

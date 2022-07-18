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

To have a task repeated based on the output/result of a previous task see :doc:`/concepts/dynamic-task-mapping`.

Dynamic DAGs with environment variables
.......................................

If you want to use variables to configure your code, you should always use
`environment variables <https://wiki.archlinux.org/title/environment_variables>`_ in your
top-level code rather than :doc:`Airflow Variables </concepts/variables>`. Using Airflow Variables
at top-level code creates a connection to metadata DB of Airflow to fetch the value, which can slow
down parsing and place extra load on the DB. See the `Airflow Variables <_best_practices/airflow_variables>`_
on how to make best use of Airflow Variables in your DAGs using Jinja templates .

For example you could set ``DEPLOYMENT`` variable differently for your production and development
environments. The variable ``DEPLOYMENT`` could be set to ``PROD`` in your production environment and to
``DEV`` in your development environment. Then you could build your dag differently in production and
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
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
    ) as dag:
        for task in ALL_TASKS:
            # create your operators and relations here
            pass

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


Dynamic DAGs with ``globals()``
...............................
You can dynamically generate DAGs by working with ``globals()``.
As long as a ``DAG`` object in ``globals()`` is created, Airflow will load it.

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

        globals()[dag_id] = dynamic_generated_dag()

The code below will generate a DAG for each config: ``dynamic_generated_dag_config1`` and ``dynamic_generated_dag_config2``.
Each of them can run separately with related configuration

.. warning::
  Using this practice, pay attention to "late binding" behaviour in Python loops. See `that GitHub discussion <https://github.com/apache/airflow/discussions/21278#discussioncomment-2103559>`_ for more details


Optimizing DAG parsing delays during execution
----------------------------------------------

Sometimes when you generate a lot of Dynamic DAGs from a single DAG file, it might cause unnecessary delays
when the DAG file is parsed during task execution. The impact is a delay before a task starts.

Why is this happening? You might not be aware but just before your task is executed,
Airflow parses the Python file the DAG comes from.

The Airflow Scheduler (or DAG Processor) requires loading of a complete DAG file to process all metadata.
However, task execution requires only a single DAG object to execute a task. Knowing this, we can
skip the generation of unnecessary DAG objects when a task is executed, shortening the parsing time.
This optimization is most effective when the number of generated DAGs is high.

There is an experimental approach that you can take to optimize this behaviour. Note that it is not always
possible to use (for example when generation of subsequent DAGs depends on the previous DAGs) or when
there are some side-effects of your DAGs generation. Also the code snippet below is pretty complex and while
we tested it and it works in most circumstances, there might be cases where detection of the currently
parsed DAG will fail and it will revert to creating all the DAGs or fail. Use this solution with care and
test it thoroughly.

Upon evaluation of a DAG file, command line arguments are supplied which we can use to determine which
Airflow component performs parsing:

* Scheduler/DAG Processor args: ``["airflow", "scheduler"]`` or ``["airflow", "dag-processor"]``
* Task execution args: ``["airflow", "tasks", "run", "dag_id", "task_id", ...]``

However, depending on the executor used and forking model, those args might be available via ``sys.args``
or via name of the process running. Airflow either executes tasks via running a new Python interpreter or
sets the name of the process as "airflow task supervisor: {ARGS}" in case of celery forked process or
"airflow task runner: dag_id task_id" in case of local executor forked process.

Upon iterating over the collection of things to generate DAGs for, you can use these arguments to determine
whether you need to generate all DAG objects (when parsing in the DAG File processor), or to generate only
a single DAG object (when executing the task):

.. code-block:: python
  :emphasize-lines: 7,8,9,19,20,24,25,31,32

  import sys
  import ast
  import setproctitle
  from airflow.models import DAG

  current_dag = None
  if len(sys.argv) > 3 and sys.argv[1] == "tasks":
      # task executed by starting a new Python interpreter
      current_dag = sys.argv[3]
  else:
      try:
          PROCTITLE_SUPERVISOR_PREFIX = "airflow task supervisor: "
          PROCTITLE_TASK_RUNNER_PREFIX = "airflow task runner: "
          proctitle = str(setproctitle.getproctitle())
          if proctitle.startswith(PROCTITLE_SUPERVISOR_PREFIX):
              # task executed via forked process in celery
              args_string = proctitle[len(PROCTITLE_SUPERVISOR_PREFIX) :]
              args = ast.literal_eval(args_string)
              if len(args) > 3 and args[1] == "tasks":
                  current_dag = args[3]
          elif proctitle.startswith(PROCTITLE_TASK_RUNNER_PREFIX):
              # task executed via forked process in standard_task_runner
              args = proctitle[len(PROCTITLE_TASK_RUNNER_PREFIX) :].split(" ")
              if len(args) > 0:
                  current_dag = args[0]
      except Exception:
          pass

  for thing in list_of_things:
      dag_id = f"generated_dag_{thing}"
      if current_dag is not None and current_dag != dag_id:
          continue  # skip generation of non-selected DAG

      dag = DAG(dag_id=dag_id, ...)
      globals()[dag_id] = dag

This optimization applies to ``airflow tasks run`` and ``airflow tasks test`` commands.

A nice example of performance improvements you can gain is shown in the
`Airflow's Magic Loop <https://medium.com/apache-airflow/airflows-magic-loop-ec424b05b629>`_ blog post
that describes how parsing during task execution was reduced from 120 seconds to 200 ms.

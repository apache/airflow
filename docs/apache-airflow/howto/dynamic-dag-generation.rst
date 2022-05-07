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

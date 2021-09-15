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

.. _best_practice:

Best Practices
==============

Creating a new DAG is a two-step process:

- writing Python code to create a DAG object,
- testing if the code meets our expectations

This tutorial will introduce you to the best practices for these two steps.

.. _best_practice:writing_a_dag:

Writing a DAG
^^^^^^^^^^^^^^

Creating a new DAG in Airflow is quite simple. However, there are many things that you need to take care of
to ensure the DAG run or failure does not produce unexpected results.

Creating a Custom Operator/Hook
-------------------------------

Please follow our guide on :ref:`custom Operators <custom_operator>`.

Creating a task
---------------

You should treat tasks in Airflow equivalent to transactions in a database. This
implies that you should never produce incomplete results from your tasks. An
example is not to produce incomplete data in ``HDFS`` or ``S3`` at the end of a
task.

Airflow can retry a task if it fails. Thus, the tasks should produce the same
outcome on every re-run. Some of the ways you can avoid producing a different
result -

* Do not use INSERT during a task re-run, an INSERT statement might lead to
  duplicate rows in your database. Replace it with UPSERT.
* Read and write in a specific partition. Never read the latest available data
  in a task. Someone may update the input data between re-runs, which results in
  different outputs. A better way is to read the input data from a specific
  partition. You can use ``data_interval_start`` as a partition. You should
  follow this partitioning method while writing data in S3/HDFS as well.
* The Python datetime ``now()`` function gives the current datetime object. This
  function should never be used inside a task, especially to do the critical
  computation, as it leads to different outcomes on each run. It's fine to use
  it, for example, to generate a temporary log.

.. tip::

    You should define repetitive parameters such as ``connection_id`` or S3 paths in ``default_args`` rather than declaring them for each task.
    The ``default_args`` help to avoid mistakes such as typographical errors. Also, most connection types have unique parameter names in
    tasks, so you can declare a connection only once in ``default_args`` (for example ``gcp_conn_id``) and it is automatically
    used by all operators that use this connection type.

Deleting a task
----------------

Never delete a task from a DAG. In case of deletion, the historical information of the task disappears from the Airflow UI.
It is advised to create a new DAG in case the tasks need to be deleted.


Communication
--------------

Airflow executes tasks of a DAG on different servers in case you are using :doc:`Kubernetes executor <../executor/kubernetes>` or :doc:`Celery executor <../executor/celery>`.
Therefore, you should not store any file or config in the local filesystem as the next task is likely to run on a different server without access to it — for example, a task that downloads the data file that the next task processes.
In the case of :class:`Local executor <airflow.executors.local_executor.LocalExecutor>`,
storing a file on disk can make retries harder e.g., your task requires a config file that is deleted by another task in DAG.

If possible, use ``XCom`` to communicate small messages between tasks and a good way of passing larger data between tasks is to use a remote storage such as S3/HDFS.
For example, if we have a task that stores processed data in S3 that task can push the S3 path for the output data in ``Xcom``,
and the downstream tasks can pull the path from XCom and use it to read the data.

The tasks should also not store any authentication parameters such as passwords or token inside them.
Where at all possible, use :doc:`Connections </concepts/connections>` to store data securely in Airflow backend and retrieve them using a unique connection id.

Top level Python Code and Dynamic DAGs
--------------------------------------

You should avoid writing the top level code which is not necessary to create Operators
and build DAG relations between them. This is because of the design decision for the scheduler of Airflow
and the impact the top-level code parsing speed on both performance and scalability of Airflow.

Airflow scheduler executes the code outside the Operator's ``execute`` methods with the minimum interval of
:ref:`min_file_process_interval<config:scheduler__min_file_process_interval>` seconds. This is done in order
to allow dynamic scheduling of the DAGs - where scheduling and dependencies might change over time and
impact the next schedule of the DAG. Airflow scheduler tries to continuously make sure that what you have
in DAGs is correctly reflected in scheduled tasks.

Specifically you should not run any database access, heavy computations and networking operations.

This limitation is especially important in case of dynamic DAG configuration, which can be configured
essentially in one of those ways:

* via `environment variables <https://wiki.archlinux.org/title/environment_variables>`_ (not to be mistaken
  with the :doc:`Airflow Variables </concepts/variables>`)
* via externally provided, generated Python code, containing meta-data in the DAG folder
* via externally provided, generated configuration meta-data file in the DAG folder

All cases are described in the following chapters.

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

    with DAG(dag_id="my_dag", schedule_interval=None, start_date=days_ago(2)) as dag:
        for task in ALL_TASKS:
            # create your operators and relations here
            pass

Don't forget that in this case you need to add empty ``__init__.py`` file in the ``my_company_utils`` folder
and you should add the ``my_company_utils/.*`` line to ``.airflowignore`` file, so that the whole folder is
ignored by the scheduler when it looks for DAGs.


Dynamic DAGs with external configuration from a structured data file
....................................................................

If you need to use a more complex meta-data to prepare your DAG structure and you would prefer to keep the
data in a structured non-python format, you should export the data to the DAG folder in a file and push
it to the DAG folder, rather than try to pull the data by the DAG's top-level code - for the reasons
explained in the parent `Top level Python code <_top-level-python-code>`_.

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


.. _best_practices/airflow_variables:

Airflow Variables
-----------------

As mentioned in the previous chapter, `Top level Python code <_top-level-python-code>`_. you should avoid
using Airflow Variables at top level Python code of DAGs. You can use the Airflow Variables freely inside the
``execute()`` methods of the operators, but you can also pass the Airflow Variables to the existing operators
via Jinja template, which will delay reading the value until the task execution.

The template syntax to do this is:

.. code-block::

    {{ var.value.<variable_name> }}

or if you need to deserialize a json object from the variable :

.. code-block::

    {{ var.json.<variable_name> }}

For security purpose, you're recommended to use the :ref:`Secrets Backend<secrets_backend_configuration>`
for any variable that contains sensitive data.

Triggering DAGs after changes
-----------------------------

Avoid triggering DAGs immediately after changing them or any other accompanying files that you change in the
DAG folder.

You should give the system sufficient time to process the changed files. This takes several steps.
First the files have to be distributed to scheduler - usually via distributed filesystem or Git-Sync, then
scheduler has to parse the Python files and store them in the database. Depending on your configuration,
speed of your distributed filesystem, number of files, number of DAGs, number of changes in the files,
sizes of the files, number of schedulers, speed of CPUS, this can take from seconds to minutes, in extreme
cases many minutes. You should wait for your DAG to appear in the UI to be able to trigger it.

In case you see long delays between updating it and the time it is ready to be triggered, you can look
at the following configuration parameters and fine tune them according your needs (see details of
each parameter by following the links):

* :ref:`config:scheduler__processor_poll_interval`
* :ref:`config:scheduler__min_file_process_interval`
* :ref:`config:scheduler__dag_dir_list_interval`
* :ref:`config:scheduler__parsing_processes`
* :ref:`config:scheduler__file_parsing_sort_mode`

Testing a DAG
^^^^^^^^^^^^^

Airflow users should treat DAGs as production level code, and DAGs should have various associated tests to ensure that they produce expected results.
You can write a wide variety of tests for a DAG. Let's take a look at some of them.

DAG Loader Test
---------------

This test should ensure that your DAG does not contain a piece of code that raises error while loading.
No additional code needs to be written by the user to run this test.

.. code-block:: bash

 python your-dag-file.py

Running the above command without any error ensures your DAG does not contain any uninstalled dependency, syntax errors, etc.

You can look into :ref:`Testing a DAG <testing>` for details on how to test individual operators.

Unit tests
-----------

Unit tests ensure that there is no incorrect code in your DAG. You can write unit tests for both your tasks and your DAG.

**Unit test for loading a DAG:**

.. code-block:: python

    import pytest

    from airflow.models import DagBag


    @pytest.fixture(scope="scope")
    def dagbag(self):
        return DagBag()


    def test_dag_loaded(self, dagbag):
        dag = dagbag.get_dag(dag_id="hello_world")
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 1


**Unit test a DAG structure:**
This is an example test want to verify the structure of a code-generated DAG against a dict object

.. code-block:: python

      def assert_dag_dict_equal(source, dag):
          assert dag.task_dict.keys() == source.keys()
          for task_id, downstream_list in source.items():
              assert dag.has_task(task_id)
              task = dag.get_task(task_id)
              assert task.downstream_task_ids == set(downstream_list)


      def test_dag():
          assert_dag_dict_equal(
              {
                  "DummyInstruction_0": ["DummyInstruction_1"],
                  "DummyInstruction_1": ["DummyInstruction_2"],
                  "DummyInstruction_2": ["DummyInstruction_3"],
                  "DummyInstruction_3": [],
              },
              dag,
          )


**Unit test for custom operator:**

.. code-block:: python

    import pytest

    from airflow.utils.state import State


    def test_my_custom_oeprator_execute_no_trigger(dag_maker):
        with dag_maker(
            dag_id="test_my_custom_oeprator_execute_no_trigger",
            schedule_interval="@daily",
            default_args={"start_date": DEFAULT_DATE},
        ) as dag:
            MyCustomOperator(
                dag=self.dag,
                task_id="test",
                prefix="s3://bucket/some/prefix",
            )
        dagrun = dag_maker.create_dagrun()
        (ti,) = dagrun.task_instances
        assert ti.state == State.SUCCESS
        # Assert something related to tasks results


Self-Checks
------------

You can also implement checks in a DAG to make sure the tasks are producing the results as expected.
As an example, if you have a task that pushes data to S3, you can implement a check in the next task. For example, the check could
make sure that the partition is created in S3 and perform some simple checks to determine if the data is correct.


Similarly, if you have a task that starts a microservice in Kubernetes or Mesos, you should check if the service has started or not using :class:`airflow.providers.http.sensors.http.HttpSensor`.

.. code-block:: python

   task = PushToS3(...)
   check = S3KeySensor(
       task_id="check_parquet_exists",
       bucket_key="s3://bucket/key/foo.parquet",
       poke_interval=0,
       timeout=0,
   )
   task >> check



Staging environment
--------------------

If possible, keep a staging environment to test the complete DAG run before deploying in the production.
Make sure your DAG is parameterized to change the variables, e.g., the output path of S3 operation or the database used to read the configuration.
Do not hard code values inside the DAG and then change them manually according to the environment.

You can use environment variables to parameterize the DAG.

.. code-block:: python

   import os

   dest = os.environ.get("MY_DAG_DEST_PATH", "s3://default-target/path/")

Mocking variables and connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When you write tests for code that uses variables or a connection, you must ensure that they exist when you run the tests. The obvious solution is to save these objects to the database so they can be read while your code is executing. However, reading and writing objects to the database are burdened with additional time overhead. In order to speed up the test execution, it is worth simulating the existence of these objects without saving them to the database. For this, you can create environment variables with mocking :any:`os.environ` using :meth:`unittest.mock.patch.dict`.

For variable, use :envvar:`AIRFLOW_VAR_{KEY}`.

.. code-block:: python

    with mock.patch.dict("os.environ", AIRFLOW_VAR_KEY="env-value"):
        assert "env-value" == Variable.get("key")

For connection, use :envvar:`AIRFLOW_CONN_{CONN_ID}`.

.. code-block:: python

    conn = Connection(
        conn_type="gcpssh",
        login="cat",
        host="conn-host",
    )
    conn_uri = conn.get_uri()
    with mock.patch.dict("os.environ", AIRFLOW_CONN_MY_CONN=conn_uri):
        assert "cat" == Connection.get("my_conn").login

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

Best Practices
==============

Running Airflow in production is seamless. It comes bundled with all the plugins and configs
necessary to run most of the DAGs. However, you can come across certain pitfalls, which can cause occasional errors.
Let's take a look at what you need to do at various stages to avoid these pitfalls, starting from writing the DAG
to the actual deployment in the production environment.


Writing a DAG
^^^^^^^^^^^^^^
Creating a new DAG in Airflow is quite simple. However, there are many things that you need to take care of
to ensure the DAG run or failure does not produce unexpected results.

Creating a task
---------------

You should treat tasks in Airflow equivalent to transactions in a database. It implies that you should never produce
incomplete results from your tasks. An example is not to produce incomplete data in ``HDFS`` or ``S3`` at the end of a task.

Airflow can retry a task if it fails. Thus, the tasks should produce the same outcome on every re-run.
Some of the ways you can avoid producing a different result -

* Do not use INSERT during a task re-run, an INSERT statement might lead to duplicate rows in your database.
  Replace it with UPSERT.
* Read and write in a specific partition. Never read the latest available data in a task.
  Someone may update the input data between re-runs, which results in different outputs.
  A better way is to read the input data from a specific partition. You can use ``execution_date`` as a partition.
  You should follow this partitioning method while writing data in S3/HDFS, as well.
* The python datetime ``now()`` function gives the current datetime object.
  This function should never be used inside a task, especially to do the critical computation, as it leads to different outcomes on each run.
  It's fine to use it, for example, to generate a temporary log.

.. tip::

    You should define repetitive parameters such as ``connection_id`` or S3 paths in ``default_args`` rather than declaring them for each task.
    The ``default_args`` help to avoid mistakes such as typographical errors.


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
Where at all possible, use :ref:`Connections <concepts-connections>` to store data securely in Airflow backend and retrieve them using a unique connection id.


Variables
---------

You should avoid usage of Variables outside an operator's ``execute()`` method or Jinja templates if possible,
as Variables create a connection to metadata DB of Airflow to fetch the value, which can slow down parsing and place extra load on the DB.

Airflow parses all the DAGs in the background at a specific period.
The default period is set using ``processor_poll_interval`` config, which is by default 1 second. During parsing, Airflow creates a new connection to the metadata DB for each DAG.
It can result in a lot of open connections.

The best way of using variables is via a Jinja template which will delay reading the value until the task execution. The template synaxt to do this is:

.. code::

    {{ var.value.<variable_name> }}

or if you need to deserialize a json object from the variable :

.. code::

    {{ var.json.<variable_name> }}


.. note::

    In general, you should not write any code outside the tasks. The code outside the tasks runs every time Airflow parses the DAG, which happens every second by default.


Testing a DAG
^^^^^^^^^^^^^

Airflow users should treat DAGs as production level code. The DAGs should have various tests to ensure that it produces expected results.
You can write a wide variety of tests for a DAG. Let's take a look at some of them.

DAG Loader Test
---------------

This test should ensure that your DAG does not contain a piece of code that raises error while loading.
No additional code needs to be written by the user to run this test.

.. code::

 python your-dag-file.py

Running the above command without any error ensures your DAG does not contain any uninstalled dependency, syntax errors, etc.

You can look into :ref:`Testing a DAG <testing>` for details on how to test individual operators.

Unit tests
-----------

Unit tests ensure that there is no incorrect code in your DAG. You can write a unit test for your tasks as well as your DAG.

**Unit test for loading a DAG:**

.. code::

 from airflow.models import DagBag
 import unittest

 class TestHelloWorldDAG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id='hello_world')
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 1)

**Unit test a DAG structure:**
This is an example test want to verify the structure of a code-generated DAG against a dict object

.. code::

 import unittest
 class testClass(unittest.TestCase):
     def assertDagDictEqual(self,source,dag):
         self.assertEqual(dag.task_dict.keys(),source.keys())
         for task_id,downstream_list in source.items():
             self.assertTrue(dag.has_task(task_id), msg="Missing task_id: {} in dag".format(task_id))
             task = dag.get_task(task_id)
             self.assertEqual(task.downstream_task_ids, set(downstream_list),
                              msg="unexpected downstream link in {}".format(task_id))
     def test_dag(self):
         self.assertDagDictEqual({
           "DummyInstruction_0": ["DummyInstruction_1"],
           "DummyInstruction_1": ["DummyInstruction_2"],
           "DummyInstruction_2": ["DummyInstruction_3"],
           "DummyInstruction_3": []
         },dag)

**Unit test for custom operator:**

.. code::

 import unittest
 from airflow.utils.state import State

 DEFAULT_DATE = '2019-10-03'
 TEST_DAG_ID = 'test_my_custom_operator'

 class MyCustomOperatorTest(unittest.TestCase):
    def setUp(self):
        self.dag = DAG(TEST_DAG_ID, schedule_interval='@daily', default_args={'start_date' : DEFAULT_DATE})
        self.op = MyCustomOperator(
            dag=self.dag,
            task_id='test',
            prefix='s3://bucket/some/prefix',
        )
        self.ti = TaskInstance(task=self.op, execution_date=DEFAULT_DATE)

    def test_execute_no_trigger(self):
        self.ti.run(ignore_ti_state=True)
        self.assertEqual(self.ti.state, State.SUCCESS)
        #Assert something related to tasks results

Self-Checks
------------

You can also implement checks in a DAG to make sure the tasks are producing the results as expected.
As an example, if you have a task that pushes data to S3, you can implement a check in the next task. For example, the check could
make sure that the partition is created in S3 and perform some simple checks to see if the data is correct or not.


Similarly, if you have a task that starts a microservice in Kubernetes or Mesos, you should check if the service has started or not using :class:`airflow.providers.http.sensors.http.HttpSensor`.

.. code::

 task = PushToS3(...)
 check = S3KeySensor(
    task_id='check_parquet_exists',
    bucket_key="s3://bucket/key/foo.parquet",
    poke_interval=0,
    timeout=0
 )
 task >> check



Staging environment
--------------------

If possible, keep a staging environment to test the complete DAG run before deploying in the production.
Make sure your DAG is parameterized to change the variables, e.g., the output path of S3 operation or the database used to read the configuration.
Do not hard code values inside the DAG and then change them manually according to the environment.

You can use environment variables  to parameterize the DAG.

.. code::

 import os

 dest = os.environ.get(
    "MY_DAG_DEST_PATH",
    "s3://default-target/path/"
 )

Deployment in Production
^^^^^^^^^^^^^^^^^^^^^^^^^
Once you have completed all the mentioned checks, it is time to deploy your DAG in production.
To do this, first, you need to make sure that the Airflow is itself production-ready.
Let's see what precautions you need to take.


Database backend
----------------

Airflow comes with an ``SQLite`` backend by default. It allows the user to run Airflow without any external database.
However, such a setup is meant to be for testing purposes only. Running the default setup can lead to data loss in multiple scenarios.
If you want to run Airflow in production, make sure you :doc:`configure the backend <howto/initialize-database>` to be an external database such as PostgreSQL or MySQL.

You can change the backend using the following config

.. code:: ini

 [core]
 sql_alchemy_conn = my_conn_string

Once you have changed the backend, airflow needs to create all the tables required for operation.
Create an empty DB and give airflow's user the permission to ``CREATE/ALTER`` it.
Once that is done, you can run -

.. code::

 airflow db upgrade

``upgrade`` keeps track of migrations already applies, so it's safe to run as often as you need.

.. note::

 Do not use ``airflow db init`` as it can create a lot of default connection, charts, etc. which are not required in production DB.


Multi-Node Cluster
-------------------

Airflow uses :class:`airflow.executors.sequential_executor.SequentialExecutor` by default. However, by its nature, the user is limited to executing at most
one task at a time. ``Sequential Executor`` also pauses the scheduler when it runs a task, hence not recommended in a production setup.
You should use :class:`Local executor <airflow.executors.local_executor.LocalExecutor>` for a single machine.
For multi-node setup, you should use :doc:`Kubernetes executor <../executor/kubernetes>` or :doc:`Celery executor <../executor/celery>`.


Once you have configured the executor, it is necessary to make sure that every node in the cluster contains the same configuration and dags.
Airflow only sends simple instructions such as "execute task X of dag Y" but does not send any dag files or configuration. You can use a simple cronjob or
any other mechanism to sync DAGs and configs across your nodes, e.g., checkout DAGs from git repo every 5 minutes on all nodes.


Logging
--------

If you are using disposable nodes in your cluster, configure the log storage to be a distributed file system (DFS) such as ``S3`` and ``GCS``, or external services such as
Stackdriver Logging, Elasticsearch or Amazon CloudWatch.
This way, the logs are available even after the node goes down or gets replaced. See :doc:`howto/write-logs` for configurations.

.. note::

    The logs only appear in your DFS after the task has finished. You can view the logs while the task is running in UI itself.


Configuration
--------------

Airflow comes bundles with a default ``airflow.cfg`` configuration file.
You should use environment variables for configurations that change across deployments
e.g. metadata DB, password. You can do it using the format ``$AIRFLOW__{SECTION}__{KEY}``

.. code::

 AIRFLOW__CORE__SQL_ALCHEMY_CONN=my_conn_id
 AIRFLOW__WEBSERVER__BASE_URL=http://host:port

Some configurations such as Airflow Backend connection URI can be derived from bash commands as well:

.. code::

 sql_alchemy_conn_cmd = bash_command_to_run


Scheduler Uptime
-----------------

Airflow users have for a long time been affected by a
`core Airflow bug <https://issues.apache.org/jira/browse/AIRFLOW-401>`_
that causes the scheduler to hang without a trace.

Until fully resolved, you can mitigate a few ways:

* Set a reasonable run_duration setting in your ``airflow.cfg``. `Example config <https://github.com/astronomer/airflow-chart/blob/63bc503c67e2cd599df0b6f831d470d09bad7ee7/templates/configmap.yaml#L44>`_.
* Add an ``exec`` style health check to your helm charts on the scheduler deployment to fail if the scheduler has not heartbeat in a while. `Example health check definition <https://github.com/astronomer/helm.astronomer.io/pull/200/files>`_.

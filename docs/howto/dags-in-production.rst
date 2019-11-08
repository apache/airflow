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

Getting a DAG ready for production
==================================

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

Airflow retries a task if it fails. Thus, the tasks should produce the same outcome on every re-run.
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


Deleting a task
----------------

Never delete a task from a DAG. In case of deletion, the historical information of the task disappears from the Airflow UI. 
It is advised to create a new DAG in case the tasks need to be deleted.


Communication
--------------

Airflow executes tasks of a DAG in different directories, which can even be present 
on different servers in case you are using :doc:`Kubernetes executor <../executor/kubernetes>` or :doc:`Celery executor <../executor/celery>`. 
Therefore, you should not store any file or config in the local filesystem — for example, a task that downloads the JAR file that the next task executes.

Always use XCom to communicate small messages between tasks or S3/HDFS to communicate large messages/files.

The tasks should also not store any authentication parameters such as passwords or token inside them. 
Always use :ref:`Connections <concepts-connections>` to store data securely in Airflow backend and retrieve them using a unique connection id.


.. note::

    Do not write any critical code outside the tasks. The code outside the tasks runs every time airflow parses the DAG, which happens every second by default.

    You should also avoid repeating arguments such as connection_id or S3 paths using default_args. It helps you to avoid mistakes while passing arguments.



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
    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id='hello_world')
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 1)

**Unit test for custom operator:**

.. code::

 import unittest
 from airflow.utils.state import State

 class MyCustomOperatorTest(unittest.TestCase):
    def setUp(self):
        self.dag = DAG(TEST_DAG_ID, schedule_interval='@daily', default_args={'start_date' : DEFAULT_DATE})
        self.op = MyCustomOperator(
            dag = self.dag,
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
As an example, if you have a task that pushed data to S3, you can implement a check in the next task. The check should 
make sure that the partition is created in S3 and check if the data is correct or not.

Similarly, if you have a task that starts a microservice in Kubernetes or Mesos, you should check if the service has started or not using :class:`airflow.sensors.http_sensor.HttpSensor`.

.. code::

 task = PushToS3(...)
 check = S3KeySensor(
    bucket_key="s3://bucket/key/foo.parquet"
 )
 task.set_downstream(check)



Staging environment
--------------------

Always keep a staging environment to test the complete DAG run before deploying in the production.
Make sure your DAG is parameterized to change the variables, e.g., the output path of S3 operation or the database used to read the configuration.
Do not hard code values inside the DAG and then change them manually according to the environment.

You can use Airflow Variables to parameterize the DAG.

.. code::

 dest = Variable(
    "my_dag_dest",
    "s3://default-target/path/"
 )

Deployment in Production
^^^^^^^^^^^^^^^^^^^^^^^^^
Once you have completed all the mentioned checks, it is time to deploy your DAG in production.
To do this, first, you need to make sure that the Airflow is itself production-ready. 
Let's see what precautions you need to take.


Backend
--------

Airflow comes with an ``SQLite`` backend by default. It allows the user to run Airflow without any external database.
However, such a setup is meant to be for testing purposes only. Running the default setup can lead to data loss in multiple scenarios. 
If you want to run Airflow in production, make sure you :doc:`configure the backend <initialize-database>` to be an external database such as ``MySQL`` or ``Postgres``. 

You can change the backend using the following config-

.. code::

 [core]
 sql_alchemy_conn = my_conn_string

Once you have changed the backend, airflow needs to create all the tables required for operation.
Create an empty DB and give airflow's user the permission to ``CREATE/ALTER`` it.
Once that is done, you can run -

.. code::

 airflow upgradedb

``upgradedb`` keeps track of migrations already applies, so it's safe to run as often as you need.

.. note::
 
 Do not use ``airflow initdb`` as it can create a lot of default connection, charts, etc. which are not required in production DB.


Multi-Node Cluster
-------------------

Airflow uses :class:`airflow.executors.sequential_executor.SequentialExecutor` by default. It works fine in most cases. However, by its nature, the user is limited to executing at most
one task at a time. It's also not suitable to work in a multi-node cluster. You should use :doc:`../executor/celery` or :doc:`../executor/kubernetes` in such cases.


Once you have configured the executor, it is necessary to make sure that every node in the cluster contains the same configuration and dags.
Airflow only sends simple instructions such as execute task X on node Y but does not send any dag files or configuration. You can use a simple CRON or
any other mechanism to sync DAGs and configs across your nodes, e.g., checkout DAGs from git repo every 5 minutes on all nodes.


Logging
--------

If you are using disposable nodes in your cluster, configure the log storage to be a distributed file system such as ``S3`` or ``GCS``.
A DFS makes these logs are available even after the node goes down or gets replaced. See :doc:`write-logs` for configurations.

.. note::

    The logs only appear in DFS after the task has finished. You can view the logs while the task is running in UI itself.


Configuration
--------------

Airflow comes bundles with a default airflow.cfg configuration file.
You should environment variables for configurations that change across deployments
e.g. metadata DB, password. You can do it using the format ``$AIRFLOW__{SECTION}__{KEY}``

.. code::

 AIRFLOW__CORE__SQL_ALCHEMY_CONN=my_conn_id
 AIRFLOW__WEBSERVER__BASE_URL=http://host:port

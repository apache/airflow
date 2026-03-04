
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

Sail Operators
==============

.. _howto/operator:PySailOperator:

PySailOperator
-------------------

Use the :class:`~airflow.providers.sail.operators.sail.PySailOperator` to
execute PySpark code on a Sail engine.

Sail is a Rust-native computation engine that implements the Spark Connect protocol,
allowing PySpark code to run without the JVM.

The operator supports two modes:

- **Remote mode**: connects to an existing Sail server via ``sc://`` protocol.
- **Local embedded mode**: starts a local Sail server, runs the job, and stops it.

Usage example (remote)
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.providers.sail.operators.sail import PySailOperator


    def my_sail_job(spark):
        df = spark.read.parquet("/data/input")
        result = df.filter(df.age > 18)
        result.write.parquet("/data/output")


    task = PySailOperator(
        task_id="sail_remote_job",
        python_callable=my_sail_job,
        conn_id="my_sail_connection",
    )

Usage example (local embedded)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.providers.sail.operators.sail import PySailOperator


    def my_sail_job(spark):
        df = spark.range(100)
        return df.count()


    task = PySailOperator(
        task_id="sail_local_job",
        python_callable=my_sail_job,
    )

Task Decorator
^^^^^^^^^^^^^^

You can also use the ``@task.pysail`` decorator:

.. code-block:: python

    from airflow.decorators import task


    @task.pysail(conn_id="my_sail_connection")
    def my_task(spark):
        return spark.range(10).count()

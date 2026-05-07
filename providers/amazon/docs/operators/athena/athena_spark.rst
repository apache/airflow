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

Athena Spark Operators
======================

Amazon Athena supports Apache Spark calculations through session-based APIs.
This page documents the provider support for submitting and monitoring those
calculations from Airflow.

AthenaSparkOperator
-------------------

Use :class:`~airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator`
to submit Spark code to an existing Athena session and wait until the calculation
reaches a terminal state.

.. code-block:: python

    from airflow import DAG
    from airflow.providers.amazon.aws.operators.athena_spark import AthenaSparkOperator
    from datetime import datetime

    with DAG(
        dag_id="example_athena_spark_operator",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
    ) as dag:
        AthenaSparkOperator(
            task_id="run_spark_code",
            session_id="my-athena-session-id",
            code_block="print('hello from athena spark')",
            poll_interval=10,
            max_polling_attempts=60,
        )

AthenaSparkSensor
-----------------

Use :class:`~airflow.providers.amazon.aws.sensors.athena_spark.AthenaSparkSensor`
to wait for an existing calculation execution ID.

.. code-block:: python

    from airflow import DAG
    from airflow.providers.amazon.aws.sensors.athena_spark import AthenaSparkSensor
    from datetime import datetime

    with DAG(
        dag_id="example_athena_spark_sensor",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
    ) as dag:
        AthenaSparkSensor(
            task_id="wait_for_spark_calculation",
            calculation_execution_id="calc-exec-123",
        )

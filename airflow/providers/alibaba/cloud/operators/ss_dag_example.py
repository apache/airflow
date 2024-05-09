# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.alibaba.cloud.operators.emr import EmrServerlessSparkStartJobRunOperator

# Ignore missing args provided by default_args
# mypy: disable-error-code="call-arg"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "emr_serverless_spark_test"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 5, 1),
    default_args={},
    max_active_runs=1,
    catchup=False,
) as dag:
    spark_test = EmrServerlessSparkStartJobRunOperator(
        task_id="spark_test",
        emr_serverless_spark_conn_id="spark_test",
        region="cn-hangzhou",
        polling_interval=5,
        workspace_id="w-ae42e9c929275cc5",
        resource_queue_id="root_queue",
        code_type="JAR",
        name="airflow-dag-test",
        entry_point="oss://datadev-oss-hdfs-test/spark-resource/examples/jars/spark-examples_2.12-3.3.1.jar",
        entry_point_args="1",
        spark_submit_parameters="--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=4 --conf spark.executor.memory=20g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=1",
        is_prod=True
    )

    spark_test

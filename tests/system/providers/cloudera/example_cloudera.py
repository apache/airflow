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

"""
Example Airflow DAG for CDE run job operator.

The target of this example DAG is to run the CdeRunJobOperator integration test.
It shows basic usage and configuration of this operator.
In the test environment, this DAG is expected to be processed by a local Airflow instance.
Please note that it is expected to have:
* CDE CLI binary available on the environment (for the jobs creation steps as part of this example,
not the operator itself). It can be fetched from the CDE virtual cluster.
* CDE Airflow connection already set up. A dummy example connection can be found in airflow/utils/db.py.

Please refer to the CDE documentation for more information:
https://docs.cloudera.com/data-engineering/cloud/orchestrate-workflows/topics/cde-airflow-dag-pipeline.html

This DAG will first create a sample Spark job which will be created on CDE and then run through
the CdeRunJobOperator on the Virtual Cluster which is defined in the associated Airflow connection.
Please note that in real life situations, those preparation steps are typically not needed as CDE jobs
are already created on the CDE Virtual Cluster.
This DAG contains the following steps:

- bash_delete_job: Ensure CDE job and associated resources are deleted on the target Virtual Cluster.

- bash_create_job_file: Prepare a simple Spark job file.
    It can be another job type, not only spark.
    Please refer to: https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-create-job.html

- bash_create_spark_job: Create and upload the CDE resources and create the CDE job.
    Resources are a CDE concept and are needed to create a CDE job properly.
    Please refer to: https://docs.cloudera.com/data-engineering/cloud/overview/topics/cde-resources.html

- test_run_job: Execute the CDE job with CdeRunJobOperator.

- bash_clean_job: Delete job and associated resources on the target Virtual Cluster.

"""
from __future__ import annotations

import os

import pendulum

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cloudera.operators.cde_operator import CdeRunJobOperator

CONNECTION_ID = os.environ.get("AIRFLOW_PROVIDER_SYSTEST_CDE_CONN_ID")
DAG_ID = "cloudera_data_engineering_operator"
# If simultaneous executions of multiple airflow systest are ever needed,
# we should probably update this variable and add a suffix to the job name with a random id.
JOB_NAME = "airflow-systest-spark-job"
RESOURCE_NAME = JOB_NAME + "-res"
JOB_FILE = "spark_version.py"

# Airflow CI requirement.
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

with DAG(
    DAG_ID,
    schedule_interval="@once",
    start_date=pendulum.datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "integration test", "CDE"],
) as dag:

    bash_delete_job = BashOperator(
        task_id="bash-delete-job",
        bash_command=f"if cde job list | grep {JOB_NAME} ;then cde job delete --name {JOB_NAME} && "
        f"cde resource delete --name {RESOURCE_NAME}; fi",
        dag=dag,
    )

    bash_create_job_file = BashOperator(
        task_id="bash-create-job-file",
        dag=dag,
        bash_command=(
            f"""echo "import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType

spark = SparkSession.builder.appName('spark version').getOrCreate()
sc = spark.sparkContext

print(sc)
print(sc.version)"> /tmp/{JOB_FILE}"""
        ),
    )

    bash_create_spark_job = BashOperator(
        task_id="bash-create-spark-job",
        bash_command=(
            f"cde resource create --name {RESOURCE_NAME} &&"
            f"cde resource upload --name {RESOURCE_NAME} --local-path /tmp/{JOB_FILE} &&"
            f"cde job create --name {JOB_NAME} --type spark --mount-1-resource {RESOURCE_NAME}"
            f" --application-file /app/mount/{JOB_FILE}"
        ),
        dag=dag,
    )

    test_run_job = CdeRunJobOperator(
        connection_id=CONNECTION_ID, dag=dag, task_id="spark-job-test", job_name=JOB_NAME
    )

    bash_clean_job = BashOperator(
        task_id="bash-clean-job",
        bash_command=f"if cde job list | grep {JOB_NAME} ;then cde job delete --name {JOB_NAME} && "
        f"cde resource delete --name {RESOURCE_NAME}; fi",
        dag=dag,
    )

    bash_delete_job >> bash_create_job_file >> bash_create_spark_job >> test_run_job >> bash_clean_job

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

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

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessStartJobOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessApplicationSensor, EmrServerlessJobSensor

EXECUTION_ROLE_ARN = getenv('EXECUTION_ROLE_ARN', 'execution_role_arn')
EMR_EXAMPLE_BUCKET = getenv('EMR_EXAMPLE_BUCKET', 'emr_example_bucket')
SPARK_JOB_DRIVER = {
    "sparkSubmit": {
        "entryPoint": "s3://us-east-1.elasticmapreduce/emr-containers/samples/wordcount/scripts/wordcount.py",
        "entryPointArguments": [f"s3://{EMR_EXAMPLE_BUCKET}/output"],
        "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g\
            --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1",
    }
}

SPARK_CONFIGURATION_OVERRIDES = {
    "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": f"s3://{EMR_EXAMPLE_BUCKET}/logs"}}
}

with DAG(
    dag_id='example_emr_serverless',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as emr_serverless_dag:

    # [START howto_operator_emr_serverless_create_application]
    emr_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id='create_emr_serverless_task',
        release_label='emr-6.6.0',
        job_type="SPARK",
        config={'name': 'new_application'},
    )
    # [END howto_operator_emr_serverless_create_application]

    # [START howto_sensor_emr_serverless_application]
    wait_for_app_creation = EmrServerlessApplicationSensor(
        task_id='wait_for_app_creation',
        application_id=emr_serverless_app.output,
    )
    # [END howto_sensor_emr_serverless_application]

    # [START howto_operator_emr_serverless_start_job]
    start_job = EmrServerlessStartJobOperator(
        task_id='start_emr_serverless_job',
        application_id=emr_serverless_app.output,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=SPARK_JOB_DRIVER,
        configuration_overrides=SPARK_CONFIGURATION_OVERRIDES,
    )
    # [END howto_operator_emr_serverless_start_job]

    # [START howto_sensor_emr_serverless_job]
    wait_for_job = EmrServerlessJobSensor(
        task_id='wait_for_job', application_id=emr_serverless_app.output, job_run_id=start_job.output
    )
    # [END howto_sensor_emr_serverless_job]

    # [START howto_operator_emr_serverless_delete_application]
    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id='delete_application', application_id=emr_serverless_app.output, trigger_rule="all_done"
    )
    # [END howto_operator_emr_serverless_delete_application]

    chain(
        emr_serverless_app,
        wait_for_app_creation,
        start_job,
        wait_for_job,
        delete_app,
    )

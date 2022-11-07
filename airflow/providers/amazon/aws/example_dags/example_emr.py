#
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

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")

# [START howto_operator_emr_steps_config]
SPARK_STEPS = [
    {
        "Name": "calculate_pi",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "PiCalc",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "JobFlowRole": JOB_FLOW_ROLE,
    "ServiceRole": SERVICE_ROLE,
}
# [END howto_operator_emr_steps_config]

with DAG(
    dag_id="example_emr",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_emr_create_job_flow]
    job_flow_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )
    # [END howto_operator_emr_create_job_flow]

    job_flow_id = job_flow_creator.output

    # [START howto_sensor_emr_job_flow]
    job_sensor = EmrJobFlowSensor(task_id="check_job_flow", job_flow_id=job_flow_id)
    # [END howto_sensor_emr_job_flow]

    # [START howto_operator_emr_modify_cluster]
    cluster_modifier = EmrModifyClusterOperator(
        task_id="modify_cluster", cluster_id=job_flow_id, step_concurrency_level=1
    )
    # [END howto_operator_emr_modify_cluster]

    # [START howto_operator_emr_add_steps]
    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=job_flow_id,
        steps=SPARK_STEPS,
    )
    # [END howto_operator_emr_add_steps]

    # [START howto_sensor_emr_step]
    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id=job_flow_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    )
    # [END howto_sensor_emr_step]

    # [START howto_operator_emr_terminate_job_flow]
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=job_flow_id,
    )
    # [END howto_operator_emr_terminate_job_flow]

    chain(
        job_flow_creator,
        job_sensor,
        cluster_modifier,
        step_adder,
        step_checker,
        cluster_remover,
    )

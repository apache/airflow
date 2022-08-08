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
import os
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator, EC2StopInstanceOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor

INSTANCE_ID = os.getenv("INSTANCE_ID", "instance-id")

with DAG(
    dag_id='example_ec2',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_ec2_start_instance]
    start_instance = EC2StartInstanceOperator(
        task_id="ec2_start_instance",
        instance_id=INSTANCE_ID,
    )
    # [END howto_operator_ec2_start_instance]

    # [START howto_sensor_ec2_instance_state]
    instance_state = EC2InstanceStateSensor(
        task_id="ec2_instance_state",
        instance_id=INSTANCE_ID,
        target_state="running",
    )
    # [END howto_sensor_ec2_instance_state]

    # [START howto_operator_ec2_stop_instance]
    stop_instance = EC2StopInstanceOperator(
        task_id="ec2_stop_instance",
        instance_id=INSTANCE_ID,
    )
    # [END howto_operator_ec2_stop_instance]

    chain(start_instance, instance_state, stop_instance)

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

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftDeleteClusterOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor

REDSHIFT_CLUSTER_IDENTIFIER = getenv("REDSHIFT_CLUSTER_IDENTIFIER", "redshift-cluster-1")

with DAG(
    dag_id="example_redshift_cluster",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_redshift_cluster]
    task_create_cluster = RedshiftCreateClusterOperator(
        task_id="redshift_create_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        cluster_type="single-node",
        node_type="dc2.large",
        master_username="adminuser",
        master_user_password="dummypass",
    )
    # [END howto_operator_redshift_cluster]

    # [START howto_sensor_redshift_cluster]
    task_wait_cluster_available = RedshiftClusterSensor(
        task_id='sensor_redshift_cluster_available',
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        target_status='available',
        poke_interval=5,
        timeout=60 * 15,
    )
    # [END howto_sensor_redshift_cluster]

    # [START howto_operator_redshift_pause_cluster]
    task_pause_cluster = RedshiftPauseClusterOperator(
        task_id='redshift_pause_cluster',
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
    )
    # [END howto_operator_redshift_pause_cluster]

    task_wait_cluster_paused = RedshiftClusterSensor(
        task_id='sensor_redshift_cluster_paused',
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        target_status='paused',
        poke_interval=5,
        timeout=60 * 15,
    )

    # [START howto_operator_redshift_resume_cluster]
    task_resume_cluster = RedshiftResumeClusterOperator(
        task_id='redshift_resume_cluster',
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
    )
    # [END howto_operator_redshift_resume_cluster]

    # [START howto_operator_redshift_delete_cluster]
    task_delete_cluster = RedshiftDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
    )
    # [END howto_operator_redshift_delete_cluster]

    chain(
        task_create_cluster,
        task_wait_cluster_available,
        task_pause_cluster,
        task_wait_cluster_paused,
        task_resume_cluster,
        task_delete_cluster,
    )

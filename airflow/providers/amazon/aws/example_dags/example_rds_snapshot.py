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
from airflow.providers.amazon.aws.operators.rds import (
    RdsCopyDbSnapshotOperator,
    RdsCreateDbSnapshotOperator,
    RdsDeleteDbSnapshotOperator,
)
from airflow.providers.amazon.aws.sensors.rds import RdsSnapshotExistenceSensor

RDS_DB_IDENTIFIER = getenv("RDS_DB_IDENTIFIER", "database-identifier")
RDS_DB_SNAPSHOT_IDENTIFIER = getenv("RDS_DB_SNAPSHOT_IDENTIFIER", "database-1-snap")

with DAG(
    dag_id='example_rds_snapshot',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_rds_create_db_snapshot]
    create_snapshot = RdsCreateDbSnapshotOperator(
        task_id='create_snapshot',
        db_type='instance',
        db_identifier=RDS_DB_IDENTIFIER,
        db_snapshot_identifier=RDS_DB_SNAPSHOT_IDENTIFIER,
    )
    # [END howto_operator_rds_create_db_snapshot]

    # [START howto_sensor_rds_snapshot_existence]
    snapshot_sensor = RdsSnapshotExistenceSensor(
        task_id='snapshot_sensor',
        db_type='instance',
        db_snapshot_identifier=RDS_DB_IDENTIFIER,
        target_statuses=['available'],
    )
    # [END howto_sensor_rds_snapshot_existence]

    # [START howto_operator_rds_copy_snapshot]
    copy_snapshot = RdsCopyDbSnapshotOperator(
        task_id='copy_snapshot',
        db_type='instance',
        source_db_snapshot_identifier=RDS_DB_IDENTIFIER,
        target_db_snapshot_identifier=f'{RDS_DB_IDENTIFIER}-copy',
    )
    # [END howto_operator_rds_copy_snapshot]

    # [START howto_operator_rds_delete_snapshot]
    delete_snapshot = RdsDeleteDbSnapshotOperator(
        task_id='delete_snapshot',
        db_type='instance',
        db_snapshot_identifier=RDS_DB_IDENTIFIER,
    )
    # [END howto_operator_rds_delete_snapshot]

    chain(create_snapshot, snapshot_sensor, copy_snapshot, delete_snapshot)

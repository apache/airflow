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
from airflow.providers.amazon.aws.operators.rds import RdsCancelExportTaskOperator, RdsStartExportTaskOperator
from airflow.providers.amazon.aws.sensors.rds import RdsExportTaskExistenceSensor

RDS_EXPORT_TASK_IDENTIFIER = getenv("RDS_EXPORT_TASK_IDENTIFIER", "export-task-identifier")
RDS_EXPORT_SOURCE_ARN = getenv(
    "RDS_EXPORT_SOURCE_ARN", "arn:aws:rds:<region>:<account number>:snapshot:snap-id"
)
BUCKET_NAME = getenv("BUCKET_NAME", "bucket-name")
BUCKET_PREFIX = getenv("BUCKET_PREFIX", "bucket-prefix")
ROLE_ARN = getenv("ROLE_ARN", "arn:aws:iam::<account number>:role/Role")
KMS_KEY_ID = getenv("KMS_KEY_ID", "arn:aws:kms:<region>:<account number>:key/key-id")


with DAG(
    dag_id='example_rds_export',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_rds_start_export_task]
    start_export = RdsStartExportTaskOperator(
        task_id='start_export',
        export_task_identifier=RDS_EXPORT_TASK_IDENTIFIER,
        source_arn=RDS_EXPORT_SOURCE_ARN,
        s3_bucket_name=BUCKET_NAME,
        s3_prefix=BUCKET_PREFIX,
        iam_role_arn=ROLE_ARN,
        kms_key_id=KMS_KEY_ID,
    )
    # [END howto_operator_rds_start_export_task]

    # [START howto_operator_rds_cancel_export]
    cancel_export = RdsCancelExportTaskOperator(
        task_id='cancel_export',
        export_task_identifier=RDS_EXPORT_TASK_IDENTIFIER,
    )
    # [END howto_operator_rds_cancel_export]

    # [START howto_sensor_rds_export_task_existence]
    export_sensor = RdsExportTaskExistenceSensor(
        task_id='export_sensor',
        export_task_identifier=RDS_EXPORT_TASK_IDENTIFIER,
        target_statuses=['canceled'],
    )
    # [END howto_sensor_rds_export_task_existence]

    chain(start_export, cancel_export, export_sensor)

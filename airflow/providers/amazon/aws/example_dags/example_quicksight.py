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

from airflow import DAG
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.sensors.quicksight import QuickSightSensor

DATA_SET_ID = "DemoDataSet_Test"
INGESTION_WAITING_ID = "DemoDataSet_Ingestion_Waiting_Test"
INGESTION_NO_WAITING_ID = "DemoDataSet_Ingestion_No_Waiting_Test"
AWS_ACCOUNT_ID = "123456789012"

with DAG(
    "sample_quicksight_dag",
    schedule_interval=None,
    start_date=datetime(2022, 2, 21),
    catchup=False,
) as dag:
    # Create and Start the QuickSight SPICE data ingestion
    # and waits for its completion.
    # [START howto_operator_quicksight]
    quicksight_create_ingestion = QuickSightCreateIngestionOperator(
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_WAITING_ID,
        aws_account_id=AWS_ACCOUNT_ID,
        task_id="sample_quicksight_dag",
    )
    quicksight_create_ingestion
    # [END howto_operator_quicksight]

    # Create and Start the QuickSight SPICE data ingestion
    # and does not wait for its completion
    # [START howto_operator_quicksight_non_waiting]
    quicksight_create_ingestion_no_waiting = QuickSightCreateIngestionOperator(
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_NO_WAITING_ID,
        aws_account_id=AWS_ACCOUNT_ID,
        wait_for_completion=False,
        task_id="sample_quicksight_no_waiting_dag",
    )

    # The following task checks the status of the QuickSight SPICE ingestion
    # job until it succeeds.
    quicksight_job_status = QuickSightSensor(
        aws_account_id=AWS_ACCOUNT_ID,
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_NO_WAITING_ID,
        task_id="check_quicksight_job_status",
    )
    quicksight_create_ingestion_no_waiting >> quicksight_job_status
    # [END howto_operator_quicksight_non_waiting]

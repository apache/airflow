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

DATA_SET_ID = "DemoDataSet_Test"
INGESTION_ID = "DemoDataSet_Ingestion_Test"
AWS_ACCOUNT_ID = "123456789012"

# [START howto_operator_quicksight]
with DAG(
    "sample_quicksight_dag",
    schedule_interval=None,
    start_date=datetime(2022, 2, 21),
    catchup=False,
) as dag:
    quicksight_create_ingestion = QuickSightCreateIngestionOperator(
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_ID,
        aws_account_id=AWS_ACCOUNT_ID,
        task_id="sample_quicksight_dag",
    )

    quicksight_create_ingestion
    # [END howto_operator_quicksight]

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
from airflow.providers.opsgenie.operators.opsgenie import (
    OpsgenieCloseAlertOperator,
    OpsgenieCreateAlertOperator,
)

with DAG(
    dag_id="opsgenie_alert_operator_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    # [START howto_opsgenie_create_alert_operator]
    opsgenie_alert_operator = OpsgenieCreateAlertOperator(task_id="opsgenie_task", message="Hello World!")
    # [END howto_opsgenie_create_alert_operator]

    # [START howto_opsgenie_close_alert_operator]
    opsgenie_close_alert_operator = OpsgenieCloseAlertOperator(
        task_id="opsgenie_close_task", identifier="identifier_example"
    )
    # [END howto_opsgenie_close_alert_operator]

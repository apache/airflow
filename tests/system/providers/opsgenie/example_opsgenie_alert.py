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
from airflow.providers.opsgenie.operators.opsgenie import (
    OpsgenieCloseAlertOperator,
    OpsgenieCreateAlertOperator,
    OpsgenieDeleteAlertOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "opsgenie_alert_operator_dag"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
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

    # [START howto_opsgenie_delete_alert_operator]
    opsgenie_delete_alert_operator = OpsgenieDeleteAlertOperator(
        task_id="opsgenie_delete_task", identifier="identifier_example"
    )
    # [END howto_opsgenie_delete_alert_operator]

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

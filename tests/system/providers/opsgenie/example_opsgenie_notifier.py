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

# [START howto_notifier_opsgenie]
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.opsgenie.notifications.opsgenie import send_opsgenie_notification
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    "opsgenie_notifier",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    on_failure_callback=[send_opsgenie_notification(payload={"message": "Something went wrong!"})],
) as dag:
    BashOperator(
        task_id="mytask",
        bash_command="fail",
        on_failure_callback=[send_opsgenie_notification(payload={"message": "Something went wrong!"})],
    )
# [END howto_notifier_opsgenie]

from tests.system.utils import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

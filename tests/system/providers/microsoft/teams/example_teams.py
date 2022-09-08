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
"""
Example use of TeamsWebhookOperator.
"""

import datetime
import os

import pendulum

from airflow import DAG
from airflow.providers.microsoft.teams.operators.teams_webhook import TeamsWebhookOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
CONN_ID = "teams_conn_id"
DAG_ID = "example_teams"

with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
) as dag:

    # [START howto_operator_teams]

    teams = TeamsWebhookOperator(
        http_conn_id=CONN_ID,
        task_id='teams_op',
        message='First notification',
        subtitle='Spato notify',
        facts=[{'name': 'Issuer', 'value': 'Apache'}],
        action_button_name='Visit',
        action_button_url='https://airflow.apache.org/',
        icon_url='https://airflow.apache.org/_images/pin_large.png',
        theme_color='FF0000',
    )

    # [END howto_operator_teams]

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

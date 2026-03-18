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
Example use of Telegram operator.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_telegram"
CONN_ID = "telegram_conn_id"
CHAT_ID = "-3222103937"

with DAG(DAG_ID, start_date=datetime(2021, 1, 1), schedule=None, tags=["example"]) as dag:
    # [START howto_operator_telegram]

    send_message_telegram_task = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=CONN_ID,
        chat_id=CHAT_ID,
        text="Hello from Airflow!",
        dag=dag,
    )

    # [END howto_operator_telegram]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

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
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "slack_webhook_example_dag"
SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "slack_default")
IMAGE_URL = (
    "https://raw.githubusercontent.com/apache/airflow/main/airflow/www/static/pin_100.png"
)

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    tags=["example"],
) as dag:
    # [START slack_webhook_operator_text_howto_guide]
    slack_webhook_operator_text = SlackWebhookOperator(
        task_id="slack_webhook_send_text",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=(
            "Apache Airflow® is an open-source platform for developing, "
            "scheduling, and monitoring batch-oriented workflows."
        ),
    )
    # [END slack_webhook_operator_text_howto_guide]

    # [START slack_webhook_operator_blocks_howto_guide]
    slack_webhook_operator_blocks = SlackWebhookOperator(
        task_id="slack_webhook_send_blocks",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "*<https://github.com/apache/airflow|Apache Airflow®>* "
                        "is an open-source platform for developing, scheduling, "
                        "and monitoring batch-oriented workflows."
                    ),
                },
                "accessory": {
                    "type": "image",
                    "image_url": IMAGE_URL,
                    "alt_text": "Pinwheel",
                },
            }
        ],
        message="Fallback message",
    )
    # [END slack_webhook_operator_blocks_howto_guide]

    slack_webhook_operator_text >> slack_webhook_operator_blocks

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

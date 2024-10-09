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
This is an example dag for using the DingdingOperator.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.dingding.hooks.dingding import DingdingHook
from airflow.providers.dingding.operators.dingding import DingdingOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_dingding_operator"


# [START howto_operator_dingding_failure_callback]
def failure_callback(context):
    """
    The function that will be executed on failure.

    :param context: The context of the executed task.
    """
    message = f"The task {context['ti'].task_id} failed"
    DingdingHook(message_type="text", message=message, at_all=True).send()


# [END howto_operator_dingding_failure_callback]

with DAG(
    dag_id=DAG_ID,
    default_args={"retries": 3, "on_failure_callback": failure_callback},
    schedule="@once",
    dagrun_timeout=timedelta(minutes=60),
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_dingding]
    text_msg_remind_none = DingdingOperator(
        task_id="text_msg_remind_none",
        message_type="text",
        message="Airflow dingding text message remind none",
        at_mobiles=None,
        at_all=False,
    )
    # [END howto_operator_dingding]

    text_msg_remind_specific = DingdingOperator(
        task_id="text_msg_remind_specific",
        message_type="text",
        message="Airflow dingding text message remind specific users",
        at_mobiles=["156XXXXXXXX", "130XXXXXXXX"],
        at_all=False,
    )

    text_msg_remind_include_invalid = DingdingOperator(
        task_id="text_msg_remind_include_invalid",
        message_type="text",
        message="Airflow dingding text message remind users including invalid",
        # 123 is invalid user or user not in the group
        at_mobiles=["156XXXXXXXX", "123"],
        at_all=False,
    )

    # [START howto_operator_dingding_remind_users]
    text_msg_remind_all = DingdingOperator(
        task_id="text_msg_remind_all",
        message_type="text",
        message="Airflow dingding text message remind all users in group",
        # list of user phone/email here in the group
        # when at_all is specific will cover at_mobiles
        at_mobiles=["156XXXXXXXX", "130XXXXXXXX"],
        at_all=True,
    )
    # [END howto_operator_dingding_remind_users]

    link_msg = DingdingOperator(
        task_id="link_msg",
        message_type="link",
        message={
            "title": "Airflow dingding link message",
            "text": "Airflow official documentation link",
            "messageUrl": "https://airflow.apache.org",
            "picURL": "https://airflow.apache.org/_images/pin_large.png",
        },
    )

    # [START howto_operator_dingding_rich_text]
    markdown_msg = DingdingOperator(
        task_id="markdown_msg",
        message_type="markdown",
        message={
            "title": "Airflow dingding markdown message",
            "text": "# Markdown message title\n"
            "content content .. \n"
            "### sub-title\n"
            "![logo](https://airflow.apache.org/_images/pin_large.png)",
        },
        at_mobiles=["156XXXXXXXX"],
        at_all=False,
    )
    # [END howto_operator_dingding_rich_text]

    single_action_card_msg = DingdingOperator(
        task_id="single_action_card_msg",
        message_type="actionCard",
        message={
            "title": "Airflow dingding single actionCard message",
            "text": "Airflow dingding single actionCard message\n"
            "![logo](https://airflow.apache.org/_images/pin_large.png)\n"
            "This is a official logo in Airflow website.",
            "hideAvatar": "0",
            "btnOrientation": "0",
            "singleTitle": "read more",
            "singleURL": "https://airflow.apache.org",
        },
    )

    multi_action_card_msg = DingdingOperator(
        task_id="multi_action_card_msg",
        message_type="actionCard",
        message={
            "title": "Airflow dingding multi actionCard message",
            "text": "Airflow dingding multi actionCard message\n"
            "![logo](https://airflow.apache.org/_images/pin_large.png)\n"
            "Airflow documentation and GitHub",
            "hideAvatar": "0",
            "btnOrientation": "0",
            "btns": [
                {"title": "Airflow Documentation", "actionURL": "https://airflow.apache.org"},
                {"title": "Airflow GitHub", "actionURL": "https://github.com/apache/airflow"},
            ],
        },
    )

    feed_card_msg = DingdingOperator(
        task_id="feed_card_msg",
        message_type="feedCard",
        message={
            "links": [
                {
                    "title": "Airflow DAG feed card",
                    "messageURL": "https://airflow.apache.org/docs/apache-airflow/stable/ui.html",
                    "picURL": "https://airflow.apache.org/_images/dags.png",
                },
                {
                    "title": "Airflow grid feed card",
                    "messageURL": "https://airflow.apache.org/docs/apache-airflow/stable/ui.html",
                    "picURL": "https://airflow.apache.org/_images/grid.png",
                },
                {
                    "title": "Airflow graph feed card",
                    "messageURL": "https://airflow.apache.org/docs/apache-airflow/stable/ui.html",
                    "picURL": "https://airflow.apache.org/_images/graph.png",
                },
            ]
        },
    )

    msg_failure_callback = DingdingOperator(
        task_id="msg_failure_callback",
        message_type="not_support_msg_type",
        message="",
    )

    (
        [
            text_msg_remind_none,
            text_msg_remind_specific,
            text_msg_remind_include_invalid,
            text_msg_remind_all,
        ]
        >> link_msg
        >> markdown_msg
        >> [
            single_action_card_msg,
            multi_action_card_msg,
        ]
        >> feed_card_msg
        >> msg_failure_callback
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

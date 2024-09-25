 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

How-to Guide for Slack Incoming Webhook notifications
=====================================================

Introduction
------------
Slack Incoming Webhook notifier (:class:`airflow.providers.slack.notifications.slack_webhook.SlackWebhookNotifier`)
allows users to send messages to a slack channel through `Incoming Webhook <https://api.slack.com/messaging/webhooks>`__
using the various ``on_*_callbacks`` at both the DAG level and Task level


Example Code:
-------------

.. code-block:: python

    from datetime import datetime, timezone
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification

    dag_failure_slack_webhook_notification = send_slack_webhook_notification(
        slack_webhook_conn_id="slackwebhook", text="The dag {{ dag.dag_id }} failed"
    )
    task_failure_slack_webhook_notification = send_slack_webhook_notification(
        slack_webhook_conn_id="slackwebhook",
        text="The task {{ ti.task_id }} failed",
    )

    with DAG(
        dag_id="mydag",
        schedule="@once",
        start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        on_failure_callback=[dag_failure_slack_webhook_notification],
        catchup=False,
    ):
        BashOperator(
            task_id="mytask", on_failure_callback=[task_failure_slack_webhook_notification], bash_command="fail"
        )

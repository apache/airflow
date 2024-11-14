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

How-to Guide for Slack notifications
====================================

Introduction
------------
Slack notifier (:class:`airflow.providers.slack.notifications.slack.SlackNotifier`) allows users to send
messages to a slack channel using the various ``on_*_callbacks`` at both the DAG level and Task level


Example Code:
-------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.slack.notifications.slack import send_slack_notification

    with DAG(
        start_date=datetime(2023, 1, 1),
        on_success_callback=[
            send_slack_notification(
                text="The DAG {{ dag.dag_id }} succeeded",
                channel="#general",
                username="Airflow",
            )
        ],
    ):
        BashOperator(
            task_id="mytask",
            on_failure_callback=[
                send_slack_notification(
                    text="The task {{ ti.task_id }} failed",
                    channel="#general",
                    username="Airflow",
                )
            ],
            bash_command="fail",
        )

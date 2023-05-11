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

How-to Guide for Pagerduty notifications
========================================

Introduction
------------
Pagerduty notifier (:class:`airflow.providers.pagerduty.notifications.pagerduty_notifier.PagerdutyNotifier`) allows users to send
messages to a pagerduty using the various ``on_*_callbacks`` at both the DAG level and Task level

Example Code:
-------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.providers.pagerduty.notifications.pagerduty_notifier import send_pagerduty_notification

    with DAG(
        dag_id="pagerduty_notifier",
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        on_success_callback=[
            send_pagerduty_notification(
                summary="DISK at 99%",
                severity="critical",
                source="database",
                # action=self.action,
                dedup_key="srv055/mysql",
                custom_details={"free space": "1%", "ping time": "1500ms", "load avg": 0.75},
                group="prod-datapipe",
                component="database",
                class_type="disk",
                images=[
                    {
                        "src": "https://chart.googleapis.com/chart",
                        "href": "https://google.com",
                        "alt": "An example link with an image",
                    }
                ],
                links=[{"href": "http://pagerduty.example.com", "text": "An example link."}],
            )
        ],
    ):
        BashOperator(
            task_id="mytask",
            on_failure_callback=[
                send_pagerduty_notification(
                    summary="DISK at 99%",
                    severity="critical",
                    source="database",
                    # action=self.action,
                    dedup_key="srv0555/mysql",
                    custom_details={"free space": "1%", "ping time": "1500ms", "load avg": 0.75},
                    group="prod-datapipe",
                    component="database",
                    class_type="disk",
                    images=[
                        {
                            "src": "https://chart.googleapis.com/chart",
                            "href": "https://google.com",
                            "alt": "An example link with an image",
                        }
                    ],
                    links=[{"href": "http://pagerduty.example.com", "text": "An example link."}],
                )
            ],
            bash_command="fail",
        )

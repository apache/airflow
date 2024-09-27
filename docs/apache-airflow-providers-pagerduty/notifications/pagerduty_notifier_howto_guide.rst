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
The Pagerduty notifier (:class:`airflow.providers.pagerduty.notifications.pagerduty.PagerdutyNotifier`) allows users to send
messages to Pagerduty using the various ``on_*_callbacks`` at both the DAG level and Task level.


Example Code:
-------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.providers.pagerduty.notifications.pagerduty import send_pagerduty_notification

    with DAG(
        "pagerduty_notifier",
        start_date=datetime(2023, 1, 1),
        on_failure_callback=[
            send_pagerduty_notification(
                summary="The dag {{ dag.dag_id }} failed",
                severity="critical",
                source="airflow dag_id: {{dag.dag_id}}",
                dedup_key="{{dag.dag_id}}-{{ti.task_id}}",
                group="{{dag.dag_id}}",
                component="airflow",
                class_type="Prod Data Pipeline",
            )
        ],
    ):
        BashOperator(
            task_id="mytask",
            bash_command="fail",
            on_failure_callback=[
                send_pagerduty_notification(
                    summary="The task {{ ti.task_id }} failed",
                    severity="critical",
                    source="airflow dag_id: {{dag.dag_id}}",
                    dedup_key="{{dag.dag_id}}-{{ti.task_id}}",
                    group="{{dag.dag_id}}",
                    component="airflow",
                    class_type="Prod Data Pipeline",
                )
            ],
        )

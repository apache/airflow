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

How-to Guide for Apprise notifications
========================================

Introduction
------------
The apprise notifier (:class:`airflow.providers.apprise.notifications.apprise.AppriseNotifier`) allows users to send
messages to `multiple service <https://github.com/caronc/apprise#supported-notifications>`_ using the various ``on_*_callbacks`` at both the Dag level and Task level.

Example Code:
-------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.apprise.notifications.apprise import send_apprise_notification
    from apprise import NotifyType

    with DAG(
        dag_id="apprise_notifier_testing",
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        on_success_callback=[
            send_apprise_notification(body="The Dag {{ dag.dag_id }} succeeded", notify_type=NotifyType.SUCCESS)
        ],
    ):
        BashOperator(
            task_id="mytask",
            on_failure_callback=[
                send_apprise_notification(body="The task {{ ti.task_id }} failed", notify_type=NotifyType.FAILURE)
            ],
            bash_command="fail",
        )

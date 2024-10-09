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

How-to Guide for Chime notifications
====================================

Introduction
------------
Chime notifier (:class:`airflow.providers.amazon.aws.notifications.chime.ChimeNotifier`) allows users to send
messages to a Chime chat room setup via a webhook using the various ``on_*_callbacks`` at both the DAG level and Task level


Example Code:
-------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.amazon.aws.notifications.chime import send_chime_notification

    with DAG(
        dag_id="mydag",
        schedule="@once",
        start_date=datetime(2023, 6, 27),
        on_success_callback=[
            send_chime_notification(chime_conn_id="my_chime_conn", message="The DAG {{ dag.dag_id }} succeeded")
        ],
        catchup=False,
    ):
        BashOperator(
            task_id="mytask",
            on_failure_callback=[
                send_chime_notification(chime_conn_id="my_chime_conn", message="The task {{ ti.task_id }} failed")
            ],
            bash_command="fail",
        )

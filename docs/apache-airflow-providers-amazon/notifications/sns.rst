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

.. _howto/notifier:SnsNotifier:

How-to Guide for Amazon Simple Notification Service (Amazon SNS) notifications
==============================================================================

Introduction
------------
`Amazon SNS <https://aws.amazon.com/sns/>`__ notifier :class:`~airflow.providers.amazon.aws.notifications.sns.SnsNotifier`
allows users to push messages to a SNS Topic using the various ``on_*_callbacks`` at both the DAG level and Task level.


Example Code:
-------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.amazon.aws.notifications.sns import send_sns_notification

    dag_failure_sns_notification = send_sns_notification(
        aws_conn_id="aws_default",
        region_name="eu-west-2",
        message="The DAG {{ dag.dag_id }} failed",
        target_arn="arn:aws:sns:us-west-2:123456789098:TopicName",
    )
    task_failure_sns_notification = send_sns_notification(
        aws_conn_id="aws_default",
        region_name="eu-west-2",
        message="The task {{ ti.task_id }} failed",
        target_arn="arn:aws:sns:us-west-2:123456789098:AnotherTopicName",
    )

    with DAG(
        dag_id="mydag",
        schedule="@once",
        start_date=datetime(2023, 1, 1),
        on_failure_callback=[dag_failure_sns_notification],
        catchup=False,
    ):
        BashOperator(
            task_id="mytask",
            on_failure_callback=[task_failure_sns_notification],
            bash_command="fail",
        )

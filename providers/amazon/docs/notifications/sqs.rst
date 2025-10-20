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

.. _howto/notifier:SqsNotifier:

How-to Guide for Amazon Simple Queue Service (Amazon SQS) notifications
=======================================================================

Introduction
------------
`Amazon SQS <https://aws.amazon.com/sqs/>`__ notifier :class:`~airflow.providers.amazon.aws.notifications.sqs.SqsNotifier`
allows users to push messages to an Amazon SQS Queue using the various ``on_*_callbacks`` at both the Dag level and Task level.


Example Code:
-------------

.. code-block:: python

    from datetime import datetime, timezone
    from airflow import DAG
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.amazon.aws.notifications.sqs import send_sqs_notification

    dag_failure_sqs_notification = send_sqs_notification(
        aws_conn_id="aws_default",
        queue_url="https://sqs.eu-west-1.amazonaws.com/123456789098/MyQueue",
        message_body="The Dag {{ dag.dag_id }} failed",
    )
    task_failure_sqs_notification = send_sqs_notification(
        aws_conn_id="aws_default",
        region_name="eu-west-1",
        queue_url="https://sqs.eu-west-1.amazonaws.com/123456789098/MyQueue",
        message_body="The task {{ ti.task_id }} failed",
    )

    with DAG(
        dag_id="mydag",
        schedule="@once",
        start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
        on_failure_callback=[dag_failure_sqs_notification],
        catchup=False,
    ):
        BashOperator(task_id="mytask", on_failure_callback=[task_failure_sqs_notification], bash_command="fail")

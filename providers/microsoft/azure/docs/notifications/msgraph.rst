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

.. _howto/notifier:MSGraphNotifier:

Microsoft Graph Notifier
========================

`Microsoft Graph <https://learn.microsoft.com/en-us/graph/overview>`__ is the recommended way to send mail
from an Office 365 or Outlook cloud mailbox programmatically.
:class:`~airflow.providers.microsoft.azure.notifications.msgraph.MSGraphNotifier` sends an email through the
`sendMail <https://learn.microsoft.com/en-us/graph/api/user-sendmail>`__ endpoint over a
:ref:`Microsoft Graph API connection <howto/connection:msgraph>`.

The permissions the app registration needs and the limit on attachments passed through ``files`` are the
same as for the email backend, and are described in :doc:`../email-backend`. That page also covers routing
the ``email_on_failure`` and ``email_on_retry`` emails through Microsoft Graph.

Sending a notification
----------------------

.. code-block:: python

    from airflow.providers.microsoft.azure.notifications.msgraph import MSGraphNotifier

    dag_failure_notification = MSGraphNotifier(
        from_email="airflow@example.com",
        to="team@example.com",
        subject="Dag {{ dag.dag_id }} failed",
        html_content="Task {{ ti.task_id }} failed on {{ ds }}",
    )

    with DAG(
        dag_id="mydag",
        schedule="@daily",
        on_failure_callback=[dag_failure_notification],
    ):
        BashOperator(
            task_id="mytask",
            bash_command="fail",
            on_failure_callback=[
                MSGraphNotifier(
                    from_email="airflow@example.com",
                    to="team@example.com",
                    subject="Task {{ ti.task_id }} failed",
                    html_content="Have a look at the logs of {{ ti.log_url }}",
                )
            ],
        )

When ``from_email`` is omitted, the ``[email] from_email`` configuration option is used instead.

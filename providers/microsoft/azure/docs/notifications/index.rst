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

Microsoft Graph Notifications
=============================

`Microsoft Graph <https://learn.microsoft.com/en-us/graph/overview>`__ is the recommended way to send mail
from an Office 365 or Outlook cloud mailbox programmatically.
:class:`~airflow.providers.microsoft.azure.notifications.msgraph.MSGraphNotifier` sends an email through the
`sendMail <https://learn.microsoft.com/en-us/graph/api/user-sendmail>`__ endpoint over a
:ref:`Microsoft Graph API connection <howto/connection:msgraph>`.

Prerequisites
-------------

The app registration behind the connection needs the ``Mail.Send`` permission. When application permissions
are used, an administrator has to grant the application access to the mailbox the mail is sent from, for
example by scoping it with an
`application access policy <https://learn.microsoft.com/en-us/graph/auth-limit-mailbox-access>`__.

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

Attachments passed through ``files`` are sent inline in the request body, which Microsoft Graph limits to
4 MB in total. Attachments adding up to more than 3 MB are rejected with a ``ValueError``, and have to be
uploaded to a draft message with an upload session instead.

Sending task failure and retry emails
-------------------------------------

Set the ``[email] email_backend`` configuration option to send the emails triggered by ``email_on_failure``
and ``email_on_retry`` through Microsoft Graph as well. See :doc:`apache-airflow:howto/email-config`.

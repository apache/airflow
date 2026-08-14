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

.. _email-configuration-msgraph:

Send email using Microsoft Graph
================================

Airflow can be configured to send e-mail from an Office 365 or Outlook cloud mailbox using
`Microsoft Graph <https://learn.microsoft.com/en-us/graph/api/user-sendmail>`__ as the
``email_backend``, so that task callback emails (success, failure, retry) go out through Graph instead
of SMTP. See :doc:`apache-airflow:howto/email-config` for the general Airflow email configuration this
page builds on.

.. note::

   If you instead want to send an email from a task callback or a deadline alert, use the
   :ref:`MSGraphNotifier <howto/notifier:MSGraphNotifier>` documented in :doc:`notifications/msgraph`.

Follow the steps below to enable it:

1. Install the ``microsoft-azure`` provider as part of your Airflow installation:

   .. code-block:: bash

      pip install 'apache-airflow[microsoft-azure]'

2. Update the ``[email]`` section in ``airflow.cfg``:

   .. code-block:: ini

      [email]
      email_backend = airflow.providers.microsoft.azure.emailer.send_email
      email_conn_id = msgraph_default
      from_email = From email <email@example.com>

   Equivalent environment variables look like:

   .. code-block:: sh

      AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.providers.microsoft.azure.emailer.send_email
      AIRFLOW__EMAIL__EMAIL_CONN_ID=msgraph_default
      AIRFLOW__EMAIL__FROM_EMAIL=email@example.com

   ``from_email`` is required and selects the mailbox the message is sent from.

3. Create a connection called ``msgraph_default``, or choose a custom connection name and set it in
   ``email_conn_id``, of type ``Microsoft Graph API``. See
   :ref:`Microsoft Graph API Connection <howto/connection:msgraph>` for how to configure it.

The app registration behind the connection needs the ``Mail.Send`` permission. When application
permissions are used, an administrator has to grant the application access to the sending mailbox, for
example by scoping it with an
`application access policy <https://learn.microsoft.com/en-us/graph/auth-limit-mailbox-access>`__.

Attachments are sent inline in the request body, which Microsoft Graph limits to 4 MB in total.
Attachments adding up to more than 3 MB are rejected with a ``ValueError``, and have to be uploaded to
a draft message with an upload session instead.

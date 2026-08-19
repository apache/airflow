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

.. _email-configuration-sendgrid:

Send email using SendGrid
==========================

Airflow can be configured to send e-mail using `SendGrid <https://sendgrid.com/>`__, either through
its SMTP relay or through this provider's API-based backend. See :doc:`apache-airflow:howto/email-config`
for the general Airflow email configuration this page builds on.

Using Default SMTP
-------------------

You can use the default Airflow SMTP backend to send email with SendGrid without installing this provider:

.. code-block:: ini

   [smtp]
   smtp_host=smtp.sendgrid.net
   smtp_starttls=False
   smtp_ssl=False
   smtp_port=587
   smtp_mail_from=<your-from-email>

Equivalent environment variables look like:

.. code-block:: sh

   AIRFLOW__SMTP__SMTP_HOST=smtp.sendgrid.net
   AIRFLOW__SMTP__SMTP_STARTTLS=False
   AIRFLOW__SMTP__SMTP_SSL=False
   AIRFLOW__SMTP__SMTP_PORT=587
   AIRFLOW__SMTP__SMTP_MAIL_FROM=<your-from-email>

Using the SendGrid Provider
-----------------------------

To send email through SendGrid's API instead, follow the steps below:

1. Set up your SendGrid account, then locate your SMTP username and API Key.

2. Install the ``sendgrid`` provider as part of your Airflow installation, e.g.:

   .. code-block:: bash

      pip install 'apache-airflow[sendgrid]' --constraint ...

   or

   .. code-block:: bash

      pip install 'apache-airflow-providers-sendgrid' --constraint ...

3. Update the ``[email]`` section in ``airflow.cfg``:

   .. code-block:: ini

      [email]
      email_backend = airflow.providers.sendgrid.utils.emailer.send_email
      email_conn_id = sendgrid_default
      from_email = "hello@eg.com"

   Equivalent environment variables look like:

   .. code-block:: sh

      AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.providers.sendgrid.utils.emailer.send_email
      AIRFLOW__EMAIL__EMAIL_CONN_ID=sendgrid_default
      SENDGRID_MAIL_FROM=hello@thelearning.dev

4. Create a connection called ``sendgrid_default``, or choose a custom connection name and set it in
   ``email_conn_id``, of ``Email`` type. Only the login and password fields are used from the connection;
   set the password field to your SendGrid API Key. See :doc:`apache-airflow:howto/connection` for how
   to create a connection.

.. note:: The callbacks for success, failure and retry will use the same configuration to send the email.

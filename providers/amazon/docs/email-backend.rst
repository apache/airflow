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

.. _email-configuration-ses:

Send email using AWS SES
==========================

Airflow can be configured to send e-mail using `AWS SES <https://aws.amazon.com/ses/>`__ as the
``email_backend``, so that task callback emails (success, failure, retry) go out through SES instead
of SMTP. See :doc:`apache-airflow:howto/email-config` for the general Airflow email configuration this
page builds on.

.. note::

   If you instead want to send an email as a Dag task, use the
   :ref:`SesEmailOperator <howto/operator:SesEmailOperator>` documented in :doc:`operators/ses`.

Follow the steps below to enable it:

1. Install the ``amazon`` provider as part of your Airflow installation:

   .. code-block:: bash

      pip install 'apache-airflow[amazon]'

2. Update the ``[email]`` section in ``airflow.cfg``:

   .. code-block:: ini

      [email]
      email_backend = airflow.providers.amazon.aws.utils.emailer.send_email
      email_conn_id = aws_default
      from_email = From email <email@example.com>

   Equivalent environment variables look like:

   .. code-block:: sh

      AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.providers.amazon.aws.utils.emailer.send_email
      AIRFLOW__EMAIL__EMAIL_CONN_ID=aws_default
      AIRFLOW__EMAIL__FROM_EMAIL=email@example.com

   ``from_email`` is required and must be a sender address verified with SES.

3. Create a connection called ``aws_default``, or choose a custom connection name and set it in
   ``email_conn_id``, of type ``Amazon Web Services``. See :doc:`apache-airflow:howto/connection` for
   how to create a connection.

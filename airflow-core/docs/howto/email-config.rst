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

Email Configuration
===================

Airflow sends task emails through the function set in ``[email] email_backend``. By default, email is sent
over SMTP by the SMTP provider (``apache-airflow-providers-smtp``). To set up the SMTP server and its
credentials, see :doc:`apache-airflow-providers-smtp:email-backend` and
:doc:`apache-airflow-providers-smtp:connections/smtp`.

The ``[email]`` section also sets the connection, sender and message templates used for any backend:

.. code-block:: ini

  [email]
  email_backend = airflow.utils.email.send_email_smtp
  email_conn_id = smtp_default
  from_email = "John Doe <johndoe@example.com>"
  subject_template = /path/to/my_subject_template_file
  html_content_template = /path/to/my_html_content_template_file

The templates use `Jinja <https://jinja.palletsprojects.com/>`_ and can access the task's information, for
example ``{{ti.log_url}}``.

.. note::

    The functions in ``airflow.utils.email`` are deprecated and now live in
    ``airflow.providers.smtp.utils.emailer``. Only the Python imports moved: keep ``email_backend`` set to
    ``airflow.utils.email.send_email_smtp``. Airflow treats that value as "send with ``SmtpNotifier``",
    which reads all SMTP settings from the Connection. Any other value, including the provider path, calls
    that function instead, and the SMTP provider's function reads the server settings from ``[smtp]``.

To send email through another service, set ``email_backend`` to a backend from another provider; see
:doc:`apache-airflow-providers:core-extensions/email-backends`.

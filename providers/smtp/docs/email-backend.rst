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

Email backend
=============

:mod:`airflow.providers.smtp.utils.emailer` sends email over SMTP. SMTP server
settings are read from the ``[smtp]`` configuration section, and the login and
password from the Airflow Connection given by ``conn_id`` (``smtp_default`` by
default). If that Connection does not exist, the email is sent without authentication.

To send Airflow's email notifications through this module, set:

.. code-block:: ini

    [email]
    email_backend = airflow.providers.smtp.utils.emailer.send_email_smtp

Unlike :class:`~airflow.providers.smtp.hooks.smtp.SmtpHook`, which takes all of its
settings from the SMTP Connection, this module reads the server settings from the
``[smtp]`` configuration section.

The ``[smtp]`` options and their defaults:

.. list-table::
   :header-rows: 1

   * - Option
     - Default
     - Description
   * - ``smtp_host``
     - ``localhost``
     - Host of the SMTP server.
   * - ``smtp_port``
     - ``25``
     - Port of the SMTP server.
   * - ``smtp_starttls``
     - ``True``
     - Send the STARTTLS command after connecting.
   * - ``smtp_ssl``
     - ``False``
     - Connect over SSL.
   * - ``smtp_mail_from``
     - ``airflow@example.com``
     - Sender address. It is used instead of the ``from_email`` argument.
   * - ``smtp_timeout``
     - ``30``
     - Seconds to wait for the connection to the server.
   * - ``smtp_retry_limit``
     - ``5``
     - How many more times to try connecting after the first attempt fails.

Set them in ``airflow.cfg`` or with environment variables such as ``AIRFLOW__SMTP__SMTP_HOST``.
``[email] ssl_context`` (``default`` or ``none``) sets the TLS context used by ``smtp_ssl`` and STARTTLS.

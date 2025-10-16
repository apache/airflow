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

.. _howto/connection:smtp:

SMTP Connection
===============

The **SMTP** connection type enables integrations such as
:class:`~airflow.providers.smtp.hooks.smtp.SmtpHook`.

.. note::
   The legacy helper in ``airflow.utils.email`` is **scheduled for deprecation**
   and will be removed in a future major release.
   Please migrate to :class:`~airflow.providers.smtp.hooks.smtp.SmtpHook`
   or other provider-level utilities for sending emails.

Default Connection ID
---------------------

The default ID is ``smtp_default`` when no ``conn_id`` is supplied.

Authenticating to SMTP
----------------------

Two methods are supported:

* **Basic** – traditional *username + password*.
* **OAuth 2 / XOAUTH2** – bearer-token based, required by Gmail API,
  Microsoft 365 / Outlook.com and other modern providers.

If you omit credentials the hook attempts an **anonymous** session, accepted
only by open-relay test servers.

Configuring the Connection
--------------------------

**Login**
    Username (for example ``user@example.com``).

**Password**
    Password or *app-specific* password.
    Ignored when ``auth_type="oauth2"``.

**Host**
    SMTP server hostname (for example ``smtp.gmail.com``).

**Port**
    Port number. Defaults to **465** when SSL is enabled, otherwise **587**.

**Extra** *(optional – JSON)*
    Additional parameters.

    **General**

    * ``from_email`` – Default **From:** address.
    * ``disable_ssl`` *(bool)* – Disable SSL/TLS entirely. Default ``false``.
    * ``disable_tls`` *(bool)* – Skip ``STARTTLS``. Default ``false``.
    * ``timeout`` *(int)* – Socket timeout (seconds). Default ``30``.
    * ``retry_limit`` *(int)* – Connection attempts before raising. Default ``5``.
    * ``ssl_context`` – ``"default"`` | ``"none"``
      See :ref:`howto/connection:smtp:ssl-context`.

    **Templating**

    * ``subject_template`` – File path for custom subject.
    * ``html_content_template`` – File path for custom HTML body.

    **Authentication**

    * ``auth_type`` – ``"basic"`` *(default)* | ``"oauth2"``
    * ``access_token`` – OAuth 2 bearer (one-hour).
    * ``client_id`` / ``client_secret`` – Credentials for token refresh.
      (auto-defaults to Google or Microsoft).
    * ``tenant_id`` – Azure tenant (default ``"common"``).
    * ``scope`` – OAuth scope

      * **Gmail**: ``https://mail.google.com/``
      * **Outlook (Graph)**: ``https://outlook.office.com/.default``

.. _howto/connection:smtp:ssl-context:

SSL / TLS Notes
^^^^^^^^^^^^^^^

* ``ssl_context="default"`` – reasonable trust store & secure ciphers *(recommended)*
* ``ssl_context="none"`` – **disables certificate validation**; use only for
  local testing with self-signed certificates.

Examples
--------

Basic Auth — SendGrid (STARTTLS 587)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export AIRFLOW_CONN_SMTP_SENDGRID='smtp://apikey:SG.YOUR_API_KEY@smtp.sendgrid.net:587?\
   disable_ssl=true&\
   from_email=you%40example.com'

OAuth 2 — Gmail (access token, STARTTLS 587)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export AIRFLOW_CONN_SMTP_GMAIL='smtp://your.name%40gmail.com@smtp.gmail.com:587?\
   auth_type=oauth2&\
   access_token=ya29.<URL_ENCODED_TOKEN>&\
   from_email=your.name%40gmail.com&\
   disable_ssl=true'


OAuth 2 — Gmail (SSL 465)
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export AIRFLOW_CONN_SMTP_GMAIL_SSL='smtp://your.name%40gmail.com@smtp.gmail.com:465?\
   auth_type=oauth2&\
   access_token=ya29.<URL_ENCODED_TOKEN>&\
   from_email=your.name%40gmail.com&\
   disable_tls=true'

OAuth 2 — Microsoft 365 (client credentials 587)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export AIRFLOW_CONN_SMTP_M365='smtp://user%40contoso.com@smtp.office365.com:587?\
   auth_type=oauth2&\
   client_id=YOUR_APP_ID&\
   client_secret=YOUR_SECRET&\
   tenant_id=YOUR_TENANT_ID&\
   scope=https%3A%2F%2Foutlook.office.com%2F.default&\
   disable_ssl=true'


OAuth2 — Microsoft 365 (client-credential flow)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export AIRFLOW_CONN_SMTP_M365='smtp://user@contoso.com@smtp.office365.com:587?\
   auth_type=oauth2&\
   client_id=YOUR_APP_ID&\
   client_secret=YOUR_SECRET&\
   tenant_id=YOUR_TENANT_ID&\
   scope=https%3A%2F%2Foutlook.office.com%2F.default'


Troubleshooting
~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - **Error message**
     - **Likely cause**
     - **Fix**
   * - ``SSL WRONG_VERSION_NUMBER``
     - Port 587 but the connection starts with SSL (no **STARTTLS**).
     - Add ``disable_ssl=true`` **or** switch to port 465.
   * - ``STARTTLS required``
     - Port 465 yet the hook still issues ``STARTTLS``.
     - Add ``disable_tls=true`` **or** switch to port 587.
   * - ``530 Authentication Required``
     - Access-token expired or missing the ``https://mail.google.com/`` scope.
     - Generate a fresh token.
   * - ``550 From address not verified``
     - Sender identity not verified at the provider **or** ``from_email`` mismatch.
     - Verify the sender / domain and ensure ``from_email`` exactly matches it.


Programmatic creation
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from airflow.models.connection import Connection

   conn = Connection(
       conn_id="smtp_gmail_token",
       conn_type="smtp",
       host="smtp.gmail.com",
       login="me@gmail.com",
       extra={"auth_type": "oauth2", "access_token": "ya29.a0AfB..."},
   )
   print(conn.test_connection())

URI encoding
^^^^^^^^^^^^

When creating connections programmatically or via the CLI, ensure that

When fields contain special characters (``/``, ``@``, ``:`` …), URL-encode them,
for example via
:py:meth:`airflow.models.connection.Connection.get_uri`.

CLI creation (Gmail OAuth 2)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Prefer environment variables for portability, but you can also create the
connection via **CLI**:

.. code-block:: bash

   airflow connections add smtp_gmail_oauth2 \
     --conn-type smtp \
     --conn-host smtp.gmail.com \
     --conn-port 587 \
     --conn-login '<YOUR_EMAIL>@gmail.com' \
     --conn-extra '{
       "from_email": "<YOUR_EMAIL>@gmail.com",
       "auth_type": "oauth2",
       "access_token": "<YOUR_OAUTH2_ACCESS_TOKEN>",
       "disable_ssl": "true"
     }'

.. note::
   The ``[smtp]`` section in ``airflow.cfg`` is used by the **core**
   e-mail helper slated for deprecation.
   When you switch to :class:`~airflow.providers.smtp.hooks.smtp.SmtpHook`
   *and* supply a ``smtp_conn_id``, the hook's connection settings take
   precedence and the global ``[smtp]`` options may be ignored.

Using ``SmtpHook`` in a Dag
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python
   :linenos:

   from datetime import datetime

   from airflow import DAG
   from airflow.operators.python import PythonOperator
   from airflow.providers.smtp.hooks.smtp import SmtpHook


   def gmail_oauth2_test():
       with SmtpHook(smtp_conn_id="smtp_gmail_oauth2") as hook:
           hook.send_email_smtp(
               to="recipient@example.com",
               subject="[Airflow→Gmail] OAuth2 OK",
               html_content="<h3>Gmail XOAUTH2 works 🎉</h3>",
           )


   with DAG(
       dag_id="test_gmail_oauth2",
       start_date=datetime(2025, 7, 1),
       schedule=None,
       catchup=False,
       tags=["example"],
   ) as dag:
       PythonOperator(
           task_id="send_mail",
           python_callable=gmail_oauth2_test,
       )

----

.. seealso::
   * :class:`airflow.providers.smtp.hooks.smtp.SmtpHook`
   * Google OAuth 2.0 for Gmail – https://developers.google.com/identity/protocols/oauth2
   * Microsoft Graph OAuth 2.0 – https://learn.microsoft.com/graph/auth/

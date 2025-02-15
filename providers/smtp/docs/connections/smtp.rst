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

The SMTP connection type enables integrations with the SMTP client.

Authenticating to SMTP
----------------------

Authenticate to the SMTP client with the login and password field.
Use standard `SMTP authentication
<https://docs.python.org/3/library/smtplib.html>`_

Default Connection IDs
----------------------

Hooks, operators, and sensors related to SMTP use ``smtp_default`` by default.

Configuring the Connection
--------------------------

Login
    Specify the username used for the SMTP client.

Password
    Specify the password used for the SMTP client.

Host
    Specify the SMTP host url.

Port
    Specify the SMTP port to connect to. The default depends on the whether you use ssl or not.

Extra (optional)
    Specify the extra parameters (as json dictionary)

    * ``from_email``: The email address from which you want to send the email.
    * ``disable_ssl``: If set to true, then a non-ssl connection is being used. Default is false. Also note that changing the ssl option also influences the default port being used.
    * ``timeout``: The SMTP connection creation timeout in seconds. Default is 30.
    * ``disable_tls``: By default the SMTP connection is created in TLS mode. Set to false to disable tls mode.
    * ``retry_limit``: How many attempts to connect to the server before raising an exception. Default is 5.
    * ``ssl_context``: Can be "default" or "none". Only valid when SSL is used. The "default" context provides a balance between security and compatibility, "none" is not recommended
      as it disables validation of certificates and allow MITM attacks, and is only needed in case your certificates are wrongly configured in your system. If not specified, defaults are taken from the
      "smtp_provider", "ssl_context" configuration with the fallback to "email". "ssl_context" configuration. If none of it is specified, "default" is used.
    * ``subject_template``: A path to a file containing the email subject template.
    * ``html_content_template``: A path to a file containing the email html content template.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_SMTP_DEFAULT='smtp://username:password@smtp.sendgrid.net:587'

Another example for connecting via a non-SSL connection.

.. code-block:: bash

   export AIRFLOW_CONN_SMTP_NOSSL='smtp://username:password@smtp.sendgrid.net:587?disable_ssl=true'

Note that you can set the port regardless of whether you choose to use ssl or not. The above examples show default ports for SSL and Non-SSL connections.

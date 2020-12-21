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

Webserver
=========

This topic describes how to configure Airflow to secure your webserver.

.. contents::
  :depth: 1
  :local:

Rendering Airflow UI in a Web Frame from another site
------------------------------------------------------

Using Airflow in a web frame is enabled by default. To disable this (and prevent click jacking attacks)
set the below:

.. code-block:: ini

    [webserver]
    x_frame_enabled = False

Sensitive Variable fields
-------------------------

By default, Airflow Value of a variable will be hidden if the key contains any words in
(‘password’, ‘secret’, ‘passwd’, ‘authorization’, ‘api_key’, ‘apikey’, ‘access_token’), but can be configured
to extend this list by using the following configurations option:

.. code-block:: ini

    [admin]
    hide_sensitive_variable_fields = comma_separated_sensitive_variable_fields_list

.. _web-authentication:

Web Authentication
------------------

By default, Airflow requires users to specify a password prior to login. You can use the
following CLI commands to create an account:

.. code-block:: bash

    # create an admin user
    airflow users create \
        --username admin \
        --firstname Peter \
        --lastname Parker \
        --role Admin \
        --email spiderman@superhero.org

It is however possible to switch on authentication by either using one of the supplied
backends or creating your own.

Be sure to checkout :doc:`/rest-api-ref` for securing the API.

.. note::

   Airflow uses the config parser of Python. This config parser interpolates
   '%'-signs.  Make sure escape any ``%`` signs in your config file (but not
   environment variables) as ``%%``, otherwise Airflow might leak these
   passwords on a config parser exception to a log.

Password
''''''''

One of the simplest mechanisms for authentication is requiring users to specify a password before logging in.

Please use command line interface ``airflow users create`` to create accounts, or do that in the UI.

Other Methods
'''''''''''''

Since the Airflow 2.0, the default UI is the Flask App Builder RBAC. A ``webserver_config.py`` configuration file
it's automatically generated and can be used to configure the Airflow to support authentication
methods like OAuth, OpenID, LDAP, REMOTE_USER.

For previous versions from Airflow, the ``$AIRFLOW_HOME/airflow.cfg`` following entry needs to be set to enable
the Flask App Builder RBAC UI.

.. code-block:: ini

    rbac = True

The default authentication option described in the :ref:`Web Authentication <web-authentication>` section it's related
with the following entry in the ``$AIRFLOW_HOME/webserver_config.py``.

.. code-block:: ini

    AUTH_TYPE = AUTH_DB

Another way to create users it's in the UI login page, allowing user self registration through a "Register" button.
The following entries in the ``$AIRFLOW_HOME/webserver_config.py`` can be edited to make it possible:

.. code-block:: ini

    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Desired Role For The Self Registered User"
    RECAPTCHA_PRIVATE_KEY = 'private_key'
    RECAPTCHA_PUBLIC_KEY = 'public_key'

    MAIL_SERVER = 'smtp.gmail.com'
    MAIL_USE_TLS = True
    MAIL_USERNAME = 'yourappemail@gmail.com'
    MAIL_PASSWORD = 'passwordformail'
    MAIL_DEFAULT_SENDER = 'sender@gmail.com'

The package ``Flask-Mail`` needs to be installed through pip to allow user self registration since it is a
feature provided by the framework Flask-AppBuilder.

To support authentication through a third-party provider, the ``AUTH_TYPE`` entry needs to be updated with the
desired option like OAuth, OpenID, LDAP, and the lines with references for the chosen option need to have
the comments removed and configured in the ``$AIRFLOW_HOME/webserver_config.py``.

For more details, please refer to
`Security section of FAB documentation <https://flask-appbuilder.readthedocs.io/en/latest/security.html>`_.

SSL
---

SSL can be enabled by providing a certificate and key. Once enabled, be sure to use
"https://" in your browser.

.. code-block:: ini

    [webserver]
    web_server_ssl_cert = <path to cert>
    web_server_ssl_key = <path to key>

Enabling SSL will not automatically change the web server port. If you want to use the
standard port 443, you'll need to configure that too. Be aware that super user privileges
(or cap_net_bind_service on Linux) are required to listen on port 443.

.. code-block:: ini

    # Optionally, set the server to listen on the standard SSL port.
    web_server_port = 443
    base_url = http://<hostname or IP>:443

Enable CeleryExecutor with SSL. Ensure you properly generate client and server
certs and keys.

.. code-block:: ini

    [celery]
    ssl_active = True
    ssl_key = <path to key>
    ssl_cert = <path to cert>
    ssl_cacert = <path to cacert>

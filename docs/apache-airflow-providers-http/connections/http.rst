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



.. _howto/connection:http:

HTTP Connection
===============

The HTTP connection enables connections to HTTP services.

Default Connection IDs
----------------------

The HTTP operators and hooks use ``http_default`` by default.

Authentication
--------------

 .. _auth_basic:

Authenticating via Basic auth
.............................
The simplest way to authenticate is to specify a *Login* and *Password* in the
Connection.

.. image:: /img/connection_username_password.png

By default, when a *Login* or *Password* is provided, the HTTP operators and
Hooks will perform a basic authentication via the
``requests.auth.HTTPBasicAuth`` class.

Authenticating via Headers
..........................
If :ref:`Basic authentication<auth_basic>` is not enough, you can also add
*Headers* to the requests performed by the HTTP operators and Hooks.

Headers can be passed in json format in the *Headers* field:

.. image:: /img/connection_headers.png

.. note:: Login and Password authentication can be used along custom Headers.

Authenticating via Auth class
.............................
For more complex use-cases, you can inject a Auth class into the HTTP operators
and Hooks via the *Auth type* setting. This is particularly useful when you
need token refresh or advanced authentication methods like kerberos, oauth, ...

.. image:: /img/connection_auth_type.png

By default, only `requests Auth classes <https://github.com/psf/requests/blob/main/src/requests/auth.py>`_
are available. But you can install any classes based on ``requests.auth.AuthBase``
into your Airflow instance (via pip install), and then specify those classes in
``extra_auth_types`` :doc:`configuration setting<../configurations-ref>` to
make them available in the Connection UI.

If the Auth class requires more than a *Username* and a *Password*, you can
pass extra keywords arguments with the *Auth kwargs* setting.

Example with the ``HTTPKerberosAuth`` from `requests-kerberos <https://pypi.org/project/requests-kerberos>`_ :

.. image:: /img/connection_auth_kwargs.png

.. tip::

    You probably don't need to write an entire custom HttpOperator or HttpHook
    to customize the connection. Simply extend the ``requests.auth.AuthBase``
    class and configure a Connection with it.

Configuring the Connection
--------------------------

Via the Admin panel
...................

Configuring the Connection via the Airflow Admin panel offers more
possibilities than via :ref:`environment variables<env-variable>`.

Login (optional)
    The login (username) of the http service you would like to connect too.
    If provided, by default, the HttpHook perform a Basic authentication.

Password (optional)
    The password of the http service you would like to connect too.
    If provided, by default, the HttpHook perform a Basic authentication.

Host (optional)
    Specify the entire url or the base of the url for the service.

Port (optional)
    A port number if applicable.

Schema (optional)
    The service type. E.g: http/https.

Auth type (optional)
    Python class used by the HttpHook (and the underlying requests library) to
    authenticate. If provided, the *Login* and *Password* are passed as the two
    first arguments to this class. If *Login* and/or *Password* are provided
    without any Auth type, the HttpHook will by default perform a basic
    authentication via the ``requests.auth.HTTPBasicAuth`` class.

    Extra classes can be added via the ``extra_auth_types``
    :doc:`configuration setting<../configurations-ref>`.

Auth kwargs (optional)
    Extra key-value parameters passed to the Auth type class.

Headers (optional)
    Extra key-value parameters added to the Headers in JSON format.

Extras (optional - deprecated)
    *Deprecated*: Specify headers in json format.

 .. _env-variable:

Via environment variable
........................

When specifying the connection in environment variable you should specify
it using URI syntax.

.. note:: All components of the URI should be **URL-encoded**.

.. code-block:: bash
   :caption: Example:

   export AIRFLOW_CONN_HTTP_DEFAULT='http://username:password@service.com:80/https?headers=header'

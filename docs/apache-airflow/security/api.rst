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

API
===

API Authentication
------------------

Authentication for the API is handled separately to the Web Authentication. The default is to
check the user session:

.. code-block:: ini

    [api]
    auth_backends = airflow.api.auth.backend.session

.. versionchanged:: 1.10.11

    In Airflow <1.10.11, the default setting was to allow all API requests without authentication, but this
    posed security risks for if the Webserver is publicly accessible.

.. versionchanged:: 2.3.0

    In Airflow <2.3.0 this setting was ``auth_backend`` and allowed only one
    value. In 2.3.0 it was changed to support multiple backends that are tried
    in turn.

If you want to check which authentication backends are currently set, you can use ``airflow config get-value api auth_backends``
command as in the example below.

.. code-block:: console

    $ airflow config get-value api auth_backends
    airflow.api.auth.backend.basic_auth

Disable authentication
''''''''''''''''''''''

If you wish to have the experimental API work, and aware of the risks of enabling this without authentication
(or if you have your own authentication layer in front of Airflow) you can set the following in ``airflow.cfg``:

.. code-block:: ini

    [api]
    auth_backends = airflow.api.auth.backend.default

.. note::

    You can only disable authentication for experimental API, not the stable REST API.

See :doc:`../modules_management` for details on how Python and Airflow manage modules.

Kerberos authentication
'''''''''''''''''''''''

Kerberos authentication is currently supported for the API.

To enable Kerberos authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backends = airflow.api.auth.backend.kerberos_auth

    [kerberos]
    keytab = <KEYTAB>

The Kerberos service is configured as ``airflow/fully.qualified.domainname@REALM``. Make sure this
principal exists in the keytab file.

Basic authentication
''''''''''''''''''''

`Basic username password authentication <https://en.wikipedia.org/wiki/Basic_access_authentication>`_ is currently
supported for the API. This works for users created through LDAP login or
within Airflow Metadata DB using password.

To enable basic authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backends = airflow.api.auth.backend.basic_auth

Username and password needs to be base64 encoded and send through the
``Authorization`` HTTP header in the following format:

.. code-block:: text

    Authorization: Basic Base64(username:password)

Here is a sample curl command you can use to validate the setup:

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080/"
    curl -X GET  \
        --user "username:password" \
        "${ENDPOINT_URL}/api/v1/pools"

Note, you can still enable this setting to allow API access through username
password credential even though Airflow webserver might be using another
authentication method. Under this setup, only users created through LDAP or
``airflow users create`` command will be able to pass the API authentication.

Roll your own API authentication
''''''''''''''''''''''''''''''''

Each auth backend is defined as a new Python module. It must have 2 defined methods:

* ``init_app(app: Flask)`` - function invoked when creating a flask application, which allows you to add a new view.
* ``requires_authentication(fn: Callable)`` - a decorator that allows arbitrary code execution before and after or instead of a view function.

and may have one of the following to support API client authorizations used by :ref:`remote mode for CLI <cli-remote>`:

* function ``create_client_session() -> requests.Session``
* attribute ``CLIENT_AUTH: tuple[str, str] | requests.auth.AuthBase | None``

After writing your backend module, provide the fully qualified module name in the ``auth_backends`` key in the ``[api]``
section of ``airflow.cfg``.

Additional options to your auth backend can be configured in ``airflow.cfg``, as a new option.

Enabling CORS
---------------

`Cross-origin resource sharing (CORS) <https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS>`_
is a browser security feature that restricts HTTP requests that are initiated
from scripts running in the browser.

``Access-Control-Allow-Headers``, ``Access-Control-Allow-Methods``, and
``Access-Control-Allow-Origin`` headers can be added by setting values for
``access_control_allow_headers``, ``access_control_allow_methods``, and
``access_control_allow_origins`` options in the ``[api]`` section of the
``airflow.cfg`` file.

.. code-block:: ini

    [api]
    access_control_allow_headers = origin, content-type, accept
    access_control_allow_methods = POST, GET, OPTIONS, DELETE
    access_control_allow_origins = https://exampleclientapp1.com https://exampleclientapp2.com

Page size limit
---------------

To protect against requests that may lead to application instability, the stable API has a limit of items in response.
The default is 100 items, but you can change it using ``maximum_page_limit``  option in ``[api]``
section in the ``airflow.cfg`` file.

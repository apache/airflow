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

.. contents::
  :depth: 1
  :local:

API Authentication
------------------

Authentication for the API is handled separately to the Web Authentication. The default is to
deny all requests:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.deny_all

.. versionchanged:: 1.10.11

    In Airflow <1.10.11, the default setting was to allow all API requests without authentication, but this
    posed security risks for if the Webserver is publicly accessible.

If you wish to have the experimental API work, and aware of the risks of enabling this without authentication
(or if you have your own authentication layer in front of Airflow) you can set the following in ``airflow.cfg``:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.default

Kerberos authentication
'''''''''''''''''''''''

Kerberos authentication is currently supported for the API.

To enable Kerberos authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.kerberos_auth

    [kerberos]
    keytab = <KEYTAB>

The Kerberos service is configured as ``airflow/fully.qualified.domainname@REALM``. Make sure this
principal exists in the keytab file.

Google OpenID authentication
''''''''''''''''''''''''''''

You can also configure
`Google OpenID <https://developers.google.com/identity/protocols/oauth2/openid-connect>`__
for authorization. To enable it, set the following option in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.providers.google.common.auth_backend.google_openid

It is also highly recommended to configure an OAuth2 audience so that the generated tokens are restricted to
use by Airflow only.

.. code-block:: ini

    [api]
    google_oauth2_audience = project-id-random-value.apps.googleusercontent.com

You can also configure the CLI to send request to a remote API instead of making a query to a local database.

.. code-block:: ini

    [cli]
    api_client = airflow.api.client.json_client
    endpoint_url = http://remote-host.example.org/

You can also set up a service account key. If omitted, authorization based on `the Application Default
Credentials <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__
will be used.

.. code-block:: ini

    [cli]
    google_key_path = <KEY_PATH>

You can get the authorization token with the ``gcloud auth print-identity-token`` command. An example request
look like the following.

  .. code-block:: bash

      ENDPOINT_URL="http://locahost:8080/"

      AUDIENCE="project-id-random-value.apps.googleusercontent.com"
      ID_TOKEN="$(gcloud auth print-identity-token "--audience=${AUDIENCE}")"

      curl -X GET  \
          "${ENDPOINT_URL}/api/experimental/pools" \
          -H 'Content-Type: application/json' \
          -H 'Cache-Control: no-cache' \
          -H "Authorization: Bearer ${ID_TOKEN}"

Basic authentication
''''''''''''''''''''

`Basic username password authentication <https://tools.ietf.org/html/rfc7617
https://en.wikipedia.org/wiki/Basic_access_authentication>`_ is currently
supported for the API. This works for users created through LDAP login or
within Airflow Metadata DB using password.

To enable basic authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.basic_auth

Username and password needs to be base64 encoded and send through the
``Authorization`` HTTP header in the following format:

.. code-block:: text

    Authorization: Basic Base64(username:password)

Here is a sample curl command you can use to validate the setup:

.. code-block:: bash

    ENDPOINT_URL="http://locahost:8080/"
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
* attribute ``CLIENT_AUTH: Optional[Union[Tuple[str, str], requests.auth.AuthBase]]``

After writing your backend module, provide the fully qualified module name in the ``auth_backend`` key in the ``[api]``
section of ``airflow.cfg``.

Additional options to your auth backend can be configured in ``airflow.cfg``, as a new option.

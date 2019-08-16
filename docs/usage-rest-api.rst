..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Using the REST API
==================

This document is meant to give an overview of all common tasks while using an REST API.

.. note::

    API endpoints and samples are described in :doc:`rest-api-ref`.

CLI
---

For some functions the cli can use the API. To configure the CLI to use the API when available
configure as follows:

.. code-block:: ini

    [cli]
    api_client = airflow.api.client.json_client
    endpoint_url = http://<WEBSERVER>:<PORT>


Authentication
--------------

Authentication for the API is handled separately to the Web Authentication. The default is to not
require any authentication on the API -- i.e. wide open by default. This is not recommended if your
Airflow webserver is publicly accessible, and you should probably use the ``deny all`` backend:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.deny_all

Two "real" methods for authentication are currently supported for the API.

To enabled Password authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.contrib.auth.backends.password_auth

It's usage is similar to the Password Authentication used for the Web interface.

To enable Kerberos authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.kerberos_auth

    [kerberos]
    keytab = <KEYTAB>

The Kerberos service is configured as ``airflow/fully.qualified.domainname@REALM``. Make sure this
principal exists in the keytab file.

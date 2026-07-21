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

.. _howto/connection:oci:

Oracle Cloud Infrastructure Connection
======================================

The Oracle Cloud Infrastructure connection configures authentication for OCI SDK clients.
It is separate from the :ref:`Oracle Database connection <howto/connection:oracle>`, which
uses the ``oracle`` connection type and the ``oracledb`` driver.

OCI support is optional because the OCI Python SDK has transitive dependencies that Apache Airflow
cannot distribute as required dependencies. Install the provider with the ``oci`` extra before using
this connection or its service hooks:

.. code-block:: bash

    pip install 'apache-airflow-providers-oracle[oci]'

The default connection ID is ``oci_default``.

Service hooks reuse this connection for credentials and region while selecting their own OCI SDK
client class. Each SDK client derives its endpoint from the configured region unless the Dag author
passes a ``service_endpoint`` argument to the hook.

Authentication types
--------------------

API key
    This is the default. Configure ``User OCID`` in Login, the optional private key passphrase in
    Password, and ``tenancy``, ``fingerprint``, ``region``, and ``key_content`` in Extra. A Dag
    author may pass ``key_file`` to the hook instead of storing ``key_content`` in the connection.

Config file
    The Dag author passes ``auth_type="config_file"`` to the hook. The optional ``config_file`` and
    ``profile`` hook arguments default to ``~/.oci/config`` and ``DEFAULT``. A connection ``region``
    overrides the profile region.

Instance principal
    The Dag author passes ``auth_type="instance_principal"`` to the hook. The OCI SDK obtains
    credentials from the compute instance metadata service. The connection ``region`` is optional
    because the signer normally discovers it from instance metadata.

Resource principal
    The Dag author passes ``auth_type="resource_principal"`` to the hook. The OCI SDK obtains
    credentials from the resource principal environment. The connection ``region`` is optional
    when the signer provides one.

File paths, principal authentication, and custom endpoints are intentionally hook arguments rather
than connection fields. This ensures that only Dag authors can make the worker read local files,
use its ambient OCI identity, or send requests to a non-standard endpoint.

Testing the connection
----------------------

The generic OCI connection test calls ``IdentityClient.list_regions`` to validate the configured
credentials independently of any service-specific hook. The identity must have the
``TENANCY_INSPECT`` permission. The connection UI tests the default API key authentication. Dag
authors can call ``test_connection()`` on a hook configured for another authentication type.

Configuring the connection
--------------------------

Login (optional)
    User OCID for ``api_key`` authentication.

Password (optional)
    Private key passphrase for ``api_key`` authentication.

Extra
    A JSON object containing connection-scoped credentials and defaults:

    * ``tenancy``: tenancy OCID for API key authentication.
    * ``fingerprint``: public key fingerprint for API key authentication.
    * ``key_content``: API signing private key content; prefer a secrets backend.
    * ``region``: OCI region identifier, for example ``us-chicago-1``.
    * ``compartment_id``: default compartment OCID used by service operations.

Dag-controlled hook arguments
-----------------------------

``auth_type``
    One of ``api_key``, ``config_file``, ``instance_principal``, or ``resource_principal``.

``key_file``
    API signing private key path for API key authentication. Do not use it together with
    connection ``key_content``.

``config_file`` and ``profile``
    OCI SDK configuration file and profile for ``config_file`` authentication.

``service_endpoint``
    Explicit service endpoint, for example
    ``https://generativeai.us-chicago-1.oci.oraclecloud.com``. Do not append the API version path.

API key example
---------------

.. code-block:: json

    {
      "tenancy": "ocid1.tenancy.oc1..example",
      "fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
      "key_content": "<private key supplied by a secrets backend>",
      "region": "us-chicago-1",
      "compartment_id": "ocid1.compartment.oc1..example"
    }

Use an Airflow secrets backend for production credentials and avoid logging connection extras or
private key material.

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

.. _howto/connection:weaviate:

Weaviate Connection
===================

The `Weaviate <https://weaviate.io/>`__ connection type enables access to Weaviate APIs.

Default Connection IDs
----------------------

Weaviate hook points to ``weaviate_default`` connection by default.

Configuring the Connection
--------------------------

Host (required)
    The host to use for the Weaviate cluster REST and GraphQL API calls. DO NOT include the schema (i.e., http or https).

OIDC Username (optional)
    Username for the OIDC user when OIDC option is to be used for authentication.

OIDC Password (optional)
    Password for the OIDC user when OIDC option is to be used for authentication.

Port (option)
    The port to use for the Weaviate cluster REST and GraphQL API calls.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in the
    connection. All parameters are optional.
    The extras are those parameters that are acceptable in the different authentication methods
    here: `Authentication <https://weaviate-python-client.readthedocs.io/en/stable/weaviate.auth.html>`__

    * If you'd like to use Vectorizers for your class, configure the API keys to use the corresponding
      embedding API. The extras accepts a key ``additional_headers`` containing the dictionary
      of API keys for the embedding API authentication. They are mentioned in a section here:
      `Third party API keys <https://weaviate.io/developers/weaviate/starter-guides/connect#third-party-api-keys>`__

Weaviate API Token (optional)
    Specify your Weaviate API Key to connect when API Key option is to be used for authentication.

Use https (optional)
    Whether to use https for the Weaviate cluster REST and GraphQL API calls.

gRPC host (optional)
    The host to use for the Weaviate cluster gRPC API.

gRPC port (optional)
    The port to use for the Weaviate cluster gRPC API.

Use a secure channel for the underlying gRPC API (optional)
    Whether to use a secure channel for the the Weaviate cluster gRPC API.


Supported Authentication Methods
--------------------------------
* API Key Authentication: This method uses the Weaviate API Key to authenticate the connection. You can either have the
  API key in the ``Weaviate API Token`` field or in the extra field as a dictionary with key ``token`` or ``api_key`` and
  value as the API key.

* Bearer Token Authentication: This method uses the Access Token to authenticate the connection. You need to
  have the Access Token in the extra field as a dictionary with key ``access_token`` and value as the Access Token. Other
  parameters such as ``expires_in`` and ``refresh_token`` are optional.

* Client Credentials Authentication: This method uses the Client Credentials to authenticate the connection. You need to
  have the Client Credentials in the extra field as a dictionary with key ``client_secret`` and value as the Client Credentials.
  The ``scope`` is optional.

* Password Authentication: This method uses the username and password to authenticate the connection. You can specify the
  scope in the extra field as a dictionary with key ``scope`` and value as the scope. The ``scope`` is optional.

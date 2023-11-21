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
    Host URL to connect to the Weaviate cluster.

OIDC Username (optional)
    Username for the OIDC user when OIDC option is to be used for authentication.

OIDC Password (optional)
    Password for the OIDC user when OIDC option is to be used for authentication.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in the
    connection. All parameters are optional.

    * If you'd like to use Vectorizers for your class, configure the API keys to use the corresponding
      embedding API. The extras accepts a key ``additional_headers`` containing the dictionary
      of API keys for the embedding API authentication. They are mentioned in a section here:
      `addtional_headers <https://weaviate.io/developers/academy/zero_to_mvp/hello_weaviate/hands_on#-client-instantiation>__`

Weaviate API Token (optional)
    Specify your Weaviate API Key to connect when API Key option is to be used for authentication.

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

.. _howto/connection:airbyte:

Airbyte Connection
==================
The Airbyte connection type uses the Airbyte API Python SDK to communicate with the server.

Authentication is **optional**: when Client ID and Client Secret are provided, the hook
authenticates via OAuth2 Client Credentials (required for Airbyte Cloud and Enterprise).
When they are omitted, the hook connects without authentication, which is suitable for
Airbyte OSS deployments that do not have auth enabled.

Host (required)
    The fully qualified host URL of the Airbyte server.

    * Airbyte Cloud: ``https://api.airbyte.com/v1/``
    * Airbyte OSS (with auth): ``http://localhost:8000/api/public/v1/``
    * Airbyte OSS (without auth): ``http://localhost:8000/api/v1/``

    Be aware: If you're changing the API path, you must update the value accordingly.

Token URL (optional)
    The prefix for the URL used to create the access token. Only used when
    Client ID and Client Secret are provided.

    * Airbyte Cloud: ``v1/applications/token`` (default value)
    * Airbyte OSS: ``/api/public/v1/applications/token``

    Be aware: If you're changing the API path, you must update the value accordingly.

Client ID (optional)
    The Client ID for OAuth2 authentication.
    Required for Airbyte Cloud and Enterprise deployments.
    You can find this in Settings > Applications in the Airbyte UI.
    Leave blank for Airbyte OSS deployments without auth enabled.

Client Secret (optional)
    The Client Secret for OAuth2 authentication.
    Required for Airbyte Cloud and Enterprise deployments.
    You can find this in Settings > Applications in the Airbyte UI.
    Leave blank for Airbyte OSS deployments without auth enabled.

Extra (optional)
    Specify custom proxies in JSON format.
    Following default requests parameters are taken into account:

    * ``proxies``

    Example: ``{"http": "http://proxy.example.com:8080", "https": "http://proxy.example.com:8080"}``

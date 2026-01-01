
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

.. _howto/connection:github:

GitHub Connection
====================
The GitHub connection provides two authentication mechanisms:
  - Token-based authentication
  - GitHub App authentication

For Token-based authentication, you must provide an access token.
For GitHub App authentication, you must configure the connection's Extras field with the required GitHub App parameters.

Configuring the Connection
--------------------------
Access Token (optional)
    Personal Access token with required permissions.
        - GitHub - Create token - https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token/
        - GitHub Enterprise - Create token - https://docs.github.com/en/enterprise-cloud@latest/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token/

Host (optional)
    Specify the GitHub Enterprise Url (as string) that can be used for GitHub Enterprise
    connection.

    The following Url should be in following format:

    * ``hostname``: Url for Your GitHub Enterprise Deployment.

    .. code-block::

        https://{hostname}/api/v3

GitHub App authentication
--------------------------------

You can authenticate using a GitHub App installation by setting the extra field of your connection, instead of using a token.

- ``key_path``: Path to the private key file used for GitHub App authentication.
- ``app_id``: The application ID.
- ``installation_id``: The ID of the app installation.
- ``token_permissions``: A dictionary of permissions. - Properties of permissions - https://docs.github.com/en/rest/apps/apps?apiVersion=2022-11-28#create-an-installation-access-token-for-an-app

Example "extras" field:

.. code-block:: json

    {
      "key_path": "FAKE_KEY.pem",
      "app_id": "123456s",
      "installation_id": 123456789,
      "token_permissions": {
        "issues":"write",
        "contents":"read"
      }
    }

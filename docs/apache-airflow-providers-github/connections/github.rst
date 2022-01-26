
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

Github Connection
====================
The Github connection type provides connection to a Github or Github Enterprise.

Configuring the Connection
--------------------------
Access Token (required)
    Personal Access token with required permissions.
        - GitHub - Create token - https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token/
        - GitHub Enterprise - Create token - https://docs.github.com/en/enterprise-cloud@latest/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token/

Host (optional)
    Specify the Github Enterprise Url (as string) that can be used for Github Enterprise
    connection.

    The following Url should be in following format:

    * ``hostname``: Url for Your GitHub Enterprise Deployment.

    .. code-block::

        https://{hostname}/api/v3

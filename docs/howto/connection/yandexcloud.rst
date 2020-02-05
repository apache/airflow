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


Yandex.Cloud Connection
================================

The Yandex.Cloud connection type enables the authentication in Yandex.Cloud services.

Authenticating to Yandex.Cloud
---------------------------------

Normally service account keys are used for Yandex.Cloud API authentication.
https://cloud.yandex.com/docs/cli/operations/authentication/service-account

As an alternative to service account key, user OAuth token can be used for authentication.
See the https://cloud.yandex.com/docs/cli/quickstart for obtaining a user OAuth token.

Default Connection IDs
----------------------

All hooks and operators related to Yandex.Cloud use ``yandexcloud_default`` connection by default.

Configuring the Connection
--------------------------

Service account auth JSON
    JSON object as a string like::
        {"id", "...", "service_account_id": "...", "private_key": "..."}

Service account auth JSON file path
    Path to the file containing service account auth JSON.

OAuth Token
    OAuth token as a string.

SSH public key (optional)
    The key will be placed to all created Compute nodes, allowing to have a root shell there.

Folder ID (optional)
    Folder is a entity to separate different projects within the cloud.

    If specified, this ID will be used by default durion creation of nodes and clusters.
    See https://cloud.yandex.com/docs/resource-manager/operations/folder/get-id for details

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

.. _yandex_cloud_connection:

Yandex.Cloud Connection
=======================

The Yandex.Cloud connection type enables the authentication in Yandex.Cloud services.

Configuring the Connection
--------------------------

Service account auth JSON
    JSON object as a string.

    Example: ``{"id": "...", "service_account_id": "...", "private_key": "..."}``

Service account auth JSON file path
    Path to the file containing service account auth JSON.

    Example: ``/home/airflow/authorized_key.json``

OAuth Token
    OAuth token as a string.

    Example: ``y3_Vdheub7w9bIut67GHeL345gfb5GAnd3dZnf08FRbvjeUFvetYiohGvc``

SSH public key (optional)
    The key will be placed to all created Compute nodes, allowing to have a root shell there.

Folder ID (optional)
    Folder is a entity to separate different projects within the cloud.

    If specified, this ID will be used by default during creation of nodes and clusters.

    See https://cloud.yandex.com/docs/resource-manager/operations/folder/get-id for details

Endpoint (optional)
    Set API endpoint

    See https://github.com/yandex-cloud/python-sdk for default

Default Connection IDs
----------------------

All hooks and operators related to Yandex.Cloud use ``yandexcloud_default`` connection by default.

Authenticating to Yandex.Cloud
------------------------------

Using Authorized keys for authorization as service account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before you start, make sure you have `created <https://cloud.yandex.com/en/docs/iam/operations/sa/create>`__
a Yandex Cloud `Service Account <https://cloud.yandex.com/en/docs/iam/concepts/users/service-accounts>`__
with the permissions ``lockbox.viewer`` and ``lockbox.payloadViewer``.

First, you need to create `Authorized key <https://cloud.yandex.com/en/docs/iam/concepts/authorization/key>`__
for your service account and save the generated JSON file with public and private key parts.

Then you need to specify the key in the ``Service account auth JSON`` field.

Alternatively, you can specify the path to JSON file in the ``Service account auth JSON file path`` field.

Using OAuth token for authorization as users account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you need to create `OAuth token <https://cloud.yandex.com/en/docs/iam/concepts/authorization/oauth-token>`__ for user account.
It will looks like ``y3_Vdheub7w9bIut67GHeL345gfb5GAnd3dZnf08FRbvjeUFvetYiohGvc``.

Then you need to specify token in the ``OAuth Token`` field.

Using metadata service
~~~~~~~~~~~~~~~~~~~~~~

If no credentials are specified, the connection will attempt to use
the `metadata service <https://cloud.yandex.com/en/docs/compute/concepts/vm-metadata>`__ for authentication.

To do this, you need to `link <https://cloud.yandex.ru/en/docs/compute/operations/vm-connect/auth-inside-vm>`__
your service account with your VM.

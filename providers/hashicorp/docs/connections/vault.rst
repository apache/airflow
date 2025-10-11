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


.. _howto/connection:vault:

Vault Connection
================

The Vault connection type enables integrations with the Hashicorp vault client.

Default Connection IDs
----------------------

Hooks related to Vault use ``vault_default`` by default.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Schema
    Vault mount point. Default value is ``secret``

Login
    Required when used ``ldap`` or ``userpass`` auth types, and you can use it to pass
    the username for ``token`` auth type and the role id for ``approle`` and ``aws_iam`` auth type.

Password
    Required when used ``ldap``, ``userpass`` or ``token``.

Port
    The port of the Vault host.

Extra
    Specify the extra parameters (as json dictionary) that can be used in Vault
    connection.

    ``auth_type``: Authentication Type for Vault. Default is ``token``. Available values are in
    ('approle', 'aws_iam', 'azure', 'github', 'gcp', 'kubernetes', 'ldap', 'radius', 'token', 'userpass')

    ``auth_mount_point``: It can be used to define mount_point for authentication chosen
    Default depends on the authentication method used.

    ``kv_engine_version``: Selects the version of the engine to run (``1`` or ``2``, default: ``2``).


    ``role_id``: Role ID for Authentication (for ``approle``, ``aws_iam`` auth_types).
    Deprecated, please use connection login instead

    ``kubernetes_role``: Role for Authentication (for ``kubernetes`` auth_type).

    ``kubernetes_jwt_path``: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
    ``/var/run/secrets/kubernetes.io/serviceaccount/token``).

    ``kubernetes_audience``: Optional audience claim to verify in the JWT token (for ``kubernetes`` auth_type).
    Required for Vault 1.21+ to suppress deprecation warnings.

    ``token_path``: path to file containing authentication token to include in requests sent to Vault
    (for ``token`` and ``github`` auth_type).

    ``gcp_key_path``: Path to Google Cloud Service Account key file (JSON)  (for ``gcp`` auth_type).
    Mutually exclusive with gcp_keyfile_dict

    ``gcp_scopes``: Comma-separated string containing OAuth2 scopes (for ``gcp`` auth_type).

    ``azure_tenant_id``: The tenant id for the Azure Active Directory (for ``azure`` auth_type).

    ``azure_resource``: The configured URL for the application registered in Azure Active Directory
    (for ``azure`` auth_type).

    ``radius_host``: Host for radius (for ``radius`` auth_type).

    ``radius_port``: Port for radius (for ``radius`` auth_type).

    ``use_tls``: Whether to use https or http protocol for the connection.

    Example "extras" field:

    .. code-block:: JSON

      {
        "auth_type": "kubernetes",
        "kubernetes_role": "vault_role",
      }

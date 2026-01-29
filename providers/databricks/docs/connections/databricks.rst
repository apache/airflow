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



.. _howto/connection:databricks:

Databricks Connection
==========================

The Databricks connection type enables the Databricks & Databricks SQL Integration.

Authenticating to Databricks
----------------------------

There are several ways to connect to Databricks using Airflow.

1. Use a `Personal Access Token (PAT)
   <https://docs.databricks.com/dev-tools/api/latest/authentication.html>`_
   i.e. add a token to the Airflow connection.
2. Use Databricks login credentials
   i.e. add the username and password used to login to the Databricks account to the Airflow connection.
   Note that username/password authentication is discouraged and not supported for
   :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksSqlOperator`.
3. Using Azure Active Directory (AAD) token generated from Azure Service Principal's ID and secret
   (only on Azure Databricks).  Service principal could be defined as a
   `user inside workspace <https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--api-access-for-service-principals-that-are-azure-databricks-workspace-users-and-admins>`_, or `outside of workspace having Owner or Contributor permissions <https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--api-access-for-service-principals-that-are-not-workspace-users>`_
4. Using Azure Active Directory (AAD) token obtained for `Azure managed identity <https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token>`_,
   when Airflow runs on the VM with assigned managed identity (system-assigned or user-assigned)
5. Using Databricks Service Principal OAuth
   i.e. use OAuth authentication with Service Principal client ID and secret.
   See `Authentication using OAuth for service principals <https://docs.databricks.com/en/dev-tools/authentication-oauth.html>`_.
6. Using Kubernetes `OIDC token federation <https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation>`_ (applicable only when Airflow runs in Kubernetes)
   i.e. automatically fetch JWT tokens from Kubernetes Service Account and exchange them for Databricks OAuth tokens.
   This is the recommended method when Airflow runs in Kubernetes. This method requires no secrets to be stored in the connection.

Default Connection IDs
----------------------

Hooks and operators related to Databricks use ``databricks_default`` by default.

Configuring the Connection
--------------------------

Host (required)
    Specify the Databricks workspace URL

Login (optional)
    * If authentication with *Databricks login credentials* is used then specify the ``username`` used to login to Databricks.
    * If authentication with *Azure Service Principal* is used then specify the ID of the Azure Service Principal
    * If authentication with *PAT* is used then either leave this field empty or use 'token' as login (both work, the only difference is that if login is empty then token will be sent in request header as Bearer token, if login is 'token' then it will be sent using Basic Auth which is allowed by Databricks API, this may be useful if you plan to reuse this connection with e.g. HttpOperator)
    * If authentication with *Databricks Service Principal OAuth* is used then specify the ID of the Service Principal
    * If authentication with *Kubernetes OIDC token federation* is used then specify ``federated_k8s`` as the login

Password (optional)
    * If authentication with *Databricks login credentials* is used then specify the ``password`` used to login to Databricks.
    * If authentication with *Azure Service Principal* is used then specify the secret of the Azure Service Principal
    * If authentication with *PAT* is used, then specify PAT (recommended)
    * If authentication with *Databricks Service Principal OAuth* is used then specify the secret of the Service Principal
    * If authentication with *Kubernetes OIDC token federation* is used then leave this field empty (not required)

Extra (optional)
    Specify the extra parameter (as json dictionary) that can be used in the Databricks connection.

    Following parameter could be used if using the *PAT* authentication method:

    * ``token``: Specify PAT to use. Consider to switch to specification of PAT in the Password field as it's more secure.

    Following parameters are necessary if using authentication with OAuth token for Databricks Service Principal:

    * ``service_principal_oauth``: required boolean flag. If specified as ``true``, use the Client ID and Client Secret as the Username and Password. See `Authentication using OAuth for service principals <https://docs.databricks.com/en/dev-tools/authentication-oauth.html>`_.

    Following parameters are necessary if using authentication with AAD token:

    * ``azure_tenant_id``: ID of the Azure Active Directory tenant
    * ``azure_resource_id``: optional Resource ID of the Azure Databricks workspace (required if Service Principal isn't
      a user inside workspace)
    * ``azure_ad_endpoint``: optional host name of Azure AD endpoint if you're using special `Azure Cloud (GovCloud, China, Germany) <https://docs.microsoft.com/en-us/graph/deployments#app-registration-and-token-service-root-endpoints>`_. The value must contain a protocol. For example: ``https://login.microsoftonline.de``.

    Following parameters are necessary if using authentication with AAD token for Azure managed identity:

    * ``use_azure_managed_identity``: required boolean flag to specify if managed identity needs to be used instead of
      service principal
    * ``use_default_azure_credential``: required boolean flag to specify if the `DefaultAzureCredential` class should be used to retrieve a AAD token. For example, this can be used when authenticating with workload identity within an Azure Kubernetes Service cluster. Note that this option can't be set together with the `use_azure_managed_identity` parameter.
    * ``azure_resource_id``: optional Resource ID of the Azure Databricks workspace (required if managed identity isn't
      a user inside workspace)

    The following parameters are necessary if using authentication with Kubernetes OIDC token federation:

    * ``federated_k8s``: set ``login`` to ``"federated_k8s"`` or add this as extra parameter. When enabled, the hook will fetch a JWT token from the Kubernetes Service Account TokenRequest API and exchange it for a Databricks OAuth token using the `OIDC token exchange API <https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-exchange.html>`_. This authentication method only works when Airflow is running inside a Kubernetes cluster (e.g., AWS EKS, Azure AKS, Google GKE).
    * ``audience``: (optional) the audience value for the Kubernetes JWT token (default: ``https://kubernetes.default.svc``). The hook will call the Kubernetes TokenRequest API to obtain a token with this audience. Note: The default audience works for most use cases. Only customize if required by your specific Databricks federation policy configuration.
    * ``expiration_seconds``: (optional) token expiration in seconds for the Kubernetes JWT (default: 3600).
    * ``k8s_token_path``: (optional) path to the Kubernetes service account token file (default: ``/var/run/secrets/kubernetes.io/serviceaccount/token``). Override this for custom Kubernetes configurations.
    * ``k8s_namespace_path``: (optional) path to the Kubernetes namespace file (default: ``/var/run/secrets/kubernetes.io/serviceaccount/namespace``). Override this for custom Kubernetes configurations.

    **Example configuration for Kubernetes OIDC token federation:**

    Minimal configuration (uses defaults):

    * Set ``Login`` to ``federated_k8s`` or leave empty and add this as extra parameter.
    * Set ``Host`` to your Databricks workspace URL

    With custom configuration:

    * Set ``Login`` to ``federated_k8s`` or leave empty and add this as extra parameter.
    * Set ``Host`` to your Databricks workspace URL
    * Add optional parameters as necessary in ``Extra`` field, e.g. ``{"audience": "custom-audience", "expiration_seconds": 7200, "k8s_token_path": "/custom/path/token", "k8s_namespace_path": "/custom/path/namespace"}``

    **Important Notes:**

    * This method requires no secrets to be stored in the Airflow connection
    * The Kubernetes service account must be configured with appropriate permissions to create TokenRequest resources (typically granted by default in most Kubernetes clusters)
    * The Databricks workspace must have federation policies configured in Databricks Account for the Kubernetes identity provider. This can be Account-level or Service principal-level policies.
    * Both Kubernetes JWT and Databricks OAuth tokens are short-lived and automatically refreshed.

    **Databricks Federation Policy Configuration:**

    Before using Kubernetes OIDC token federation, you must configure a federation policy in your Databricks account. The policy configuration is the same regardless of whether you create it at the account level or for a specific service principal. The difference is only where you create the policy:

    * **Account-level policy:** All users and service principals in the account can use this authentication method
    * **Service principal policy:** Only the specific service principal can use this authentication method (recommended for workloads)

    **Federation Policy Configuration**:

    * **Issuer:** ``https://kubernetes.default.svc``
    * **Audience:** ``https://kubernetes.default.svc`` (default)
    * **Subject:** ``system:serviceaccount:<namespace>:<pod-service-account-name>``
    * **Token Signature Validation (JWKS JSON):** (Optional) Use your Kubernetes cluster's public keys or configure Databricks to fetch them from the well-known endpoint

    **Complete Federation Policy Example:**

    .. code-block:: json

       {
         "oidc_policy": {
           "issuer": "https://kubernetes.default.svc",
           "audiences": ["https://kubernetes.default.svc"],
           "subject": "system:serviceaccount:airflow:airflow-worker",
           "jwks_json": {
             "keys": [{
               "kty": "RSA",
               "e": "AQAB",
               "use": "sig",
               "kid": "your-cluster-key-id",
               "alg": "RS256",
               "n": "your-cluster-public-key-modulus..."
             }]
           }
         }
       }

    Example matching JWT token that Kubernetes will provide:

    .. code-block:: json

       {
         "iss": "https://kubernetes.default.svc",
         "aud": ["https://kubernetes.default.svc"],
         "sub": "system:serviceaccount:airflow:airflow-worker"
       }

    (where ``airflow`` is your Kubernetes namespace and ``airflow-worker`` is your service account name)

    **Troubleshooting Common Issues:**

    * **"Kubernetes service account token not found" error:** This authentication method only works when Airflow is running inside a Kubernetes cluster. Ensure your pods have the service account token mounted (default behavior in Kubernetes).

    * **Permission denied errors:** Verify your Kubernetes service account has permission to create TokenRequest resources. In most clusters, this is granted by default to all service accounts.

    * **Token exchange failures:** Ensure your Databricks federation policy configuration matches the JWT token properties:

      - The Issuer in your policy must match the ``iss`` claim in the JWT (typically ``https://kubernetes.default.svc``)
      - The Audience in your policy must match the ``aud`` claim (set via the ``audience`` connection extra, default is ``https://kubernetes.default.svc``)
      - The Subject pattern in your policy must match the ``sub`` claim (``system:serviceaccount:<namespace>:<service-account-name>``)

    **Note:** For complete step-by-step instructions on creating and configuring federation policies in Databricks, including how to obtain and configure JWKS public keys, see the `Databricks federation policy configuration guide <https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-policy>`_.

    Following parameters could be set when using
    :class:`~airflow.providers.databricks.operators.databricks_sql.DatabricksSqlOperator`:

    * ``http_path``: optional HTTP path of Databricks SQL endpoint or Databricks cluster. See `documentation <https://docs.databricks.com/dev-tools/python-sql-connector.html#get-started>`_.
    * ``session_configuration``: optional map containing Spark session configuration parameters.
    * named internal arguments to the ``Connection`` object from ``databricks-sql-connector`` package.


When specifying the connection using an environment variable you should specify it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://@host-url?token=yourtoken'

   # For Kubernetes OIDC token federation (minimal):
   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://federated_k8s@my-workspace.cloud.databricks.com'

   # For Kubernetes OIDC token federation with custom audience and expiration:
   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://federated_k8s@my-workspace.cloud.databricks.com?audience=custom-audience&expiration_seconds=7200'

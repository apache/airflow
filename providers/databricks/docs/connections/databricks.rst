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
5. Using Databricks-managed Service Principal OAuth (available on all supported clouds)
   i.e. use OAuth authentication with Databricks-managed Service Principal client ID and secret.
   See `Authentication using OAuth for service principals <https://docs.databricks.com/en/dev-tools/authentication-oauth.html>`_.
6. Using Kubernetes `OIDC token federation <https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation>`_ (applicable only when Airflow runs in Kubernetes)
   i.e. automatically fetch JWT tokens from Kubernetes Service Account via projected volume path or TokenRequest API and exchange them for Databricks OAuth tokens.
   This is the recommended method when Airflow runs in Kubernetes. This method requires no secrets to be stored in the connection and eliminates the need
   for token management (no rotation, expiration handling, or credential storage).

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
    * If authentication with *Databricks-managed Service Principal OAuth* is used then specify the ID of the Databricks-managed Service Principal
    * If authentication with *Kubernetes OIDC token federation* is used then specify ``federated_k8s`` as the login

Password (optional)
    * If authentication with *Databricks login credentials* is used then specify the ``password`` used to login to Databricks.
    * If authentication with *Azure Service Principal* is used then specify the secret of the Azure Service Principal
    * If authentication with *PAT* is used, then specify PAT (recommended)
    * If authentication with *Databricks-managed Service Principal OAuth* is used then specify the secret of the Databricks-managed Service Principal
    * If authentication with *Kubernetes OIDC token federation* is used then leave this field empty (not required)

Extra (optional)
    Specify the extra parameter (as json dictionary) that can be used in the Databricks connection.

    Following parameter could be used if using the *PAT* authentication method:

    * ``token``: Specify PAT to use. Consider to switch to specification of PAT in the Password field as it's more secure.

    Following parameters are necessary if using authentication with OAuth token for Databricks-managed Service Principal:

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

    * ``federated_k8s``: set ``login`` to ``"federated_k8s"`` or add this as a boolean flag in extra parameters (``{"federated_k8s": true}``). When enabled, the hook will fetch a JWT token from Kubernetes and exchange it for a Databricks OAuth token using the `OIDC token exchange API <https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-exchange.html>`_. This authentication method only works when Airflow is running inside a Kubernetes cluster (e.g., AWS EKS, Azure AKS, Google GKE).
    * ``client_id``: (required) Databricks service principal client UUID. The service principal must exist in your Databricks account and be assigned to the workspace specified in the Host field.

    **Two methods are supported for obtaining the Kubernetes JWT token:**

    **Method 1: Projected Volume**

    * ``k8s_projected_volume_token_path``: (optional) path to a `Kubernetes projected volume service account token <https://kubernetes.io/docs/concepts/configuration/secret/#projected-volume>`_. When configured, the hook will read the token directly from this file. The token must be configured in your Pod spec with the appropriate audience and expiration. **Important:** The ``k8s_projected_volume_token_path`` must match the full path constructed from your Pod spec: ``{mountPath}/{path}``, where ``mountPath`` is the volume mount path and ``path`` is the ``serviceAccountToken`` path field. For example, if your Pod spec has ``mountPath: /var/run/secrets/databricks`` and ``path: token``, then ``k8s_projected_volume_token_path`` should be ``/var/run/secrets/databricks/token``. This is the recommended method as it's simpler and more efficient (no API calls). See the example Pod configuration below.

    **Method 2: TokenRequest API**

    If ``k8s_projected_volume_token_path`` is not configured, the hook will use the `TokenRequest API <https://kubernetes.io/docs/reference/kubernetes-api/authentication-resources/token-request-v1/>`_ method (dynamic token generation with in-cluster authentication):

    * ``audience``: (optional) the audience value for the Kubernetes JWT token (default: ``https://kubernetes.default.svc``). **Important:** For production deployments, especially when using multiple Kubernetes clusters, it is recommended to use a unique audience per cluster/environment (e.g., ``databricks-prod-airflow``, ``databricks-staging-airflow``) to allow separate Databricks federation policies and proper access control. The default generic audience is only suitable for single-cluster development setups.
    * ``expiration_seconds``: (optional) token expiration in seconds for the Kubernetes JWT (default: 3600).
    * ``k8s_token_path``: (optional) path to the Kubernetes service account token file used to authenticate to the TokenRequest API (default: ``/var/run/secrets/kubernetes.io/serviceaccount/token``). Override this for custom Kubernetes configurations.
    * ``k8s_namespace_path``: (optional) path to the Kubernetes namespace file (default: ``/var/run/secrets/kubernetes.io/serviceaccount/namespace``). Override this for custom Kubernetes configurations.

    **Example configuration for Kubernetes OIDC token federation:**

    **Option A: Using Projected Volume**

    1. Configure your Pod spec with a projected volume:

    .. code-block:: yaml

       apiVersion: v1
       kind: Pod
       metadata:
         name: airflow-worker
       spec:
         serviceAccountName: airflow-worker
         containers:
         - name: airflow
           image: apache/airflow:latest
           volumeMounts:
           - name: databricks-token
             mountPath: /var/run/secrets/databricks
             readOnly: true
         volumes:
         - name: databricks-token
           projected:
             sources:
             - serviceAccountToken:
                 path: token
                 expirationSeconds: 3600
                 audience: databricks-prod-airflow

    2. Configure the Airflow connection:

    * Set ``Login`` to ``federated_k8s``
    * Set ``Host`` to your Databricks workspace URL
    * Set ``Extra`` to include the full token path matching your Pod spec: ``{"k8s_projected_volume_token_path": "/var/run/secrets/databricks/token", "client_id": "your-service-principal-client-uuid"}``

      **Note:** The ``k8s_projected_volume_token_path`` value must match ``{mountPath}/{path}`` from your Pod spec. In the example above:
      - ``mountPath`` is ``/var/run/secrets/databricks`` (from volumeMounts)
      - ``path`` is ``token`` (from ``serviceAccountToken``)
      - Full path is ``/var/run/secrets/databricks/token``

    **Option B: Using TokenRequest API**

    Minimal configuration (uses defaults):

    **Option 1: Using Login field**
    * Set ``Login`` to ``federated_k8s``
    * Set ``Host`` to your Databricks workspace URL
    * Add ``client_id`` in ``Extra`` field, e.g. ``{"client_id": "your-service-principal-client-uuid"}``

    **Option 2: Using Extra field only**
    * Leave ``Login`` empty
    * Set ``Host`` to your Databricks workspace URL
    * Add both ``federated_k8s`` and ``client_id`` in ``Extra`` field, e.g. ``{"federated_k8s": true, "client_id": "your-service-principal-client-uuid"}``

    With custom configuration:

    * Set ``Login`` to ``federated_k8s`` or leave empty and add this as extra parameter.
    * Set ``Host`` to your Databricks workspace URL
    * Add required and optional parameters in ``Extra`` field, e.g. ``{"client_id": "your-service-principal-client-uuid", "audience": "custom-audience", "expiration_seconds": 7200, "k8s_token_path": "/custom/path/token", "k8s_namespace_path": "/custom/path/namespace"}``

    **Best Practices for Multi-Cluster Environments:**

    If you operate multiple Kubernetes clusters (e.g., production, staging, development) accessing the same Databricks workspace, it is recommended to use unique audiences for each cluster:

    * **Production:** ``{"audience": "databricks-prod-airflow"}``
    * **Staging:** ``{"audience": "databricks-staging-airflow"}``
    * **Development:** ``{"audience": "databricks-dev-airflow"}``

    Or include cluster identifiers for even better isolation:

    * ``databricks-prod-eks-us-west-2-airflow``
    * ``databricks-staging-aks-westeurope-airflow``

    **Why unique audiences are important:**

    * **Separate policies:** Create different Databricks federation policies for each cluster with appropriate permissions
    * **Selective revocation:** Revoke access for one cluster without affecting others
    * **Audit trail:** Identify which cluster made specific API calls
    * **Security isolation:** Prevent dev/staging clusters from accessing production resources

    Each cluster requires its own federation policy in Databricks with its corresponding audience value.

    **Choosing Between Methods:**

    * **Projected Volume (Method 1)**: Recommended when you control your Pod specs. Benefits include:

      - No Kubernetes API permissions needed (no TokenRequest API calls)
      - Better performance (simple file read vs API call)
      - Explicit configuration in Pod spec (clearer what audience/expiration is used)
      - More Kubernetes-native approach

    * **TokenRequest API (Method 2)**: Use when:

      - You cannot modify Pod specs
      - You need dynamic audience/expiration configuration
      - You're working with existing deployments that use default service accounts

    **Important Notes:**

    * This method requires no secrets to be stored in the Airflow connection
    * For TokenRequest API method: The TokenRequest API is called to request a token for the ``default`` service account, regardless of which service account the pod is running with. The pod's service account must have appropriate RBAC permissions to create TokenRequest resources for the ``default`` service account.
    * For Projected Volume method: No special Kubernetes permissions needed, just standard service account token projection.
    * The Databricks workspace must have federation policy configured in Databricks Account for the Kubernetes identity provider. **Only service principal-level policies are supported** for Kubernetes OIDC token federation.
    * ``client_id`` is required in the connection extra parameters. Service principal-level Databricks federation must be used because Kubernetes service account tokens do not support custom claims, which are required for account-wide federation.
    * The federation policy must be configured for the specific service principal identified by the ``client_id``. Account-wide federation policies are not supported for Kubernetes OIDC token federation.
    * Both Kubernetes JWT and Databricks OAuth tokens are short-lived and automatically refreshed.

    **Databricks Federation Policy Configuration:**

    Before using Kubernetes OIDC token federation, you must configure a federation policy in your Databricks account:

    * **Service principal policy (required):** Configured in the service principal settings. Only the specific service principal can use this authentication method. The ``client_id`` value must be provided in the connection extra parameters and must match the service principal UUID for which the federation policy is configured.
    * **Account-level policy:** Not supported for Kubernetes OIDC token federation.

    Before configuring the federation policy, you need to understand the token issuer and JWKS requirements.

    **Understanding Token Issuer and JWKS Requirements:**

    The token issuer (``iss`` claim) is determined by your Kubernetes cluster's OIDC configuration, **not** by Airflow. The issuer affects whether JWKS JSON must be provided in the Databricks federation policy:

    * **Standard Kubernetes (self-managed or most managed clusters):** Issuer is ``https://kubernetes.default.svc`` (internal cluster address)

      - **JWKS JSON:** - (Required) You must manually provide your cluster's public keys in the federation policy
      - **Why:** Databricks cannot reach the internal cluster address to fetch keys automatically
      - This is the most common scenario for Airflow deployments

    * **Managed Kubernetes with public OIDC endpoints (AWS EKS with IRSA, Azure AKS with Workload Identity, GKE with Workload Identity):** Issuer is a public URL

      - **JWKS JSON:** - (Optional) Databricks can automatically fetch keys from the public endpoint
      - **Example issuers:**

        - AWS EKS: ``https://oidc.eks.us-west-2.amazonaws.com/id/CLUSTER_ID``
        - Azure AKS: ``https://westus.oic.prod-aks.azure.com/TENANT_ID/CLUSTER_UUID/``
        - GKE: ``https://container.googleapis.com/v1/projects/PROJECT/locations/REGION/clusters/CLUSTER``

    **How to Determine Your Cluster's Issuer:**

    To find out what issuer your cluster uses, inspect an actual JWT token:

    .. code-block:: bash

       # Request a token from your cluster
       token=$(kubectl create token <service-account-name> --audience="test" --duration=600s)

       # Decode the token
       echo $token | cut -d. -f2 | base64 -d | jq .

       # Look for the "iss" (issuer) claim in the decoded token

    **DatabricksFederation Policy Configuration:**

    **Scenario 1: Standard Kubernetes (Most Common) - JWKS JSON Required**

    Federation policy configuration:

    * **Issuer:** (Required) ``https://kubernetes.default.svc``
    * **Audience:**  (Required) ``https://kubernetes.default.svc`` (if using default). Recommended to use unique audience per cluster (e.g., ``databricks-prod-airflow``, ``databricks-staging-airflow``)
    * **Subject:** (Required) ``system:serviceaccount:<namespace>:<service-account-name>``

      * **For Projected Volume method:** Use the actual service account name from your airflow worker pod (e.g., ``system:serviceaccount:airflow:my-sa``)
      * **For TokenRequest API method:** Always use ``system:serviceaccount:<namespace>:default`` because the TokenRequest API is called to request a token for the ``default`` service account, regardless of which service account the airflow worker pod is running with.
    * **JWKS JSON:** - (Required) Your cluster's public signing keys

    Complete example of a Databricks federation policy with JWKS JSON:

    .. code-block:: json

       {
         "oidc_policy": {
           "issuer": "https://kubernetes.default.svc",
           "audiences": ["https://kubernetes.default.svc"],
           "subject": "system:serviceaccount:airflow:default",
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

    **How to obtain your cluster's JWKS:**

    .. code-block:: bash

       kubectl get --raw /openid/v1/jwks

    Copy the entire output and use it as the value for ``jwks_json`` in your Databricks federation policy. Each Kubernetes cluster has unique signing keys, which is what provides the security - only tokens signed by your specific cluster can be validated.

    Example JWT token from standard Kubernetes matching the policy:

    .. code-block:: json

       {
         "iss": "https://kubernetes.default.svc",
         "aud": ["https://kubernetes.default.svc"],
         "sub": "system:serviceaccount:airflow:default"
       }

    **Scenario 2: Managed Kubernetes with Public OIDC - JWKS JSON Optional**

    Federation policy configuration:

    * **Issuer:** (Required) Your cluster's public OIDC issuer URL (e.g., ``https://oidc.eks.us-west-2.amazonaws.com/id/YOUR_CLUSTER_ID`` for EKS)
    * **Audience:**  (Required) The audience value used when requesting tokens. For standard Kubernetes, use ``https://kubernetes.default.svc`` (if using default) or a unique audience per cluster (e.g., ``databricks-prod-airflow``, ``databricks-staging-airflow``)
    * **Subject:** (Required) ``system:serviceaccount:<namespace>:<service-account-name>``

      * **For Projected Volume method:** Use the actual service account name from your airflow worker pod (e.g., ``system:serviceaccount:airflow:my-sa``)
      * **For TokenRequest API method:** Always use ``system:serviceaccount:<namespace>:default`` because the TokenRequest API is called to request a token for the ``default`` service account, regardless of which service account the airflow worker pod is running with.

    Complete example of a Databricks federation policy without JWKS JSON (recommended for public OIDC issuers):

    .. code-block:: json

       {
         "oidc_policy": {
           "issuer": "https://oidc.eks.us-west-2.amazonaws.com/id/YOUR_CLUSTER_ID",
           "audiences": ["https://kubernetes.default.svc"],
           "subject": "system:serviceaccount:airflow:default"
         }
       }

    In this case, Databricks will automatically fetch the public keys from ``<issuer>/.well-known/openid-configuration``.

    **Note:** If your cluster's OIDC issuer is not publicly accessible or you prefer to provide JWKS JSON explicitly, you can include it in the same format as Scenario 1.

    Example JWT token from EKS matching the policy:

    .. code-block:: json

       {
         "iss": "https://oidc.eks.us-west-2.amazonaws.com/id/YOUR_CLUSTER_ID",
         "aud": ["https://kubernetes.default.svc"],
         "sub": "system:serviceaccount:airflow:default"
       }

    **Troubleshooting Common Issues:**

    * **"Kubernetes service account token not found" error:** This authentication method only works when Airflow is running inside a Kubernetes cluster. Ensure your pods have the service account token mounted (default behavior in Kubernetes).

    * **Permission denied errors:** For the TokenRequest API method, verify your pod's service account has RBAC permissions to create TokenRequest resources for the ``default`` service account. By default, service accounts can only create TokenRequest resources for themselves, not for other service accounts. You must explicitly grant these permissions via RBAC policies (ClusterRole and ClusterRoleBinding or Role and RoleBinding).

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

   # For Kubernetes OIDC token federation with projected volume (recommended):
   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://federated_k8s@my-workspace.cloud.databricks.com?k8s_projected_volume_token_path=%2Fvar%2Frun%2Fsecrets%2Fdatabricks%2Ftoken&client_id=your-service-principal-client-uuid'

   # For Kubernetes OIDC token federation with TokenRequest API (minimal):
   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://federated_k8s@my-workspace.cloud.databricks.com?client_id=your-service-principal-client-uuid'

   # For Kubernetes OIDC token federation with TokenRequest API and custom audience:
   export AIRFLOW_CONN_DATABRICKS_DEFAULT='databricks://federated_k8s@my-workspace.cloud.databricks.com?client_id=your-service-principal-client-uuid&audience=custom-audience&expiration_seconds=7200'

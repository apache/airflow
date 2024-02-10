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


Azure Key Vault Backend
^^^^^^^^^^^^^^^^^^^^^^^

To enable the Azure Key Vault as secrets backend, specify
:py:class:`~airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
    backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": "airflow-variables", "vault_url": "https://example-akv-resource-name.vault.azure.net/"}

For client authentication, the ``DefaultAzureCredential`` from the Azure Python SDK is used as credential provider,
which supports service principal, managed identity and user credentials.

For example, to specify a service principal with secret you can set the environment variables ``AZURE_TENANT_ID``, ``AZURE_CLIENT_ID`` and ``AZURE_CLIENT_SECRET``.

Optional lookup
"""""""""""""""

Optionally connections, variables, or config may be looked up exclusive of each other or in any combination.
This will prevent requests being sent to Azure Key Vault for the excluded type.

If you want to look up some and not others in Azure Key Vault you may do so by setting the relevant ``*_prefix`` parameter of the ones to be excluded as ``null``.

For example, if you want to set parameter ``connections_prefix`` to ``"airflow-connections"`` and not look up variables, your configuration file should look like this:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
    backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": null, "vault_url": "https://example-akv-resource-name.vault.azure.net/"}

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_prefix`` as ``airflow-connections``, then for a connection id of ``smtp_default``,
you would want to store your connection at ``airflow-connections-smtp-default``.

The value of the secret must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``airflow-variables``, then for an Variable key of ``hello``,
you would want to store your Variable at ``airflow-variables-hello``.


Authentication
""""""""""""""
There are 3 ways to authenticate Azure Key Vault  backend.

1. Set ``tenant_id``, ``client_id``, ``client_secret`` (using `ClientSecretCredential <https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.clientsecretcredential?view=azure-python>`_)
2. Set ``managed_identity_client_id``, ``workload_identity_tenant_id`` (using `DefaultAzureCredential <https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python>`_ with these arguments)
3. Not providing extra connection configuration for falling back to `DefaultAzureCredential <https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python>`_


Reference
"""""""""

For more details on client authentication refer to the `DefaultAzureCredential Class reference <https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python>`_.

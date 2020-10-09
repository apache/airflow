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
:py:class:`~airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend
    backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": "airflow-variables", "vault_url": "https://example-akv-resource-name.vault.azure.net/"}

For client authentication, the ``DefaultAzureCredential`` from the Azure Python SDK is used as credential provider,
which supports service principal, managed identity and user credentials.


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

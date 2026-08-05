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

.. _howto/connection:openai:

OpenAI Connection
=================

The `OpenAI <https://openai.com/>`__ connection type enables access to OpenAI APIs.

Default Connection IDs
----------------------

OpenAI hook points to ``openai_default`` connection by default.

Configuring the Connection
--------------------------

API Key (required)
    Specify your OpenAI API Key to connect.

Host (optional)
    The host address of the OpenAI instance.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in the
    connection. All parameters are optional.
    This ``extra`` field accepts a nested dictionary with key ``openai_client_kwargs`` as key-value pairs that
    are passed to the `OpenAI client <https://github.com/openai/openai-python/blob/main/src/openai/_client.py>`__
    on instantiation. For example, to set the timeout for the client, you can pass the following dictionary
    as the ``extra`` field:

    .. code-block:: json

        {
          "openai_client_kwargs": {
            "timeout": 10,
            "api_key": "YOUR_API_KEY"
          }
        }

Authentication
--------------

The authentication mechanism is selected with the ``auth_type`` key in the ``extra`` field. It
defaults to ``"api_key"``, which uses the API key from the connection password (backward compatible
with existing connections).

Set ``auth_type`` to ``"workload_identity"`` to authenticate with short-lived identity tokens
instead of a long-lived API key. This exchanges a token minted by your environment's identity
provider, so no API key is stored in the connection. ``identity_provider_id`` and
``service_account_id`` are always required, and the token source is chosen with
``workload_identity_provider``:

* ``"kubernetes"`` -- a Kubernetes service account token read from ``token_file_path`` (defaults to
  the in-cluster path ``/var/run/secrets/kubernetes.io/serviceaccount/token``).
* ``"azure"`` -- an Azure managed identity. Optional keys: ``resource``, ``client_id``,
  ``object_id``, ``msi_res_id``, ``api_version``.
* ``"gcp"`` -- a Google Cloud ID token for the given ``audience``.
* ``"custom"`` -- import ``token_provider`` (a dotted path to a ``Callable[[], str]``) and use it as
  the token source. The callable is imported and invoked in the process that runs the hook, so point
  it only at trusted code. Optional ``token_type`` (``"jwt"`` or ``"id"``, defaults to ``"jwt"``);
  ``token_type`` applies only to the ``custom`` source.

The optional ``refresh_buffer_seconds`` controls how long before expiry the token is refreshed.

For example, to authenticate from a Kubernetes pod:

.. code-block:: json

    {
      "auth_type": "workload_identity",
      "workload_identity_provider": "kubernetes",
      "identity_provider_id": "idp-123",
      "service_account_id": "sa-456"
    }

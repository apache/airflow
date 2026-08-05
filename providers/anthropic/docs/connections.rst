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

.. _howto/connection:anthropic:

Anthropic Connection
====================

The `Anthropic <https://www.anthropic.com/>`__ connection type enables access to the Claude API
through the official Anthropic Python SDK.

Default Connection IDs
----------------------

The Anthropic hook points to the ``anthropic_default`` connection by default.

Configuring the Connection
--------------------------

API Key
    Your Anthropic API key. Required for the first-party API (``platform="anthropic"``),
    and also used as the Microsoft Foundry API key (``platform="foundry"``, together with
    the ``resource`` extra). Not required for the ``bedrock``, ``vertex`` or ``aws``
    platforms, which authenticate through the respective cloud provider's credential chain.

Base URL (optional)
    A custom base URL for the first-party API (for example, an LLM gateway or proxy).

Extra (optional)
    A JSON dictionary of additional parameters. All keys are optional:

    * ``platform`` — which client to build: ``anthropic`` (default), ``bedrock``, ``vertex``,
      ``aws`` or ``foundry``.
    * ``model`` — default model id used whenever an operator or hook call does not pass
      ``model`` (e.g. ``hook.create_message(...)``, ``hook.create_agent(...)``). Set it here
      to change the model without editing Dags; falls back to the provider default
      (``claude-opus-4-8``).
    * ``aws_region`` — AWS region for the ``bedrock`` and ``aws`` platforms.
    * ``project_id`` / ``region`` — GCP project and region for the ``vertex`` platform.
    * ``resource`` — Azure resource name for the ``foundry`` platform.
    * ``anthropic_client_kwargs`` — a nested dictionary forwarded verbatim to the client
      constructor (for example ``timeout``, ``max_retries`` or ``default_headers``).

    For example, to set the client timeout:

    .. code-block:: json

        {
          "anthropic_client_kwargs": {
            "timeout": 30,
            "max_retries": 5
          }
        }

Workload Identity Federation (keyless auth)
-------------------------------------------

For the first-party ``anthropic`` platform you can authenticate with short-lived OIDC
tokens instead of a static API key, via `Workload Identity Federation
<https://platform.claude.com/docs/en/manage-claude/workload-identity-federation>`__.
Two ways:

* **Configured on the connection** — leave the API Key empty and set a ``workload_identity``
  block in ``extra``:

  .. code-block:: json

      {
        "workload_identity": {
          "identity_token_file": "/var/run/secrets/anthropic.com/token",
          "federation_rule_id": "fdrl_...",
          "organization_id": "00000000-0000-0000-0000-000000000000",
          "service_account_id": "svac_...",
          "workspace_id": "wrkspc_..."
        }
      }

* **From the environment** — leave both the API Key and ``extra`` empty; the SDK resolves
  credentials from the standard ``ANTHROPIC_FEDERATION_RULE_ID`` / ``ANTHROPIC_ORGANIZATION_ID``
  / ``ANTHROPIC_SERVICE_ACCOUNT_ID`` / ``ANTHROPIC_IDENTITY_TOKEN_FILE`` environment variables.

.. note::

    ``ANTHROPIC_API_KEY`` in the worker environment **shadows** federation — unset it where
    the worker runs if you intend to use WIF.

.. note::

    The Message Batches API, token counting, the Models API and Managed Agents (including
    :class:`~airflow.providers.anthropic.operators.agent.AnthropicAgentSessionOperator`) are
    served only by the first-party Anthropic API (``platform="anthropic"``) and Claude
    Platform on AWS (``platform="aws"``). They are **not** available on Amazon Bedrock,
    Google Vertex AI or Microsoft Foundry; the hook raises a clear error if you call them on
    those platforms.

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

LLM Retry Policies
===================

.. versionadded:: 3.3.0

The ``LLMRetryPolicy`` uses an LLM to classify task errors and make intelligent
retry decisions. It works with any LLM provider supported by pydantic-ai
(OpenAI, Anthropic, Bedrock, Vertex, Ollama, etc.).

For the core retry policy concepts, see :doc:`apache-airflow:core-concepts/tasks`.

Setup
-----

1. Install the provider with the LLM backend you need:

   .. code-block:: bash

       pip install 'apache-airflow-providers-common-ai[anthropic]'

2. Create a connection (``Admin > Connections``):

   - **Connection Id**: ``pydanticai_default``
   - **Connection Type**: ``Pydantic AI``
   - **Password**: Your API key
   - **Extra**: ``{"model": "anthropic:claude-haiku-4-5-20251001"}``

Usage
-----

.. code-block:: python

    from airflow.providers.common.ai.policies.retry import LLMRetryPolicy
    from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule
    from datetime import timedelta

    llm_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        timeout=30.0,  # max seconds to wait for LLM response
        fallback_rules=[  # used when LLM call fails
            RetryRule(exception=ConnectionError, action=RetryAction.RETRY,
                      retry_delay=timedelta(seconds=10)),
            RetryRule(exception=PermissionError, action=RetryAction.FAIL),
        ],
    )

    @task(retries=5, retry_policy=llm_policy)
    def call_external_api():
        ...

How it works
------------

When a task fails, ``LLMRetryPolicy``:

1. Sends the exception message to the configured LLM
2. The LLM classifies the error into a category (``rate_limit``, ``auth``,
   ``network``, ``data``, ``transient``, ``permanent``)
3. Based on the classification, returns RETRY (with a suggested delay) or FAIL
4. The classification reason is logged in the task logs

If the LLM call fails (provider down, timeout, bad credentials), the policy
falls back to ``fallback_rules`` if configured, or to the task's standard
retry behaviour.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Parameter
     - Default
     - Description
   * - ``llm_conn_id``
     - (required)
     - Airflow connection ID for the LLM provider.
   * - ``model_id``
     - None
     - Override the model from the connection (e.g., ``"openai:gpt-4o-mini"``).
   * - ``instructions``
     - (built-in)
     - Custom system prompt for error classification.
   * - ``fallback_rules``
     - None
     - List of ``RetryRule`` objects used when the LLM call fails.
   * - ``timeout``
     - 30.0
     - Max seconds to wait for the LLM response before falling back.

Local LLM support
-----------------

For environments where exception data must not leave the infrastructure, point
to a local model via Ollama or vLLM:

.. code-block:: python

    LLMRetryPolicy(
        llm_conn_id="ollama_local",  # host=http://localhost:11434
        model_id="ollama:llama3.2",
    )

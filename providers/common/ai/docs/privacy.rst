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

Privacy Guard
=============

The ``common.ai`` provider can sanitize model-bound text with
`Microsoft Presidio <https://github.com/microsoft/presidio>`__ before that text
is sent to the LLM.

This feature is configured per task via the ``input_guard`` parameter on
``LLMOperator``, ``AgentOperator``, and ``LLMFileAnalysisOperator`` (and the
matching TaskFlow decorators).

What v1 protects
----------------

When ``input_guard={"enabled": True}`` is set, the provider sanitizes:

- the prompt and message history sent to the model
- system instructions passed to the pydantic-ai ``Agent``
- normalized text extracted by ``LLMFileAnalysisOperator``

The provider uses Presidio's analyzer and anonymizer to detect entities and
apply one of four operator modes:

- ``redact``
- ``mask``
- ``replace``
- ``hash``

Logging sanitized previews
--------------------------

To help validate a DAG in a real environment, the input guard can log the
**sanitized** outbound text after Presidio has transformed it.

Enable it with:

.. code-block:: python

    input_guard = {
        "enabled": True,
        "mode": "replace",
        "log_sanitized_text": True,
        "log_preview_chars": 240,
    }

When enabled, task logs include lines similar to:

.. code-block:: text

    Input guard sanitized outbound instructions: mode=replace, entities=EMAIL_ADDRESS:1, preview='...'
    Input guard sanitized outbound message[0].parts[0]: mode=replace, entities=CREDIT_CARD:1, EMAIL_ADDRESS:1, preview='...'

Only the sanitized preview is logged. The raw pre-sanitized text is not logged
by the input guard itself.

What v1 does **not** protect
----------------------------

This first iteration only sanitizes **text sent to the model**.

It does **not** sanitize Airflow-local copies of the same data. In particular,
raw values may still appear in:

- Human-in-the-Loop approval state
- HITL review XCom payloads
- task logs, especially at DEBUG level
- durable execution cache files

Local artifact redaction is planned for a later iteration.

Multimodal attachments
----------------------

Presidio sanitizes text, not binary attachments.

When ``input_guard`` is enabled, ``LLMFileAnalysisOperator`` rejects PNG, JPG,
JPEG, and PDF attachments by default if they would be sent as ``BinaryContent``.
To bypass this behavior, set:

.. code-block:: python

    input_guard = {
        "enabled": True,
        "attachment_policy": "allow_unmodified",
    }

Use that mode carefully: the binary payload is sent to the model unchanged.

Example
-------

.. code-block:: python

    from airflow.providers.common.ai.operators.llm import LLMOperator

    LLMOperator(
        task_id="summarize_ticket",
        llm_conn_id="pydanticai_default",
        prompt="Customer email is alice@example.com and card is 4111 1111 1111 1111",
        input_guard={
            "enabled": True,
            "mode": "replace",
            "entities": ("EMAIL_ADDRESS", "CREDIT_CARD"),
        },
    )

See ``airflow.providers.common.ai.example_dags.example_llm_input_guard`` for
runnable operator, agent, and file-analysis examples covering all four
anonymization modes plus sanitized-preview logging.

Limitations
-----------

- Presidio may miss sensitive values or produce false positives.
- This is a privacy guardrail for model input, not an access-control mechanism.
- Tools can still access sensitive data locally before the sanitized text is
  prepared for the model.

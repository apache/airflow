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

.. _howto/operator:llm_file_analysis:

``LLMFileAnalysisOperator`` & ``@task.llm_file_analysis``
=========================================================

Use :class:`~airflow.providers.common.ai.operators.llm_file_analysis.LLMFileAnalysisOperator`
or the ``@task.llm_file_analysis`` decorator to analyze files from object storage
or local storage with a single prompt.

The operator resolves ``file_path`` through
:class:`~airflow.providers.common.compat.sdk.ObjectStoragePath`, reads supported
formats in a read-only manner, injects file metadata and normalized content into
the prompt, and optionally attaches images or PDFs as multimodal inputs.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

Analyze a text-like file or prefix with one prompt:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_file_analysis.py
    :language: python
    :start-after: [START howto_operator_llm_file_analysis_basic]
    :end-before: [END howto_operator_llm_file_analysis_basic]

Directory / Prefix Analysis
---------------------------

Use a directory or object-storage prefix when you want the operator to analyze
multiple files in one request. ``max_files`` bounds how many resolved files are
included in the request, while the size and text limits keep the request safe:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_file_analysis.py
    :language: python
    :start-after: [START howto_operator_llm_file_analysis_prefix]
    :end-before: [END howto_operator_llm_file_analysis_prefix]

.. note::

    Prefix resolution enumerates objects under the supplied path and checks each
    candidate to find files before ``max_files`` is applied. For very large
    object-store prefixes, prefer a more specific path or a narrower prefix to
    avoid expensive listing and stat calls.

Multimodal Analysis
-------------------

Set ``multi_modal=True`` for PNG/JPG/PDF inputs so they are sent as binary
attachments to a vision-capable model:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_file_analysis.py
    :language: python
    :start-after: [START howto_operator_llm_file_analysis_multimodal]
    :end-before: [END howto_operator_llm_file_analysis_multimodal]

Structured Output
-----------------

Set ``output_type`` to a Pydantic ``BaseModel`` when you want a typed response
back from the LLM instead of a plain string:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_file_analysis.py
    :language: python
    :start-after: [START howto_operator_llm_file_analysis_structured]
    :end-before: [END howto_operator_llm_file_analysis_structured]

TaskFlow Decorator
------------------

The ``@task.llm_file_analysis`` decorator wraps the operator. The function
returns the prompt string; file settings are passed to the decorator:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_file_analysis.py
    :language: python
    :start-after: [START howto_decorator_llm_file_analysis]
    :end-before: [END howto_decorator_llm_file_analysis]

Parameters
----------

- ``prompt``: The analysis request to send to the LLM (operator) or the return
  value of the decorated function (decorator).
- ``llm_conn_id``: Airflow connection ID for the LLM provider.
- ``file_path``: File or prefix to analyze.
- ``file_conn_id``: Optional connection ID for the storage backend. Overrides a
  connection embedded in ``file_path``.
- ``multi_modal``: Allow PNG/JPG/PDF inputs as binary attachments. Default ``False``.
- ``max_files``: Maximum number of files included from a prefix. Extra files are
  omitted and noted in the prompt. Default ``20``.
- ``max_file_size_bytes``: Maximum size of any single input file. Default ``5 MiB``.
- ``max_total_size_bytes``: Maximum cumulative size across all resolved files.
  Default ``20 MiB``.
- ``max_text_chars``: Maximum normalized text context sent to the LLM after
  sampling and truncation. Default ``100000``.
- ``sample_rows``: Maximum number of sampled rows or records included for CSV,
  Parquet, and Avro inputs. This controls structural preview depth, while
  ``max_file_size_bytes`` and ``max_total_size_bytes`` are byte-level read
  guards and ``max_text_chars`` is the final prompt-text budget. Default ``10``.
- ``model_id``: Model identifier (e.g. ``"openai:gpt-5"``). Overrides the
  connection's extra field.
- ``system_prompt``: System-level instructions appended to the operator's
  built-in read-only guidance.
- ``output_type``: Expected output type (default: ``str``). Set to a Pydantic
  ``BaseModel`` for structured output.
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai
  ``Agent`` constructor (e.g. ``retries``, ``model_settings``).

Supported Formats
-----------------

- Text-like: ``.log``, ``.json``, ``.csv``, ``.parquet``, ``.avro``
- Multimodal: ``.png``, ``.jpg``, ``.jpeg``, ``.pdf`` when ``multi_modal=True``
- Gzip-compressed text inputs are supported for ``.log.gz``, ``.json.gz``, and
  ``.csv.gz``.
- Gzip is not supported for ``.parquet``, ``.avro``, image, or PDF inputs.

Parquet and Avro readers require their corresponding optional extras:

.. code-block:: bash

    pip install apache-airflow-providers-common-ai[parquet]
    pip install apache-airflow-providers-common-ai[avro]

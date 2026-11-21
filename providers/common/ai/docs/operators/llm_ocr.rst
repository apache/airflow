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

.. _howto/operator:llm_ocr:

OCR of documents: ``LLMOCROperator``
====================================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

Use :class:`~airflow.providers.common.ai.operators.llm_ocr.LLMOCROperator` to read a PDF with a
vendor's OCR model and get its pages back as Markdown. The operator does what an orchestrator is
for: it cuts a document that is too large for one request, sends the parts concurrently on the
task's event loop, and puts the pages back in document order. The vendor's API is left to a small
adapter, selected by the ``"<provider>:<model>"`` prefix of ``model_id``.

The result is one dict per page, in the shape
:class:`~airflow.providers.common.ai.operators.document_loader.DocumentLoaderOperator` returns,
so it feeds the same downstream embedding operators:

.. code-block:: python

    {"text": "# Title\n...", "metadata": {"page_number": 1, "file_name": "2025.pdf", "model": "..."}}

``LLMOCROperator`` requires Airflow 3.2 or later. Cutting a document needs the ``pdf`` extra::

    pip install "apache-airflow-providers-common-ai[pdf]"

Basic usage
-----------

Pass the document as ``source_bytes``: bytes, or their base64 string as an XCom carries them.
``source_bytes`` is a template field, so the output of an upstream task is resolved when the
task runs.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_ocr.py
    :language: python
    :start-after: [START howto_operator_llm_ocr_basic]
    :end-before: [END howto_operator_llm_ocr_basic]

``@task.llm_ocr``
-----------------

The decorated function returns the document; the task returns its pages. The function can be
``async def``: it then downloads the document on the task's event loop and the document never
goes through an XCom. A synchronous function runs in a worker thread.

``output_processor`` is called with the pages and the arguments of the function it names, and
what it returns is the task's result. Use it when the task must say which document its pages
belong to, so mapped results can be joined by key instead of by position:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_ocr.py
    :language: python
    :start-after: [START howto_decorator_llm_ocr]
    :end-before: [END howto_decorator_llm_ocr]

Large documents
---------------

A service may limit the pages of one document. ``pages_per_request`` is the most pages the
operator sends in one request, and ``max_concurrency`` how many requests of the task are in
flight at once. When ``pages_per_request`` is not set, the adapter's own limit applies; a
document within the limit is sent whole. A part the service does not read in time is sent again
as two halves, down to single pages.

The cut does not have to happen in the OCR task. Cutting in a task of its own makes every part a
mapped task instance, with its own retry, and ``max_active_tis_per_dag`` becomes one limit over
the parts of all documents, the way a deployment's quota counts. It costs two more tasks and the
parts travelling through XCom:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_ocr.py
    :language: python
    :start-after: [START howto_decorator_llm_ocr_parts]
    :end-before: [END howto_decorator_llm_ocr_parts]

Errors and retries
------------------

A failed part fails the task with an error that says what to do:

- :class:`~airflow.providers.common.ai.exceptions.OCRBusyError`: the service is throttling or
  failing for now (429, 5xx). ``retry_after`` holds the seconds its ``Retry-After`` header asked
  to wait. Worth a retry.
- :class:`~airflow.providers.common.ai.exceptions.OCRTimeoutError`: a single page was not read
  in time. Worth a retry.
- :class:`~airflow.providers.common.ai.exceptions.OCRError`: the service refused the document
  (4xx). A retry repeats it.

Mistral OCR
-----------

The ``mistral`` adapter speaks the Mistral OCR API (``POST /v1/ocr``). The same contract is
served by Mistral itself, by Mistral Document AI on Azure AI Foundry and by gateways such as
LiteLLM, so the connection says where and the adapter stays the same. It reads a ``pydanticai``
or ``http`` connection:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Field
     - Meaning
   * - Host
     - Base URL of the service: ``https://api.mistral.ai``, or an Azure AI Foundry account
       (``https://<account>.services.ai.azure.com``)
   * - Password
     - API key, sent as a Bearer token
   * - Extra ``ocr_path``
     - Path of the OCR endpoint. ``/v1/ocr`` by default; ``/providers/mistral/azure/ocr`` on
       Azure AI Foundry
   * - Extra ``max_pages_per_request``
     - The most pages the service takes in one document. Unset means no limit; Azure AI Foundry
       refuses a document of more than 30 pages

``image_annotation_schema`` is a JSON Schema the model fills for every picture of a page (its
type, a description, ...). The annotations come back in the pages' ``metadata["images"]``.

Other OCR services
------------------

Another package adds a vendor by implementing
:class:`~airflow.providers.common.ai.ocr.OCRAdapter` and calling
:func:`~airflow.providers.common.ai.ocr.register_adapter` at import time. The adapter's ``name``
is the ``model_id`` prefix that selects it.

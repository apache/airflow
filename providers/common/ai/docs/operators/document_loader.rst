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

.. _howto/operator:document_loader:

``DocumentLoaderOperator``
==========================

Use :class:`~airflow.providers.common.ai.operators.document_loader.DocumentLoaderOperator`
to parse files into ``list[dict(text, metadata)]`` for downstream embedding
pipelines.  The operator bridges Airflow's connectivity layer (hooks that
produce bytes or local files) and the AI embedding layer (operators that
need structured text with metadata).

The operator is **framework-agnostic** — it has no dependency on LlamaIndex,
LangChain, or any other AI framework.

Built-in formats
----------------

``.txt``, ``.md``, ``.csv``, and ``.json`` are handled with zero extra
dependencies:

.. code-block:: python

    from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator

    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="/opt/airflow/data/articles/",
    )

CSV files produce one document per row.  JSON files with a top-level array
produce one document per element; a single JSON object produces one document.

PDF parsing
-----------

Install the ``pdf`` extra to parse PDF files via `pypdf <https://pypdf.readthedocs.io/>`__:

.. code-block:: bash

    pip install apache-airflow-providers-common-ai[pdf]

.. code-block:: python

    load_pdfs = DocumentLoaderOperator(
        task_id="load_pdfs",
        source_path="/opt/airflow/data/reports/*.pdf",
    )

Each page with extractable text becomes a separate document.  Empty pages are
skipped.  The ``page_number`` is included in the document metadata.

DOCX parsing
------------

Install the ``docx`` extra to parse Word documents via
`python-docx <https://python-docx.readthedocs.io/>`__:

.. code-block:: bash

    pip install apache-airflow-providers-common-ai[docx]

.. code-block:: python

    load_word = DocumentLoaderOperator(
        task_id="load_word",
        source_path="/opt/airflow/data/specs/*.docx",
    )

All non-empty paragraphs are concatenated into a single document per file.

Glob patterns and filtering
----------------------------

Pass a glob pattern to ``source_path`` to match multiple files.  Use
``file_extensions`` to limit which files are processed:

.. code-block:: python

    load_filtered = DocumentLoaderOperator(
        task_id="load_filtered",
        source_path="/opt/airflow/data/mixed/*",
        file_extensions=[".pdf", ".txt"],
    )

Composing with downstream operators
------------------------------------

The output format (``list[dict(text, metadata)]``) is designed to feed
directly into embedding operators.  For example, with the LlamaIndex
``EmbeddingOperator``:

.. code-block:: python

    load = DocumentLoaderOperator(
        task_id="load",
        source_path="/data/docs/*.pdf",
    )

    embed = EmbeddingOperator(
        task_id="embed",
        documents="{{ ti.xcom_pull(task_ids='load') }}",
        llm_conn_id="openai_default",
    )

    load >> embed

Composing with Airflow providers
---------------------------------

Use any Airflow provider to download files, then parse them with
``DocumentLoaderOperator``:

.. code-block:: python

    from airflow.providers.amazon.aws.transfers.s3_to_local import S3ToLocalFilesystemOperator

    download = S3ToLocalFilesystemOperator(
        task_id="download",
        bucket_name="my-bucket",
        key="documents/report.pdf",
        local_path="/tmp/report.pdf",
    )

    load = DocumentLoaderOperator(
        task_id="load",
        source_path="/tmp/report.pdf",
    )

    download >> load

For **structured API data** (Salesforce SOQL results, database query exports),
a ``@task`` that maps fields to text and metadata is more appropriate than
``DocumentLoaderOperator``, which is designed for binary file parsing:

.. code-block:: python

    @task
    def transform_cases(records: list[dict]) -> list[dict]:
        return [
            {
                "text": f"{r['Subject']}\n\n{r['Description']}",
                "metadata": {"case_id": r["Id"], "source": "salesforce"},
            }
            for r in records
        ]

Loading from bytes
------------------

When upstream tasks pass file content via XCom, use ``source_bytes`` with
an explicit ``file_type``:

.. code-block:: python

    load = DocumentLoaderOperator(
        task_id="load",
        source_bytes="{{ ti.xcom_pull(task_ids='fetch_file') }}",
        file_type=".pdf",
    )

Parameters
----------

- ``source_path``: Local file path, directory, or glob pattern.
  Mutually exclusive with ``source_bytes``.
- ``source_bytes``: Raw file bytes from XCom.  Requires ``file_type``.
  Mutually exclusive with ``source_path``.
- ``file_type``: File extension hint (e.g. ``".pdf"``).  Required with
  ``source_bytes``.  Optional with ``source_path`` to override
  auto-detection.
- ``parser``: Parsing backend.  ``"auto"`` (default) selects from the file
  extension.  Set explicitly to force a specific backend (e.g. ``"text"``
  to treat an unknown extension as plain text).
- ``file_extensions``: Filter which files to process when ``source_path``
  matches multiple files.
- ``metadata_fields``: Extra key-value pairs merged into every document's
  metadata dict.

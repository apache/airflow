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
pipelines. The operator bridges Airflow's connectivity layer (hooks that
produce bytes or local files) and the AI embedding layer (operators that
need structured text with metadata).

The operator is **framework-agnostic** -- it has no dependency on LlamaIndex,
LangChain, or any other AI framework.

Basic usage
-----------

``.txt``, ``.md``, ``.csv``, and ``.json`` are handled with zero extra
dependencies:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_document_loader.py
    :language: python
    :start-after: [START howto_operator_document_loader_basic]
    :end-before: [END howto_operator_document_loader_basic]

CSV files produce one document per row, with empty cells skipped. JSON files
with a top-level array produce one document per element; a single JSON object
produces one document. By default each dict is flattened into ``"key: value,
key: value"`` text so the embedding sees content tokens rather than JSON
syntax (see the ``json_text_field`` section below for the structured variant).

PDF parsing
-----------

Install the ``pdf`` extra to parse PDF files via
`pypdf <https://pypdf.readthedocs.io/>`__::

    pip install apache-airflow-providers-common-ai[pdf]

Each page with extractable text becomes a separate document. Empty pages are
skipped. ``page_number`` is included in the document metadata.

DOCX parsing
------------

Install the ``docx`` extra to parse Word documents via
`python-docx <https://python-docx.readthedocs.io/>`__::

    pip install apache-airflow-providers-common-ai[docx]

All non-empty paragraphs are concatenated into a single document per file.

.. note::

   DOCX extraction reads paragraph text only. Tables, headers, footers, and
   footnotes are not included. For richer DOCX parsing, use a dedicated
   extraction tool (``Unstructured``, ``docling``) as a custom parser
   backend.

Directory mode and filtering
----------------------------

Point ``source_path`` at a directory or pass a glob pattern (``**`` enables
recursive matching). Combine with ``file_extensions`` to scope which files
are processed:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_document_loader.py
    :language: python
    :start-after: [START howto_operator_document_loader_directory]
    :end-before: [END howto_operator_document_loader_directory]

Directory-mode behavior when ``file_extensions`` is omitted:

- Files whose name starts with a ``.`` (``.DS_Store``, editor swap files,
  ``.gitkeep``, ...) are silently ignored.
- Files whose extension is not in the built-in dispatch map are skipped
  with a warning rather than crashing the operator. A glob pattern that
  matches an unknown extension is treated as intentional and parsed via
  the explicit ``parser`` argument.

Loading from bytes
------------------

When upstream tasks produce file content as bytes (S3, GCS, HTTP, etc.),
pass them via ``source_bytes`` and tell the operator how to interpret them
with ``file_type``. ``source_bytes`` is not a template field because Jinja
would render ``bytes`` as their ``repr`` text, which would break binary
parsing:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_document_loader.py
    :language: python
    :start-after: [START howto_operator_document_loader_bytes]
    :end-before: [END howto_operator_document_loader_bytes]

PDF and DOCX bytes are parsed via an in-memory stream -- no temporary files
on disk.

Structured JSON ingestion
-------------------------

For arrays of records where one field is the body and the rest are metadata
(article ingestion, ticket exports, ...), set ``json_text_field`` to the key
that holds the text. Every other key on the same item lands in ``metadata``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_document_loader.py
    :language: python
    :start-after: [START howto_operator_document_loader_json_field]
    :end-before: [END howto_operator_document_loader_json_field]

For **arbitrary API data** (Salesforce SOQL results, database query exports),
a ``@task`` that maps fields to text and metadata is still appropriate when
the field shape is more complex than what ``json_text_field`` covers:

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

No chunking
-----------

The operator parses files into documents; it does **not** split them into
fixed-size chunks. The right chunking strategy depends on the embedding
model and is intentionally left to a downstream text-splitter or embedding
operator (LlamaIndex's ``LlamaIndexEmbeddingOperator``, LangChain's text splitters,
...).

Format coverage roadmap
-----------------------

The current built-in dispatch covers ``.txt``, ``.md``, ``.csv``, ``.json``,
``.pdf``, ``.docx``. Additional formats are deferred to follow-ups, each
gated behind its own extra so users only install what they need:

- ``.pptx`` via ``python-pptx``
- ``.epub`` via ``ebooklib``
- ``.xlsx`` via ``openpyxl``
- ``.html`` / ``.htm`` via ``beautifulsoup4``
- Image OCR (``.png`` / ``.jpg``) via ``pytesseract``
- Audio transcription via a model call (``LLMOperator`` or ``AgentOperator``
  is a better fit for transcription than this parser)

For anything not in the dispatch map, set ``parser`` explicitly (``"text"``
to read as plain text) or write the parser inline in a ``@task`` that calls
``DocumentLoaderOperator`` with ``source_bytes`` for known formats.

Composing with downstream embedding operators
---------------------------------------------

The output format (``list[dict(text, metadata)]``) is designed to feed
directly into embedding operators. With LlamaIndex's ``LlamaIndexEmbeddingOperator``:

.. code-block:: python

    load = DocumentLoaderOperator(
        task_id="load",
        source_path="/data/docs/*.pdf",
    )

    embed = LlamaIndexEmbeddingOperator(
        task_id="embed",
        documents="{{ ti.xcom_pull(task_ids='load') }}",
        llm_conn_id="openai_default",
    )

    load >> embed

Cloud storage URIs
------------------

``source_path`` accepts any URI that
:class:`~airflow.sdk.ObjectStoragePath` resolves via fsspec
(``s3://``, ``gs://``, ``azure://``, ``file://``, ...). Point it at a
single object or a directory; cross-directory globs in cloud URIs are not
supported in this version.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_document_loader.py
    :language: python
    :start-after: [START howto_operator_document_loader_cloud_uri]
    :end-before: [END howto_operator_document_loader_cloud_uri]

Use ``source_conn_id`` to point at the Airflow connection that holds the
cloud credentials (``aws_default``, ``google_cloud_default``, ...). For
single-file URIs, ``source_conn_id`` works the same way.

If you'd rather download the file with a dedicated provider operator
first (e.g. to get retry semantics specific to that storage), the
download-then-parse pattern still works:

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

Non-UTF-8 inputs
----------------

The text parsers (``.txt`` / ``.md`` / ``.csv`` / ``.json``) and the bytes
path default to UTF-8. To handle Windows-1252 CSVs, files with a leading
``utf-8-sig`` byte-order mark, or any other encoding, set the ``encoding``
parameter on the operator (and optionally ``encoding_errors="replace"`` to
tolerate mixed-encoding sources at the cost of some character loss). A
failed decode includes the offending file path in the error so
directory-mode runs are easy to diagnose.

Metadata precedence
-------------------

Auto-extracted metadata keys -- ``file_name``, ``file_path``, ``row_index``,
``item_index``, ``page_number`` -- take precedence over keys with the same
name in ``metadata_fields``. ``metadata_fields`` fills gaps; it never
overwrites the auto-extracted shape.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``source_path``
     - Local file, directory, or glob pattern, **or** a storage URI
       (``s3://``, ``gs://``, ``azure://``, ``file://``) resolved via
       :class:`~airflow.sdk.ObjectStoragePath`. ``**`` is recursive for
       local globs; cross-directory globs in cloud URIs are not supported.
       Mutually exclusive with ``source_bytes``.
   * - ``source_conn_id``
     - Airflow connection ID for the cloud-storage credentials used by
       ``ObjectStoragePath`` (``aws_default``, ``google_cloud_default``,
       ...). Ignored for local paths.
   * - ``source_bytes``
     - Raw file bytes from XCom. Requires ``file_type``. Mutually exclusive
       with ``source_path``. Not a template field (bytes don't survive Jinja).
   * - ``file_type``
     - File extension hint (e.g. ``".pdf"``). Required with ``source_bytes``;
       optional with ``source_path`` to override auto-detection.
   * - ``parser``
     - Parsing backend. ``"auto"`` (default) picks from the file extension.
       Set explicitly to force a backend (e.g. ``"text"`` to treat an
       unknown extension as plain text).
   * - ``file_extensions``
     - Filter for ``source_path`` directory or glob. When omitted in
       directory mode, files whose name starts with a ``.`` are ignored
       and unknown-extension files are skipped with a warning.
   * - ``metadata_fields``
     - Extra key-value pairs merged into every document's metadata. Does
       not override auto-extracted keys.
   * - ``encoding``
     - Text encoding for the bytes path and ``.txt`` / ``.md`` / ``.csv`` /
       ``.json`` files. Defaults to ``"utf-8"``.
   * - ``encoding_errors``
     - How decode errors are handled (``"strict"`` / ``"replace"`` /
       ``"ignore"``). Defaults to ``"strict"``.
   * - ``json_text_field``
     - When parsing JSON, treat this key as the embedding text; every other
       key on the same item lands in ``metadata``. When unset, dicts are
       flattened to ``"k: v, k: v"`` so the embedding sees content tokens
       rather than JSON syntax.

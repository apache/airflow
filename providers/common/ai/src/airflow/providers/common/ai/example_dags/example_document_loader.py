# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAGs demonstrating DocumentLoaderOperator usage patterns.

Each DAG covers a single pattern. The hook docs reference these via
``.. exampleinclude::`` so the runnable snippets stay in sync.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator
from airflow.providers.common.compat.sdk import dag, task


# [START howto_operator_document_loader_basic]
@dag(schedule=None, tags=["example"])
def example_document_loader_basic():
    """Parse a single local file -- the operator infers the format from the suffix."""

    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="/opt/airflow/data/articles/sample.md",
    )

    @task
    def count_chunks(docs: list[dict]) -> int:
        return len(docs)

    count_chunks(load_docs.output)


# [END howto_operator_document_loader_basic]

example_document_loader_basic()


# [START howto_operator_document_loader_directory]
@dag(schedule=None, tags=["example"])
def example_document_loader_directory():
    """Walk a directory recursively, only picking up PDFs and Markdown."""

    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        # `**` matches across subdirectories thanks to glob's recursive mode.
        source_path="/opt/airflow/data/library/**/*",
        file_extensions=[".pdf", ".md"],
        metadata_fields={"corpus": "library_v3"},
    )

    @task
    def summarise(docs: list[dict]) -> dict:
        return {
            "files": len({d["metadata"]["file_path"] for d in docs}),
            "chunks": len(docs),
        }

    summarise(load_docs.output)


# [END howto_operator_document_loader_directory]

example_document_loader_directory()


# [START howto_operator_document_loader_bytes]
@dag(schedule=None, tags=["example"])
def example_document_loader_bytes():
    """Feed raw bytes from an upstream hook (e.g. an S3 download) into the parser."""

    @task
    def fetch_pdf_bytes() -> bytes:
        # In real use this would be an S3Hook.read_key, a GCSHook.download_as_bytes,
        # or any other byte-producing call.
        return b"%PDF-1.4 ..."

    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_bytes=fetch_pdf_bytes(),
        file_type=".pdf",
        metadata_fields={"corpus": "uploads"},
    )

    load_docs


# [END howto_operator_document_loader_bytes]

example_document_loader_bytes()


# [START howto_operator_document_loader_json_field]
@dag(schedule=None, tags=["example"])
def example_document_loader_json_field():
    """Read an array of records, embedding only the ``body`` field per item.

    Every other key (``title``, ``author``, ``published_at``, ...) lands in
    ``metadata`` so it stays available for filtering or display.
    """

    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="/opt/airflow/data/articles.json",
        json_text_field="body",
    )

    load_docs


# [END howto_operator_document_loader_json_field]

example_document_loader_json_field()


# [START howto_operator_document_loader_cloud_uri]
@dag(schedule=None, tags=["example"])
def example_document_loader_cloud_uri():
    """Read PDFs directly from S3 -- no separate download step."""

    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="s3://my-bucket/reports/",
        source_conn_id="aws_default",
        file_extensions=[".pdf"],
    )

    load_docs


# [END howto_operator_document_loader_cloud_uri]

example_document_loader_cloud_uri()

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
"""Example DAGs demonstrating ``LLMOCROperator`` and ``@task.llm_ocr`` usage patterns.

Each DAG covers a single pattern. The operator docs reference these via
``.. exampleinclude::`` so the runnable snippets stay in sync.
"""

from __future__ import annotations

import base64
from pathlib import Path

from airflow.providers.common.ai.ocr.pdf import split_pdf
from airflow.providers.common.ai.operators.llm_ocr import LLMOCROperator
from airflow.providers.common.compat.sdk import dag, task

DOCUMENTS = ["/opt/airflow/data/contracts/2025.pdf", "/opt/airflow/data/contracts/2026.pdf"]


# [START howto_operator_llm_ocr_basic]
@dag(schedule=None, tags=["example"])
def example_llm_ocr_basic():
    """Read one PDF an upstream task produced as base64."""

    @task
    def load_document() -> str:
        return base64.b64encode(Path(DOCUMENTS[0]).read_bytes()).decode()

    ocr = LLMOCROperator(
        task_id="ocr",
        source_bytes=load_document(),
        llm_conn_id="mistral_default",
        model_id="mistral:mistral-ocr-latest",
        file_name="2025.pdf",
    )

    @task
    def count_pages(pages: list[dict]) -> int:
        return len(pages)

    count_pages(ocr.output)


# [END howto_operator_llm_ocr_basic]

example_llm_ocr_basic()


# [START howto_decorator_llm_ocr]
@dag(schedule=None, tags=["example"])
def example_llm_ocr_decorator():
    """One mapped task per document: each cuts its document and sends 4 parts at once."""

    def pages_of_document(pages: list[dict], path: str) -> dict:
        """Return the pages with the document they are of."""
        return {"path": path, "pages": pages}

    @task.llm_ocr(
        llm_conn_id="mistral_default",
        model_id="mistral:mistral-ocr-latest",
        pages_per_request=16,
        max_concurrency=4,
        output_processor=pages_of_document,
    )
    def ocr_document(path: str) -> bytes:
        return Path(path).read_bytes()

    @task
    def count_pages(documents: list[dict]) -> dict[str, int]:
        return {document["path"]: len(document["pages"]) for document in documents}

    count_pages(ocr_document.expand(path=DOCUMENTS))


# [END howto_decorator_llm_ocr]

example_llm_ocr_decorator()


# [START howto_decorator_llm_ocr_parts]
@dag(schedule=None, tags=["example"])
def example_llm_ocr_parts():
    """Cut the document in a task of its own: every part is a mapped task with its own retry."""

    @task
    def split_document(path: str) -> list[dict]:
        parts = split_pdf(Path(path).read_bytes(), pages_per_part=16)
        return [
            {"part": number, "content": base64.b64encode(part).decode()} for number, part in enumerate(parts)
        ]

    def pages_of_part(pages: list[dict], part: dict) -> dict:
        """Return the pages with the part they are of."""
        return {"part": part["part"], "pages": pages}

    @task.llm_ocr(
        llm_conn_id="mistral_default",
        model_id="mistral:mistral-ocr-latest",
        output_processor=pages_of_part,
        max_active_tis_per_dag=8,
    )
    def ocr_part(part: dict) -> str:
        # The part fits in one request: it is sent as it is.
        return part["content"]

    @task
    def join_parts(parts: list[dict]) -> list[dict]:
        return [page for part in sorted(parts, key=lambda part: part["part"]) for page in part["pages"]]

    join_parts(ocr_part.expand(part=split_document(DOCUMENTS[0])))


# [END howto_decorator_llm_ocr_parts]

example_llm_ocr_parts()

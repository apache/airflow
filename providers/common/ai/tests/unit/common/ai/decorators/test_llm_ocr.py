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
from __future__ import annotations

import asyncio
import base64
import threading
from unittest.mock import patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.decorators.llm_ocr import _LLMOCRDecoratedOperator, llm_ocr_task
from airflow.providers.common.ai.ocr import dispatch
from airflow.providers.common.ai.ocr.base import OCRAdapter, OCRPage, OCRResult
from airflow.providers.common.ai.operators import llm_ocr

CONN = Connection(conn_id="ocr", conn_type="http", host="https://ocr.example", password="key")


class LineAdapter(OCRAdapter):
    """Reads a document of text lines: every line is a page."""

    name = "lines"
    conn_types = frozenset({"http"})

    async def ocr(
        self, document, *, mime_type="application/pdf", include_images=False, image_annotation_schema=None
    ):
        lines = document.decode().splitlines()
        return OCRResult(pages=[OCRPage(index, line) for index, line in enumerate(lines)], model=self.model)


@pytest.fixture(autouse=True)
def _adapters(monkeypatch):
    monkeypatch.setattr(dispatch, "_REGISTERED_ADAPTERS", {LineAdapter.name: LineAdapter})


def run(python_callable, **kwargs) -> object:
    task = _LLMOCRDecoratedOperator(
        task_id="ocr", python_callable=python_callable, llm_conn_id="ocr", model_id="lines:model", **kwargs
    )
    with (
        patch.object(llm_ocr, "get_async_connection", autospec=True, return_value=CONN),
        patch.object(llm_ocr, "get_async_extra_dejson", autospec=True, return_value={}),
    ):
        # Not an empty context: context_merge() only adds the arguments to one that holds something.
        return asyncio.run(task.aexecute({"run_id": "manual"}))


class TestLLMOCRDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMOCRDecoratedOperator.custom_operator_name == "@task.llm_ocr"

    def test_the_document_is_not_a_template_field(self):
        assert "source_bytes" not in _LLMOCRDecoratedOperator.template_fields
        assert {"op_args", "op_kwargs", "llm_conn_id", "model_id"} <= set(
            _LLMOCRDecoratedOperator.template_fields
        )

    def test_a_synchronous_function_and_the_output_processor_run_off_the_event_loop(self):
        threads = {}

        def document() -> bytes:
            threads["callable"] = threading.current_thread()
            return b"one"

        def processor(pages: list[dict]) -> int:
            threads["processor"] = threading.current_thread()
            return len(pages)

        assert run(document, output_processor=processor) == 1
        assert threading.main_thread() not in threads.values()

    def test_the_decorator_builds_a_decorated_ocr_task(self):
        @llm_ocr_task(llm_conn_id="ocr", model_id="lines:model")
        def document() -> bytes:
            return b"one"

        assert document.operator_class is _LLMOCRDecoratedOperator

    def test_the_function_returns_the_document_and_the_task_its_pages(self):
        def document(lines: int) -> bytes:
            return "\n".join(f"line {number}" for number in range(lines)).encode()

        pages = run(document, op_kwargs={"lines": 3})

        assert [page["text"] for page in pages] == ["line 0", "line 1", "line 2"]
        assert [page["metadata"]["page_number"] for page in pages] == [1, 2, 3]

    def test_an_async_function_is_awaited_for_the_document(self):
        async def document() -> str:
            return base64.b64encode(b"one\ntwo").decode()

        pages = run(document)

        assert [page["text"] for page in pages] == ["one", "two"]

    @pytest.mark.parametrize("returned", [None, b"", "", {"content": "..."}])
    def test_a_function_that_returns_no_document_fails_the_task(self, returned):
        with pytest.raises(TypeError, match="must return the document"):
            run(lambda: returned)

    def test_the_output_processor_returns_the_result_from_the_pages_and_the_arguments_it_names(self):
        def content(document: dict) -> bytes:
            return document["content"]

        def with_name(pages: list[dict], document: dict) -> dict:
            return {"name": document["name"], "pages": len(pages)}

        result = run(
            content,
            op_kwargs={"document": {"name": "rules.pdf", "content": b"one\ntwo\nthree"}},
            output_processor=with_name,
        )

        assert result == {"name": "rules.pdf", "pages": 3}

    def test_the_output_processor_also_gets_an_argument_that_was_passed_by_position(self):
        def content(document: dict) -> bytes:
            return document["content"]

        result = run(
            content,
            op_args=[{"name": "rules.pdf", "content": b"one\ntwo"}],
            output_processor=lambda pages, document: (document["name"], len(pages)),
        )

        assert result == ("rules.pdf", 2)

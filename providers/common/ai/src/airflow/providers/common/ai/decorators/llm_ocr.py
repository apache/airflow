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
"""
TaskFlow decorator for ``@task.llm_ocr``.

The user writes a function that **returns the document to read**; the task returns its pages.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.llm_ocr import LLMOCROperator
from airflow.providers.common.compat.sdk import (
    SET_DURING_EXECUTION,
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    determine_kwargs,
    task_decorator_factory,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


class _LLMOCRDecoratedOperator(DecoratedOperator, LLMOCROperator):
    """
    Wraps a callable that returns the document to read.

    The user function is called at execution time and returns the document, as bytes or as their
    base64 string; an ``async def`` function is awaited, so it can download the document on the
    task's event loop. All other parameters (``llm_conn_id``, ``model_id``,
    ``pages_per_request``, ...) are passed through to
    :class:`~airflow.providers.common.ai.operators.llm_ocr.LLMOCROperator`.

    :param python_callable: A reference to a callable that returns the document.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    :param output_processor: Called with the pages and the arguments of the callable it names
        (``document``, ``ti``, ...); what it returns is the task's result. For a task that must
        return more than the pages, such as which document they belong to. It runs on the
        event loop: no I/O in it.
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *LLMOCROperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.llm_ocr"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        output_processor: Callable[..., Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            source_bytes=SET_DURING_EXECUTION,
            **kwargs,
        )
        self.output_processor = output_processor

    async def aexecute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        # The arguments by name, the positional ones too: output_processor asks for them by name.
        arguments = inspect.signature(self.python_callable).bind(*self.op_args, **kwargs).arguments

        source = self.python_callable(*self.op_args, **kwargs)
        if inspect.isawaitable(source):
            source = await source
        if not isinstance(source, (bytes, str)) or not source:
            raise TypeError(
                "The @task.llm_ocr callable must return the document as non-empty bytes or their "
                f"base64 string, got {type(source).__name__}"
            )
        self.source_bytes = source

        pages = await LLMOCROperator.aexecute(self, context)
        if self.output_processor is None:
            return pages
        context_merge(context, arguments)
        return self.output_processor(pages, **determine_kwargs(self.output_processor, (pages,), context))


def llm_ocr_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that returns a document into a task that reads it with an OCR model.

    The function body builds or downloads the document (it can use Airflow context, XCom, etc.,
    and can be ``async def``). The decorator cuts a large document into requests, sends them
    concurrently and returns the pages in order, see
    :class:`~airflow.providers.common.ai.operators.llm_ocr.LLMOCROperator`.

    Usage::

        @task.llm_ocr(llm_conn_id="mistral_default", model_id="mistral:mistral-ocr-latest")
        def read_contract(path: str) -> bytes:
            return Path(path).read_bytes()

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LLMOCRDecoratedOperator,
        **kwargs,
    )

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
"""Operator for analyzing files with LLMs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.utils.file_analysis import build_file_analysis_request
from airflow.providers.common.ai.utils.logging import log_run_summary

if TYPE_CHECKING:
    from pydantic_ai import Agent

    from airflow.sdk import Context


class LLMFileAnalysisOperator(LLMOperator):
    """
    Analyze files from object storage or local storage using a single LLM call.

    The operator resolves ``file_path`` via
    :class:`~airflow.providers.common.compat.sdk.ObjectStoragePath`, normalizes
    supported formats into text context, and optionally attaches images/PDFs as
    multimodal inputs when ``multi_modal=True``.

    :param prompt: The analysis prompt for the LLM.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param file_path: File or prefix to analyze.
    :param file_conn_id: Optional connection ID for the storage backend.
        Overrides a connection embedded in ``file_path``.
    :param multi_modal: Allow PNG/JPG/PDF inputs as binary attachments.
        Default ``False``.
    :param max_files: Maximum number of files to include from a prefix.
        Excess files are omitted and noted in the prompt. Default ``20``.
    :param max_file_size_bytes: Maximum size of any single input file.
        Default ``5 MiB``.
    :param max_total_size_bytes: Maximum cumulative size across all resolved
        files. Default ``20 MiB``.
    :param max_text_chars: Maximum normalized text context passed to the LLM
        after sampling/truncation. Default ``100000``.
    :param sample_rows: Maximum number of sampled rows or records included for
        CSV, Parquet, and Avro inputs. This limits structural preview depth,
        while ``max_file_size_bytes`` and ``max_total_size_bytes`` limit bytes
        read from storage and ``max_text_chars`` limits the final prompt text
        budget. Default ``10``.
    """

    template_fields: Sequence[str] = (
        *LLMOperator.template_fields,
        "file_path",
        "file_conn_id",
    )

    def __init__(
        self,
        *,
        file_path: str,
        file_conn_id: str | None = None,
        multi_modal: bool = False,
        max_files: int = 20,
        max_file_size_bytes: int = 5 * 1024 * 1024,
        max_total_size_bytes: int = 20 * 1024 * 1024,
        max_text_chars: int = 100_000,
        sample_rows: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if max_files <= 0:
            raise ValueError("max_files must be greater than zero.")
        if max_file_size_bytes <= 0:
            raise ValueError("max_file_size_bytes must be greater than zero.")
        if max_total_size_bytes <= 0:
            raise ValueError("max_total_size_bytes must be greater than zero.")
        if max_text_chars <= 0:
            raise ValueError("max_text_chars must be greater than zero.")
        if sample_rows <= 0:
            raise ValueError("sample_rows must be greater than zero.")

        self.file_path = file_path
        self.file_conn_id = file_conn_id
        self.multi_modal = multi_modal
        self.max_files = max_files
        self.max_file_size_bytes = max_file_size_bytes
        self.max_total_size_bytes = max_total_size_bytes
        self.max_text_chars = max_text_chars
        self.sample_rows = sample_rows

    def execute(self, context: Context) -> Any:
        request = build_file_analysis_request(
            file_path=self.file_path,
            file_conn_id=self.file_conn_id,
            prompt=self.prompt,
            multi_modal=self.multi_modal,
            max_files=self.max_files,
            max_file_size_bytes=self.max_file_size_bytes,
            max_total_size_bytes=self.max_total_size_bytes,
            max_text_chars=self.max_text_chars,
            sample_rows=self.sample_rows,
        )
        self.log.info(
            "Calling model for file analysis: files=%s, attachments=%s, text_files=%s, total_size_bytes=%s, "
            "omitted_files=%s, text_truncated=%s, multi_modal=%s, sample_rows=%s",
            len(request.resolved_paths),
            request.attachment_count,
            request.text_file_count,
            request.total_size_bytes,
            request.omitted_files,
            request.text_truncated,
            self.multi_modal,
            self.sample_rows,
        )
        self.log.debug("Resolved file analysis paths: %s", request.resolved_paths)
        agent: Agent[None, Any] = self.llm_hook.create_agent(
            output_type=self.output_type,
            instructions=self._build_system_prompt(),
            **self.agent_params,
        )
        result = agent.run_sync(request.user_content)
        log_run_summary(self.log, result)
        output = result.output

        if self.require_approval:
            self.defer_for_approval(context, output)  # type: ignore[misc]

        if isinstance(output, BaseModel):
            output = output.model_dump()

        return output

    def execute_complete(self, context: Context, generated_output: str, event: dict[str, Any]) -> Any:
        """Resume after human review, restoring structured outputs for XCom consumers."""
        output = super().execute_complete(context, generated_output, event)
        if isinstance(self.output_type, type) and issubclass(self.output_type, BaseModel):
            return self.output_type.model_validate_json(output).model_dump()
        return output

    def _build_system_prompt(self) -> str:
        prompt = (
            "You are a read-only file analysis assistant.\n"
            "Use only the provided metadata, normalized file content, and multimodal attachments.\n"
            "Do not claim to have modified files or executed any external actions.\n"
            "If file content is truncated or sampled, say so in your answer."
        )
        if self.system_prompt:
            prompt += f"\n\nAdditional instructions:\n{self.system_prompt}"
        return prompt

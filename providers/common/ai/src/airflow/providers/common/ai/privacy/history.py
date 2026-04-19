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
"""Pydantic AI history processors backed by Presidio."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from pydantic_ai.messages import BinaryContent

from airflow.providers.common.ai.privacy.exceptions import ATTACHMENT_REJECTION_MSG, InputGuardAttachmentError
from airflow.providers.common.ai.privacy.presidio_guard import PresidioInputGuard

if TYPE_CHECKING:
    import logging

    from airflow.providers.common.ai.privacy.config import InputGuardConfig


def sanitize_user_content(
    content: Any,
    *,
    guard: PresidioInputGuard,
    config: InputGuardConfig,
    source: str = "content",
) -> Any:
    """Sanitize supported user-content payloads while respecting attachment policy."""
    if isinstance(content, BinaryContent):
        if config.attachment_policy == "reject":
            raise InputGuardAttachmentError(ATTACHMENT_REJECTION_MSG)
        return content
    if isinstance(content, str):
        return guard.sanitize_text(content, source=source)
    if isinstance(content, list):
        return [
            sanitize_user_content(item, guard=guard, config=config, source=f"{source}[{i}]")
            for i, item in enumerate(content)
        ]
    if isinstance(content, tuple):
        return tuple(
            sanitize_user_content(item, guard=guard, config=config, source=f"{source}[{i}]")
            for i, item in enumerate(content)
        )
    if isinstance(content, dict):
        return {
            key: sanitize_user_content(value, guard=guard, config=config, source=f"{source}.{key}")
            for key, value in content.items()
        }
    # Non-text, non-container types (int, float, …) pass through unchanged.
    return content


def contains_binary_content(content: Any) -> bool:
    """Return True when user content includes at least one binary attachment."""
    if isinstance(content, BinaryContent):
        return True
    if isinstance(content, list | tuple):
        return any(contains_binary_content(item) for item in content)
    if isinstance(content, dict):
        return any(contains_binary_content(value) for value in content.values())
    return False


def sanitize_file_content(
    user_content: Any,
    input_guard: InputGuardConfig | dict[str, Any] | None,
    logger: logging.Logger | None = None,
) -> Any:
    """
    Sanitize file-analysis content using the given guard configuration.

    Performs an early binary-attachment check before invoking the guard so that
    multimodal attachments are rejected before any LLM call is made.

    :param user_content: The prompt/content payload built from the analysed files.
    :param input_guard: Guard configuration as an ``InputGuardConfig`` or raw dict.
    :param logger: Logger to use for sanitization summaries.
    :return: Sanitized content, or the original content when guarding is disabled.
    """
    from airflow.providers.common.ai.privacy.config import InputGuardConfig as _Config

    config = _Config.from_value(input_guard)
    if not config.enabled:
        return user_content
    if config.attachment_policy == "reject" and contains_binary_content(user_content):
        raise InputGuardAttachmentError(ATTACHMENT_REJECTION_MSG)
    guard = PresidioInputGuard(config, logger=logger)
    return sanitize_user_content(
        user_content, guard=guard, config=config, source="file_analysis.user_content"
    )


def build_history_processor(config: InputGuardConfig, logger: logging.Logger | None = None):
    """Build a Pydantic AI history processor that sanitizes outbound content."""
    guard = PresidioInputGuard(config, logger=logger)

    def _processor(messages: list[Any]) -> list[Any]:
        sanitized_messages: list[Any] = []
        for message_index, message in enumerate(messages):
            parts = getattr(message, "parts", None)
            if parts is None:
                sanitized_messages.append(message)
                continue

            new_parts = []
            changed = False
            for part_index, part in enumerate(parts):
                content = getattr(part, "content", None)
                if content is None:
                    new_parts.append(part)
                    continue
                sanitized_content = sanitize_user_content(
                    content,
                    guard=guard,
                    config=config,
                    source=f"message[{message_index}].parts[{part_index}]",
                )
                if sanitized_content == content:
                    new_parts.append(part)
                    continue
                changed = True
                # pydantic-ai messages are always dataclasses; replace() is safe.
                new_parts.append(replace(part, content=sanitized_content))

            if not changed:
                sanitized_messages.append(message)
                continue

            sanitized_messages.append(replace(message, parts=new_parts))
        return sanitized_messages

    return _processor

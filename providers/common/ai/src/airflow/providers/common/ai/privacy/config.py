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
"""Configuration for LLM input guarding."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from airflow.providers.common.ai.privacy.exceptions import InputGuardError

InputGuardMode = Literal["redact", "mask", "replace", "hash"]
AttachmentPolicy = Literal["reject", "allow_unmodified"]


@dataclass(slots=True)
class InputGuardConfig:
    """Runtime configuration for outbound LLM input sanitization."""

    enabled: bool = False
    language: str = "en"
    entities: tuple[str, ...] | None = None
    mode: InputGuardMode = "replace"
    replace_value_template: str = "<{entity_type}>"
    mask_char: str = "*"
    mask_from_end: bool = False
    score_threshold: float | None = None
    attachment_policy: AttachmentPolicy = "reject"
    log_sanitized_text: bool = False
    log_preview_chars: int = 200

    @classmethod
    def from_value(cls, value: InputGuardConfig | dict[str, Any] | None) -> InputGuardConfig:
        """Normalize supported config inputs into an InputGuardConfig."""
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            raise InputGuardError("input_guard must be a dict or InputGuardConfig instance.")
        try:
            entities = value.get("entities")
            if entities is not None:
                value = {**value, "entities": tuple(entities)}
            return cls(**value)
        except TypeError as exc:
            raise InputGuardError(f"Invalid input_guard configuration: {exc}") from exc

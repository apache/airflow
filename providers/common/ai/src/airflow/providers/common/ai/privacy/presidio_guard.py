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
"""Presidio-backed sanitization helpers for model-bound text."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

if TYPE_CHECKING:
    import logging

    from airflow.providers.common.ai.privacy.config import InputGuardConfig

# Masks up to this many characters per detected entity, effectively masking all practical PII.
_MAX_MASK_CHARS = 10_000


@lru_cache(maxsize=1)
def _create_presidio_engines() -> tuple[Any, Any]:
    """Create and cache shared Presidio engines for the provider process."""
    try:
        from presidio_analyzer import AnalyzerEngine
        from presidio_anonymizer import AnonymizerEngine
    except ImportError as exc:
        raise AirflowOptionalProviderFeatureException(
            "input_guard requires the presidio extra: "
            "pip install apache-airflow-providers-common-ai[presidio]"
        ) from exc

    return AnalyzerEngine(), AnonymizerEngine()


class PresidioInputGuard:
    """Sanitize outbound strings and JSON-like payloads with Presidio."""

    def __init__(self, config: InputGuardConfig, logger: logging.Logger | None = None) -> None:
        self.config = config
        self.logger = logger
        self._analyzer, self._anonymizer = _create_presidio_engines()

    def sanitize_text(self, text: str, *, source: str = "text") -> str:
        """Analyze and anonymize one text blob."""
        if not text:
            return text

        analyze_kwargs: dict[str, Any] = {
            "text": text,
            "language": self.config.language,
        }
        if self.config.entities:
            analyze_kwargs["entities"] = list(self.config.entities)
        if self.config.score_threshold is not None:
            analyze_kwargs["score_threshold"] = self.config.score_threshold

        results = self._analyzer.analyze(**analyze_kwargs)
        if not results:
            return text

        sanitized_text = self._anonymizer.anonymize(
            text=text,
            analyzer_results=results,
            operators=self._build_operators(results),
        ).text
        self._log_sanitization(results=results, sanitized_text=sanitized_text, source=source)
        return sanitized_text

    def sanitize_value(self, value: Any, *, source: str = "value") -> Any:
        """Recursively sanitize text in JSON-like tool payloads."""
        if isinstance(value, str):
            return self.sanitize_text(value, source=source)
        if isinstance(value, Mapping):
            return {
                key: self.sanitize_value(subvalue, source=f"{source}.{key}")
                for key, subvalue in value.items()
            }
        if isinstance(value, list):
            return [
                self.sanitize_value(item, source=f"{source}[{index}]") for index, item in enumerate(value)
            ]
        if isinstance(value, tuple):
            return tuple(
                self.sanitize_value(item, source=f"{source}[{index}]") for index, item in enumerate(value)
            )
        return value

    def _build_operators(self, results: Sequence[Any]) -> dict[str, Any]:
        from presidio_anonymizer.entities import OperatorConfig

        if self.config.mode == "redact":
            return {"DEFAULT": OperatorConfig("redact", {})}
        if self.config.mode == "hash":
            return {"DEFAULT": OperatorConfig("hash", {})}
        if self.config.mode == "mask":
            return {
                "DEFAULT": OperatorConfig(
                    "mask",
                    {
                        "masking_char": self.config.mask_char,
                        "chars_to_mask": _MAX_MASK_CHARS,
                        "from_end": self.config.mask_from_end,
                    },
                )
            }

        operators: dict[str, Any] = {}
        for result in results:
            entity_type = getattr(result, "entity_type", "PII")
            operators[entity_type] = OperatorConfig(
                "replace",
                {"new_value": self.config.replace_value_template.format(entity_type=entity_type)},
            )
        operators.setdefault("DEFAULT", OperatorConfig("replace", {"new_value": "<PII>"}))
        return operators

    def _log_sanitization(self, *, results: Sequence[Any], sanitized_text: str, source: str) -> None:
        """Emit privacy-safe logs for sanitized outbound content."""
        if self.logger is None:
            return

        entity_counts = Counter(getattr(result, "entity_type", "PII") for result in results)
        entity_summary = ", ".join(
            f"{entity_type}:{count}" for entity_type, count in sorted(entity_counts.items())
        )

        if self.config.log_sanitized_text:
            preview = sanitized_text
            if len(preview) > self.config.log_preview_chars:
                preview = preview[: self.config.log_preview_chars] + "..."
            self.logger.info(
                "Input guard sanitized outbound %s: mode=%s, entities=%s, preview=%r",
                source,
                self.config.mode,
                entity_summary,
                preview,
            )
            return

        self.logger.info(
            "Input guard sanitized outbound %s: mode=%s, entities=%s",
            source,
            self.config.mode,
            entity_summary,
        )

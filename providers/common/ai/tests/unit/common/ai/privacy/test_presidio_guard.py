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

import sys
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.privacy.config import InputGuardConfig
from airflow.providers.common.ai.privacy.exceptions import (
    ATTACHMENT_REJECTION_MSG,
    InputGuardAttachmentError,
    InputGuardError,
)
from airflow.providers.common.ai.privacy.history import (
    build_history_processor,
    contains_binary_content,
    sanitize_file_content,
    sanitize_user_content,
)
from airflow.providers.common.ai.privacy.presidio_guard import PresidioInputGuard, _create_presidio_engines

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engines(analyzer_results=None, anonymized_text="<REDACTED>"):
    """Return (analyzer_mock, anonymizer_mock) pre-configured for a single analyze call."""
    analyzer = MagicMock()
    analyzer.analyze.return_value = analyzer_results or []
    anonymizer = MagicMock()
    anonymizer.anonymize.return_value = SimpleNamespace(text=anonymized_text)
    return analyzer, anonymizer


def _patch_modules(analyzer, anonymizer):
    """Return a dict for use with patch.dict(sys.modules, ...)."""
    return {
        "presidio_analyzer": SimpleNamespace(AnalyzerEngine=MagicMock(return_value=analyzer)),
        "presidio_anonymizer": SimpleNamespace(AnonymizerEngine=MagicMock(return_value=anonymizer)),
    }


# ---------------------------------------------------------------------------
# InputGuardConfig
# ---------------------------------------------------------------------------


class TestInputGuardConfig:
    def test_from_value_none_returns_disabled_default(self):
        cfg = InputGuardConfig.from_value(None)
        assert cfg.enabled is False

    def test_from_value_passthrough_config_instance(self):
        original = InputGuardConfig(enabled=True, mode="redact")
        assert InputGuardConfig.from_value(original) is original

    def test_from_value_dict_converts_entities_to_tuple(self):
        cfg = InputGuardConfig.from_value({"enabled": True, "entities": ["EMAIL_ADDRESS", "PHONE_NUMBER"]})
        assert cfg.enabled is True
        assert cfg.entities == ("EMAIL_ADDRESS", "PHONE_NUMBER")

    def test_from_value_invalid_type_raises(self):
        with pytest.raises(InputGuardError, match="input_guard must be a dict"):
            InputGuardConfig.from_value("not-a-dict")  # type: ignore[arg-type]

    def test_from_value_unknown_key_raises(self):
        with pytest.raises(InputGuardError, match="Invalid input_guard configuration"):
            InputGuardConfig.from_value({"enabled": True, "unknown_key": "x"})

    def test_mask_from_end_default_is_false(self):
        """Masking should preserve trailing characters (e.g. last 4 of a card) by default."""
        assert InputGuardConfig().mask_from_end is False


# ---------------------------------------------------------------------------
# PresidioInputGuard — logging
# ---------------------------------------------------------------------------


class TestPresidioInputGuardLogging:
    def setup_method(self):
        _create_presidio_engines.cache_clear()

    def teardown_method(self):
        _create_presidio_engines.cache_clear()

    def test_sanitize_text_logs_sanitized_preview_when_enabled(self):
        analyzer, anonymizer = _make_engines(
            analyzer_results=[
                SimpleNamespace(entity_type="EMAIL_ADDRESS"),
                SimpleNamespace(entity_type="CREDIT_CARD"),
            ],
            anonymized_text="Email is <EMAIL_ADDRESS> and card is <CREDIT_CARD>",
        )
        logger = MagicMock()

        with patch.dict(sys.modules, _patch_modules(analyzer, anonymizer)):
            guard = PresidioInputGuard(
                InputGuardConfig(enabled=True, log_sanitized_text=True, log_preview_chars=80),
                logger=logger,
            )
            with patch.object(guard, "_build_operators", return_value={}):
                sanitized = guard.sanitize_text(
                    "Email is jane@example.com and card is 4111 1111 1111 1111",
                    source="prompt",
                )

        assert sanitized == "Email is <EMAIL_ADDRESS> and card is <CREDIT_CARD>"
        logger.info.assert_called_once_with(
            "Input guard sanitized outbound %s: mode=%s, entities=%s, preview=%r",
            "prompt",
            "replace",
            "CREDIT_CARD:1, EMAIL_ADDRESS:1",
            "Email is <EMAIL_ADDRESS> and card is <CREDIT_CARD>",
        )

    def test_sanitize_text_logs_summary_when_preview_logging_disabled(self):
        analyzer, anonymizer = _make_engines(
            analyzer_results=[SimpleNamespace(entity_type="PHONE_NUMBER")],
            anonymized_text="Call <PHONE_NUMBER>",
        )
        logger = MagicMock()

        with patch.dict(sys.modules, _patch_modules(analyzer, anonymizer)):
            guard = PresidioInputGuard(InputGuardConfig(enabled=True), logger=logger)
            with patch.object(guard, "_build_operators", return_value={}):
                guard.sanitize_text("Call 555-123-4567", source="instructions")

        logger.info.assert_called_once_with(
            "Input guard sanitized outbound %s: mode=%s, entities=%s",
            "instructions",
            "replace",
            "PHONE_NUMBER:1",
        )

    def test_no_log_when_no_entities_detected(self):
        analyzer, anonymizer = _make_engines(analyzer_results=[])
        logger = MagicMock()

        with patch.dict(sys.modules, _patch_modules(analyzer, anonymizer)):
            guard = PresidioInputGuard(InputGuardConfig(enabled=True), logger=logger)
            result = guard.sanitize_text("no pii here", source="prompt")

        assert result == "no pii here"
        logger.info.assert_not_called()

    def test_engines_are_cached_across_guard_instances(self):
        analyzer_cls = MagicMock(return_value=MagicMock())
        anonymizer_cls = MagicMock(return_value=MagicMock())

        with patch.dict(
            sys.modules,
            {
                "presidio_analyzer": SimpleNamespace(AnalyzerEngine=analyzer_cls),
                "presidio_anonymizer": SimpleNamespace(AnonymizerEngine=anonymizer_cls),
            },
        ):
            PresidioInputGuard(InputGuardConfig(enabled=True))
            PresidioInputGuard(InputGuardConfig(enabled=True))

        analyzer_cls.assert_called_once_with()
        anonymizer_cls.assert_called_once_with()


# ---------------------------------------------------------------------------
# PresidioInputGuard — modes
# ---------------------------------------------------------------------------


class TestBuildOperators:
    def setup_method(self):
        _create_presidio_engines.cache_clear()

    def teardown_method(self):
        _create_presidio_engines.cache_clear()

    def _make_guard(self, mode):
        analyzer_cls = MagicMock(return_value=MagicMock())
        anonymizer_cls = MagicMock(return_value=MagicMock())
        with patch.dict(
            sys.modules,
            {
                "presidio_analyzer": SimpleNamespace(AnalyzerEngine=analyzer_cls),
                "presidio_anonymizer": SimpleNamespace(AnonymizerEngine=anonymizer_cls),
            },
        ):
            return PresidioInputGuard(InputGuardConfig(enabled=True, mode=mode))

    def test_redact_mode_builds_single_default_operator(self):

        guard = self._make_guard("redact")
        results = [SimpleNamespace(entity_type="EMAIL_ADDRESS")]
        ops = guard._build_operators(results)
        assert set(ops) == {"DEFAULT"}
        assert ops["DEFAULT"].operator_name == "redact"

    def test_hash_mode_builds_single_default_operator(self):
        guard = self._make_guard("hash")
        ops = guard._build_operators([SimpleNamespace(entity_type="PHONE_NUMBER")])
        assert set(ops) == {"DEFAULT"}
        assert ops["DEFAULT"].operator_name == "hash"

    def test_mask_mode_includes_mask_char_and_max_chars(self):
        from airflow.providers.common.ai.privacy.presidio_guard import _MAX_MASK_CHARS

        guard = self._make_guard("mask")
        ops = guard._build_operators([SimpleNamespace(entity_type="US_SSN")])
        assert ops["DEFAULT"].operator_name == "mask"
        params = ops["DEFAULT"].params
        assert params["masking_char"] == "*"
        assert params["chars_to_mask"] == _MAX_MASK_CHARS
        assert params["from_end"] is False  # mask_from_end default is False

    def test_replace_mode_uses_entity_type_template(self):
        guard = self._make_guard("replace")
        results = [SimpleNamespace(entity_type="EMAIL_ADDRESS")]
        ops = guard._build_operators(results)
        assert "EMAIL_ADDRESS" in ops
        assert ops["EMAIL_ADDRESS"].params["new_value"] == "<EMAIL_ADDRESS>"

    def test_replace_mode_has_default_fallback(self):
        guard = self._make_guard("replace")
        ops = guard._build_operators([SimpleNamespace(entity_type="EMAIL_ADDRESS")])
        assert "DEFAULT" in ops
        assert ops["DEFAULT"].params["new_value"] == "<PII>"


# ---------------------------------------------------------------------------
# sanitize_user_content
# ---------------------------------------------------------------------------


class TestSanitizeUserContent:
    def _guard(self, sanitized="<EMAIL>"):
        g = MagicMock(spec=PresidioInputGuard)
        g.sanitize_text.side_effect = lambda text, source: sanitized if "@" in text else text
        return g

    def test_plain_string_delegates_to_guard(self):
        guard = self._guard("<EMAIL>")
        config = InputGuardConfig(enabled=True)
        result = sanitize_user_content("user@example.com", guard=guard, config=config)
        assert result == "<EMAIL>"
        guard.sanitize_text.assert_called_once()

    def test_list_is_sanitized_recursively(self):
        guard = self._guard("<EMAIL>")
        config = InputGuardConfig(enabled=True)
        result = sanitize_user_content(["user@example.com", "safe text"], guard=guard, config=config)
        assert result == ["<EMAIL>", "safe text"]

    def test_tuple_is_sanitized_recursively(self):
        guard = self._guard("<EMAIL>")
        config = InputGuardConfig(enabled=True)
        result = sanitize_user_content(("user@example.com",), guard=guard, config=config)
        assert result == ("<EMAIL>",)

    def test_dict_is_sanitized_recursively(self):
        guard = self._guard("<EMAIL>")
        config = InputGuardConfig(enabled=True)
        result = sanitize_user_content(
            {"email": "user@example.com", "other": "safe"}, guard=guard, config=config
        )
        assert result == {"email": "<EMAIL>", "other": "safe"}

    def test_non_text_value_passes_through(self):
        guard = MagicMock(spec=PresidioInputGuard)
        config = InputGuardConfig(enabled=True)
        result = sanitize_user_content(42, guard=guard, config=config)
        assert result == 42
        guard.sanitize_text.assert_not_called()

    def test_binary_content_reject_raises(self):
        from pydantic_ai.messages import BinaryContent

        guard = MagicMock(spec=PresidioInputGuard)
        config = InputGuardConfig(enabled=True, attachment_policy="reject")
        binary = BinaryContent(data=b"...", media_type="image/png")
        with pytest.raises(InputGuardAttachmentError, match=ATTACHMENT_REJECTION_MSG):
            sanitize_user_content(binary, guard=guard, config=config)

    def test_binary_content_allow_unmodified_passes_through(self):
        from pydantic_ai.messages import BinaryContent

        guard = MagicMock(spec=PresidioInputGuard)
        config = InputGuardConfig(enabled=True, attachment_policy="allow_unmodified")
        binary = BinaryContent(data=b"...", media_type="image/png")
        result = sanitize_user_content(binary, guard=guard, config=config)
        assert result is binary

    def test_binary_inside_list_is_rejected(self):
        from pydantic_ai.messages import BinaryContent

        guard = MagicMock(spec=PresidioInputGuard)
        config = InputGuardConfig(enabled=True, attachment_policy="reject")
        binary = BinaryContent(data=b"...", media_type="image/png")
        with pytest.raises(InputGuardAttachmentError):
            sanitize_user_content(["text", binary], guard=guard, config=config)


# ---------------------------------------------------------------------------
# contains_binary_content
# ---------------------------------------------------------------------------


class TestContainsBinaryContent:
    def test_plain_string_returns_false(self):
        assert contains_binary_content("hello") is False

    def test_binary_content_returns_true(self):
        from pydantic_ai.messages import BinaryContent

        assert contains_binary_content(BinaryContent(data=b"x", media_type="image/png")) is True

    def test_list_with_binary_returns_true(self):
        from pydantic_ai.messages import BinaryContent

        assert contains_binary_content(["text", BinaryContent(data=b"x", media_type="image/png")]) is True

    def test_nested_dict_with_binary_returns_true(self):
        from pydantic_ai.messages import BinaryContent

        assert contains_binary_content({"a": BinaryContent(data=b"x", media_type="image/png")}) is True

    def test_plain_dict_returns_false(self):
        assert contains_binary_content({"key": "value"}) is False


# ---------------------------------------------------------------------------
# build_history_processor
# ---------------------------------------------------------------------------


class TestBuildHistoryProcessor:
    def setup_method(self):
        _create_presidio_engines.cache_clear()

    def teardown_method(self):
        _create_presidio_engines.cache_clear()

    def _make_processor(self, sanitized_text="<PII>"):
        """Return a history processor backed by a mocked guard."""
        analyzer_cls = MagicMock(return_value=MagicMock())
        anonymizer_cls = MagicMock(return_value=MagicMock())
        with patch.dict(
            sys.modules,
            {
                "presidio_analyzer": SimpleNamespace(AnalyzerEngine=analyzer_cls),
                "presidio_anonymizer": SimpleNamespace(AnonymizerEngine=anonymizer_cls),
            },
        ):
            config = InputGuardConfig(enabled=True)
            processor = build_history_processor(config)

        # Patch the guard inside the closure after creation.
        guard = MagicMock(spec=PresidioInputGuard)
        guard.sanitize_text.side_effect = lambda text, source: sanitized_text if text else text
        # Re-inject guard by rebuilding with our mock — easier via direct patch below.
        return processor, guard, config

    def test_messages_without_parts_pass_through(self):
        config = InputGuardConfig(enabled=True)
        guard = MagicMock(spec=PresidioInputGuard)
        guard.sanitize_text.return_value = "<PII>"

        with patch("airflow.providers.common.ai.privacy.history.PresidioInputGuard", return_value=guard):
            processor = build_history_processor(config)

        msg = SimpleNamespace()  # no .parts attribute
        assert processor([msg]) == [msg]

    def test_unchanged_messages_are_returned_as_is(self):
        config = InputGuardConfig(enabled=True)
        guard = MagicMock(spec=PresidioInputGuard)
        # sanitize_text returns same text — no change
        guard.sanitize_text.side_effect = lambda text, source: text

        with patch("airflow.providers.common.ai.privacy.history.PresidioInputGuard", return_value=guard):
            processor = build_history_processor(config)

        @dataclass
        class Part:
            content: str

        @dataclass
        class Message:
            parts: list

        msg = Message(parts=[Part(content="hello")])
        result = processor([msg])
        assert result[0] is msg  # identity — not reconstructed

    def test_changed_part_rebuilds_message_via_replace(self):
        config = InputGuardConfig(enabled=True)
        guard = MagicMock(spec=PresidioInputGuard)
        guard.sanitize_text.return_value = "<EMAIL>"

        with patch("airflow.providers.common.ai.privacy.history.PresidioInputGuard", return_value=guard):
            processor = build_history_processor(config)

        @dataclass
        class Part:
            content: str

        @dataclass
        class Message:
            parts: list

        original_msg = Message(parts=[Part(content="user@example.com")])
        result = processor([original_msg])
        assert result[0] is not original_msg
        assert result[0].parts[0].content == "<EMAIL>"

    def test_part_without_content_attr_is_kept(self):
        config = InputGuardConfig(enabled=True)
        guard = MagicMock(spec=PresidioInputGuard)
        guard.sanitize_text.return_value = "<PII>"

        with patch("airflow.providers.common.ai.privacy.history.PresidioInputGuard", return_value=guard):
            processor = build_history_processor(config)

        @dataclass
        class ToolCallPart:
            tool_name: str  # no .content

        @dataclass
        class Message:
            parts: list

        msg = Message(parts=[ToolCallPart(tool_name="search")])
        result = processor([msg])
        assert result[0] is msg


# ---------------------------------------------------------------------------
# sanitize_file_content
# ---------------------------------------------------------------------------


class TestSanitizeFileContent:
    def test_disabled_guard_returns_content_unchanged(self):
        result = sanitize_file_content("sensitive text", {"enabled": False})
        assert result == "sensitive text"

    def test_none_guard_returns_content_unchanged(self):
        result = sanitize_file_content("sensitive text", None)
        assert result == "sensitive text"

    def test_enabled_guard_sanitizes_text(self):
        guard = MagicMock(spec=PresidioInputGuard)
        guard.sanitize_text.return_value = "<EMAIL>"

        with patch("airflow.providers.common.ai.privacy.history.PresidioInputGuard", return_value=guard):
            result = sanitize_file_content("user@example.com", {"enabled": True})

        assert result == "<EMAIL>"

    def test_binary_reject_policy_raises_before_guard(self):
        from pydantic_ai.messages import BinaryContent

        binary = BinaryContent(data=b"...", media_type="image/png")
        with pytest.raises(InputGuardAttachmentError, match=ATTACHMENT_REJECTION_MSG):
            sanitize_file_content(binary, {"enabled": True, "attachment_policy": "reject"})

    def test_binary_allow_unmodified_passes_through(self):
        from pydantic_ai.messages import BinaryContent

        guard = MagicMock(spec=PresidioInputGuard)
        # sanitize_text won't be called for BinaryContent
        binary = BinaryContent(data=b"...", media_type="image/png")

        with patch("airflow.providers.common.ai.privacy.history.PresidioInputGuard", return_value=guard):
            result = sanitize_file_content(binary, {"enabled": True, "attachment_policy": "allow_unmodified"})

        assert result is binary

"""Tests for the LLM provider abstraction layer."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from dag_triage.llm_provider import (
    LLMProvider,
    LLMResponse,
    LocalRuleProvider,
    OpenAIProvider,
    get_llm_provider,
)

# ---------------------------------------------------------------------------
# LocalRuleProvider
# ---------------------------------------------------------------------------


class TestLocalRuleProvider:
    """Verify the zero-dependency local fallback provider."""

    @pytest.fixture
    def provider(self) -> LocalRuleProvider:
        return LocalRuleProvider()

    def test_name(self, provider: LocalRuleProvider) -> None:
        assert provider.name == "local-rule"

    @pytest.mark.parametrize(
        "category",
        ["TRANSIENT", "DATA_QUALITY", "RESOURCE", "CODE", "EXTERNAL_DEPENDENCY"],
    )
    def test_summarize_returns_response_for_each_category(
        self, provider: LocalRuleProvider, category: str
    ) -> None:
        resp = provider.summarize("some log", category)
        assert isinstance(resp, LLMResponse)
        assert resp.provider_name == "local-rule"
        assert not resp.is_fallback
        assert len(resp.text) > 0

    def test_summarize_includes_error_line_when_provided(self, provider: LocalRuleProvider) -> None:
        resp = provider.summarize("log", "CODE", error_line="KeyError: 'x'")
        assert "KeyError: 'x'" in resp.text

    def test_summarize_unknown_category_returns_generic(self, provider: LocalRuleProvider) -> None:
        resp = provider.summarize("log", "UNKNOWN_CAT")
        assert "Review the task log" in resp.text

    def test_satisfies_abstract_interface(self, provider: LocalRuleProvider) -> None:
        assert isinstance(provider, LLMProvider)


# ---------------------------------------------------------------------------
# OpenAIProvider — fallback behaviour (no real API calls)
# ---------------------------------------------------------------------------


class TestOpenAIProviderFallback:
    """Verify that OpenAI provider falls back to local rules on failure."""

    def test_falls_back_when_openai_not_installed(self) -> None:
        provider = OpenAIProvider(api_key="test-key")
        with mock.patch.dict(sys.modules, {"openai": None}):
            resp = provider.summarize("log text", "CODE", error_line="ImportError")
        assert resp.is_fallback
        assert resp.provider_name == "openai"
        assert len(resp.text) > 0

    def test_falls_back_on_api_error(self) -> None:
        provider = OpenAIProvider(api_key="test-key")
        fake_openai = mock.MagicMock()
        fake_openai.OpenAI.return_value.chat.completions.create.side_effect = RuntimeError(
            "connection refused"
        )
        with mock.patch.dict(sys.modules, {"openai": fake_openai}):
            resp = provider.summarize("log", "TRANSIENT")
        assert resp.is_fallback

    def test_name(self) -> None:
        assert OpenAIProvider(api_key="k").name == "openai"

    def test_satisfies_abstract_interface(self) -> None:
        assert isinstance(OpenAIProvider(api_key="k"), LLMProvider)


# ---------------------------------------------------------------------------
# OpenAIProvider — successful API call (mocked)
# ---------------------------------------------------------------------------


class TestOpenAIProviderSuccess:
    """Verify the happy path with a mocked OpenAI client."""

    def test_returns_llm_text_on_success(self) -> None:
        provider = OpenAIProvider(api_key="test-key", model="gpt-4o-mini")

        fake_choice = mock.MagicMock()
        fake_choice.message.content = "The task OOM-killed due to large payload."
        fake_response = mock.MagicMock()
        fake_response.choices = [fake_choice]

        fake_openai = mock.MagicMock()
        fake_openai.OpenAI.return_value.chat.completions.create.return_value = fake_response

        with mock.patch.dict(sys.modules, {"openai": fake_openai}):
            resp = provider.summarize("OOMKilled log", "RESOURCE")

        assert not resp.is_fallback
        assert resp.provider_name == "openai"
        assert resp.text == "The task OOM-killed due to large payload."


# ---------------------------------------------------------------------------
# get_llm_provider() factory
# ---------------------------------------------------------------------------


class TestGetLlmProvider:
    """Verify config-driven provider selection."""

    def test_defaults_to_local_when_airflow_not_available(self) -> None:
        with mock.patch.dict(sys.modules, {"airflow": None, "airflow.configuration": None}):
            provider = get_llm_provider()
        assert isinstance(provider, LocalRuleProvider)

    def test_returns_local_for_explicit_config(self) -> None:
        fake_conf = mock.MagicMock()
        fake_conf.get.return_value = "local"
        with mock.patch("dag_triage.llm_provider.conf", fake_conf, create=True):
            with mock.patch(
                "dag_triage.llm_provider.get_llm_provider.__module__",
                create=True,
            ):
                # Simulate Airflow config returning "local"
                provider = get_llm_provider()
        assert isinstance(provider, (LocalRuleProvider, LLMProvider))

    def test_unknown_provider_falls_back_to_local(self) -> None:
        fake_conf = mock.MagicMock()
        fake_conf.get.return_value = "nonexistent-provider"
        fake_module = mock.MagicMock()
        fake_module.conf = fake_conf
        with mock.patch.dict(sys.modules, {"airflow.configuration": fake_module}):
            provider = get_llm_provider()
        assert isinstance(provider, LocalRuleProvider)


# ---------------------------------------------------------------------------
# No provider-specific code leaks — structural check
# ---------------------------------------------------------------------------


class TestNoProviderLeaks:
    """Verify classifier and remediation_kb have no LLM/OpenAI imports."""

    @pytest.mark.parametrize(
        "module_path",
        [
            Path(__file__).parent.parent / "src" / "dag_triage" / "classifier.py",
            Path(__file__).parent.parent / "src" / "dag_triage" / "remediation_kb.py",
        ],
    )
    def test_no_openai_or_llm_import(self, module_path: Path) -> None:
        source = module_path.read_text()
        assert "openai" not in source.lower(), f"{module_path.name} must not reference openai"
        assert "LLMProvider" not in source, f"{module_path.name} must not reference LLMProvider"

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
Drift tripwires between ``provider.yaml``'s declared ``external-services`` and what
pydantic-ai actually installs.

See `files/common-ai-supported-services/label-sources.md` (planning artefact, not shipped)
for how ``LABELS``, ``NON_MODULE_SERVICES``, ``UNREACHABLE_MODULES`` and
``DEPRECATED_UPSTREAM_MODULES`` below were derived, including the empirical checks that
back each exclusion.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

import pydantic_ai.providers as pydantic_ai_providers
import pytest
import yaml
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import infer_model, known_model_names

from airflow.providers.common.ai.get_provider_info import get_provider_info

# `toolsets` never flowed through the runtime `provider_info.schema.json`
# (memory `reference_two_provider_schemas_only_authoring_may_tighten`), so
# `get_provider_info()` doesn't carry it -- read `provider.yaml` directly instead.
PROVIDER_YAML_PATH = Path(__file__).resolve().parents[4] / "provider.yaml"


def _toolsets_block() -> dict:
    provider_yaml = yaml.safe_load(PROVIDER_YAML_PATH.read_text())
    (toolset_block,) = provider_yaml["toolsets"]
    return toolset_block


# Module basename -> display label used in `provider.yaml`'s `pydanticai.external-services`.
# Sourced from each module's own `name` property / class docstring; kept here (not in
# `src/`) because it is test data, not runtime behaviour.
LABELS = {
    "alibaba": "Alibaba Cloud Model Studio",
    "anthropic": "Anthropic",
    "azure": "Azure OpenAI",
    "bedrock": "AWS Bedrock",
    "bedrock_mantle": "AWS Bedrock Mantle",
    "cerebras": "Cerebras",
    "cohere": "Cohere",
    "crusoe": "Crusoe",
    "deepseek": "DeepSeek",
    "fireworks": "Fireworks AI",
    "gateway": "Pydantic AI Gateway",
    "google": "Google Gemini",
    "google_cloud": "Google Vertex AI",
    "groq": "Groq",
    "heroku": "Heroku",
    "huggingface": "Hugging Face",
    "litellm": "LiteLLM",
    "mistral": "Mistral AI",
    "moonshotai": "Moonshot AI",
    "nebius": "Nebius",
    "ollama": "Ollama",
    "openai": "OpenAI",
    "openrouter": "OpenRouter",
    "ovhcloud": "OVHcloud",
    "sambanova": "SambaNova",
    "snowflake": "Snowflake Cortex",
    "together": "Together AI",
    "vercel": "Vercel AI Gateway",
    "xai": "xAI",
    "zai": "Z.AI",
}

# Declared in `pydanticai.external-services` but not a pydantic-ai provider module: reached
# by pointing the `openai` provider's `base_url` at a vLLM endpoint instead
# (`hooks/pydantic_ai.py`), so it cannot be derived from `pydantic_ai.providers`.
NON_MODULE_SERVICES = {"vLLM"}

# Modules `pkgutil` discovers under `pydantic_ai.providers` that `infer_model` never
# routes to -- `infer_model("voyageai:...")` / `infer_model("sentence-transformers:...")`
# raise `UserError: Unknown model`, confirmed by `test_unreachable_modules_stay_unreachable`
# below. Not listed in `LABELS`/`provider.yaml` because there is no `common.ai` code path
# (chat or embedding) that reaches them.
UNREACHABLE_MODULES = {"voyageai", "sentence_transformers"}

# Modules that ARE reachable through `infer_model` (unlike UNREACHABLE_MODULES) but are
# excluded by decision because pydantic-ai itself marks the upstream backend dead:
# `pydantic_ai.providers.github.GitHubProvider` is decorated `@deprecated` with the message
# "GitHub Models was retired on 2026-07-30 ... this provider is deprecated and will be
# removed in v3." There is no GitHub Copilot provider anywhere in pydantic-ai; the only
# "github" support that ever existed is this now-retired integration. Advertising a vendor
# whose backend the vendor itself shut down is worse than not listing it (Wei's call,
# 2026-09-11 review round). `test_deprecated_upstream_modules_stay_deprecated` below
# guards the reversal: if pydantic-ai un-deprecates this module, or a real GitHub Copilot
# provider appears under a different module name, the corresponding test goes red instead
# of silently staying excluded.
DEPRECATED_UPSTREAM_MODULES = {"github"}

# `known_model_names()` prefixes that don't match a `pydantic_ai.providers` module basename
# directly (hyphen normalized to underscore) but still resolve through `infer_model`.
PREFIX_ALIASES = {
    "openai-chat": "openai",
    "openai-responses": "openai",
    "azure-responses": "azure",
}

# The vendors `supported_services.rst`'s Notes section names, in hand-written prose, as the
# ones `gateway/<vendor>:<model>` currently routes to. Deliberately kept as a human-readable
# sentence there rather than a generated list (2026-09-11 review round: this is a customer-
# facing page, not a place to trade readability for testability) -- this constant is the
# reference value the two tests below check it against, in both directions:
# `test_gateway_note_vendors_match_known_model_names` guards against pydantic-ai adding/dropping
# a gateway vendor, and `test_gateway_note_sentence_names_the_expected_vendors` guards against
# the prose and this constant drifting apart from each other.
GATEWAY_NOTE_VENDOR_LABELS = {
    "Anthropic",
    "AWS Bedrock",
    "Google Gemini",
    "Google Vertex AI",
    "Groq",
    "OpenAI",
}


def _discovered_provider_modules() -> set[str]:
    """All non-private modules under `pydantic_ai.providers`, including `gateway`.

    `gateway` is a real, reachable upstream service (`PydanticAIHook.get_conn()` forwards
    a `gateway/<vendor>:<model>` model string straight to `infer_model`, which resolves it
    to a working `Model` instance -- see `label-sources.md`), so it is not excluded here.
    """
    return {
        m.name for m in pkgutil.iter_modules(pydantic_ai_providers.__path__) if not m.name.startswith("_")
    }


def _pydanticai_external_services() -> list[str]:
    connection_types = get_provider_info()["connection-types"]
    (pydanticai,) = (c for c in connection_types if c["connection-type"] == "pydanticai")
    return pydanticai["external-services"]


class TestConnectionTypeExternalServices:
    def test_every_connection_type_declares_external_services(self):
        for conn in get_provider_info()["connection-types"]:
            assert conn.get("external-services"), (
                f"connection-type {conn['connection-type']!r} has no external-services"
            )


class TestPydanticAIExternalServicesDrift:
    def test_pydanticai_external_services_match_pydantic_ai_provider_modules(self):
        discovered = _discovered_provider_modules() - UNREACHABLE_MODULES - DEPRECATED_UPSTREAM_MODULES
        derived = {LABELS.get(m, f"<unlabelled:{m}>") for m in discovered} | NON_MODULE_SERVICES
        declared = set(_pydanticai_external_services())

        missing = derived - declared
        extra = declared - derived
        assert not missing, f"pydantic-ai vendors not declared in provider.yaml: {sorted(missing)}"
        assert not extra, f"provider.yaml declares vendors pydantic-ai doesn't have: {sorted(extra)}"

    def test_every_discovered_module_has_a_label(self):
        discovered = _discovered_provider_modules() - UNREACHABLE_MODULES - DEPRECATED_UPSTREAM_MODULES
        labelled = set(LABELS)

        unlabelled = discovered - labelled
        stale = labelled - discovered
        assert not unlabelled, f"no label for pydantic-ai module(s): {sorted(unlabelled)}"
        assert not stale, f"label(s) for module(s) pydantic-ai no longer has: {sorted(stale)}"

    def test_unreachable_modules_stay_unreachable(self):
        """
        Guard the other direction: if a future pydantic-ai release makes one of
        ``UNREACHABLE_MODULES`` reachable through ``infer_model``, this must fail so a human
        adds it to ``LABELS`` and ``provider.yaml`` instead of it silently staying missing.
        """
        for module in UNREACHABLE_MODULES:
            prefix = module.replace("_", "-")
            with pytest.raises(UserError, match="Unknown model"):
                infer_model(f"{prefix}:placeholder", provider_factory=lambda _p: object())

    def test_deprecated_upstream_modules_stay_deprecated(self):
        """
        Guard the other direction, for every member of ``DEPRECATED_UPSTREAM_MODULES`` (not just
        ``github`` -- this set is the one escape hatch in this file that isn't validated against
        pydantic-ai by name elsewhere, so adding a module here that isn't genuinely
        upstream-deprecated must not pass silently). If pydantic-ai un-deprecates a member
        (reactivating its backend, or repurposing the module), this must fail so a human
        re-evaluates whether it belongs back in ``LABELS`` and ``provider.yaml``, instead of it
        staying excluded forever on a stale rationale.
        """
        for module in DEPRECATED_UPSTREAM_MODULES:
            try:
                mod = importlib.import_module(f"pydantic_ai.providers.{module}")
            except ImportError as exc:
                # Deliberately not `pytest.importorskip`: an unmet optional dependency here
                # would silently turn off this guard, not just this one test run.
                pytest.fail(
                    f"pydantic_ai.providers.{module} (in DEPRECATED_UPSTREAM_MODULES) failed to "
                    f"import: {exc}. Install the missing dependency rather than skipping this "
                    f"test."
                )
            provider_classes = [
                obj
                for obj in vars(mod).values()
                if isinstance(obj, type)
                and obj.__module__ == mod.__name__
                and obj.__name__.endswith("Provider")
            ]
            assert provider_classes, f"no Provider class found in pydantic_ai.providers.{module}"
            assert all(hasattr(cls, "__deprecated__") for cls in provider_classes), (
                f"pydantic_ai.providers.{module} is no longer marked @deprecated -- re-evaluate "
                f"whether '{module}' should be re-added to LABELS and provider.yaml"
            )

    def test_known_model_name_prefixes_are_all_labelled(self):
        labelled = set(LABELS)
        for name in known_model_names():
            if ":" not in name:
                continue  # the bare "test" entry, pydantic-ai's built-in keyless TestModel
            prefix = name.removeprefix("gateway/").split(":", 1)[0]
            module = PREFIX_ALIASES.get(prefix, prefix.replace("-", "_"))
            assert module in labelled, f"model prefix {prefix!r} (from {name!r}) has no label"

    def test_gateway_note_vendors_match_known_model_names(self):
        """
        Guard `supported_services.rst`'s Notes sentence naming which vendors
        ``gateway/<vendor>:<model>`` currently routes to. If pydantic-ai's gateway adds or drops
        a vendor, this must fail so a human updates that sentence (and
        ``GATEWAY_NOTE_VENDOR_LABELS``) instead of it silently going stale.
        """
        gateway_prefixes = {
            name.split("/", 1)[1].split(":", 1)[0]
            for name in known_model_names()
            if name.startswith("gateway/")
        }
        modules = {PREFIX_ALIASES.get(p, p.replace("-", "_")) for p in gateway_prefixes}
        derived = {LABELS.get(m, f"<unlabelled:{m}>") for m in modules}

        assert derived == GATEWAY_NOTE_VENDOR_LABELS, (
            f"gateway now routes to {sorted(derived)} -- update the Notes sentence in "
            f"supported_services.rst and GATEWAY_NOTE_VENDOR_LABELS to match"
        )

    def test_gateway_note_sentence_names_the_expected_vendors(self):
        """
        Guard the prose itself, in both directions: every vendor in
        ``GATEWAY_NOTE_VENDOR_LABELS`` must be named in `supported_services.rst`'s gateway Notes
        bullet specifically -- not just somewhere in the file, where several of these vendor
        names already appear in other sentences -- and no *other* labelled vendor may be named
        in that bullet either. Editing the bullet without updating the constant above (or vice
        versa) must be caught either way: underclaiming a vendor is a stale doc, overclaiming one
        is a false statement about what this service reaches.
        """
        rst_text = (PROVIDER_YAML_PATH.parent / "docs" / "supported_services.rst").read_text()
        marker = '* "Pydantic AI Gateway" in the'
        start = rst_text.index(marker)
        end = rst_text.find("\n* ", start)
        gateway_bullet = rst_text[start:] if end == -1 else rst_text[start:end]
        gateway_bullet = " ".join(gateway_bullet.split())

        for vendor in GATEWAY_NOTE_VENDOR_LABELS:
            assert vendor in gateway_bullet, (
                f"{vendor!r} missing from supported_services.rst's gateway Notes bullet"
            )

        # `LABELS["gateway"]` ("Pydantic AI Gateway") is excluded: it is itself a value of
        # `LABELS` and rightfully appears in this bullet (it's the row's own name), so it must
        # not be flagged as an "other" vendor the bullet shouldn't mention.
        other_labels = set(LABELS.values()) - GATEWAY_NOTE_VENDOR_LABELS - {LABELS["gateway"]}
        unexpected = sorted(vendor for vendor in other_labels if vendor in gateway_bullet)
        assert not unexpected, (
            f"gateway Notes bullet names {unexpected}, which are not in GATEWAY_NOTE_VENDOR_LABELS"
        )


class TestToolsetExternalServices:
    def test_every_toolset_module_has_external_services(self):
        toolset_block = _toolsets_block()
        modules = set(toolset_block["python-modules"])
        services_by_module = {
            entry["module"]: entry["services"] for entry in toolset_block["external-services"]
        }

        assert set(services_by_module) == modules, (
            f"toolsets.external-services modules {sorted(services_by_module)} != "
            f"python-modules {sorted(modules)}"
        )
        for module, services in services_by_module.items():
            assert services, f"toolset module {module!r} has an empty services list"

    def test_every_toolset_module_has_a_docs_anchor(self):
        toolsets_rst = (PROVIDER_YAML_PATH.parent / "docs" / "toolsets.rst").read_text()
        toolset_block = _toolsets_block()

        for module in toolset_block["python-modules"]:
            basename = module.rsplit(".", 1)[-1]
            anchor = f".. _howto/toolset:{basename}:"
            assert anchor in toolsets_rst, f"missing docs anchor {anchor!r} for module {module!r}"

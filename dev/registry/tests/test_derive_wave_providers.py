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
"""Unit tests for dev/registry/derive_wave_providers.py."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from derive_wave_providers import META_TOKENS, NON_PROVIDER_TOKENS, derive

AIRFLOW_ROOT = Path(__file__).parents[3]
GLOBAL_CONSTANTS = AIRFLOW_ROOT / "dev" / "breeze" / "src" / "airflow_breeze" / "global_constants.py"
PUBLISH_DOCS_WORKFLOW = AIRFLOW_ROOT / ".github" / "workflows" / "publish-docs-to-s3.yml"
PUBLISH_DOCS_TO_S3 = (
    AIRFLOW_ROOT / "dev" / "breeze" / "src" / "airflow_breeze" / "utils" / "publish_docs_to_s3.py"
)


def fake_git(wave_tags=None, provider_tags=None):
    """Build a git_runner that returns canned outputs based on the args."""
    wave_tags = wave_tags or []
    provider_tags = provider_tags or []

    def runner(*args):
        if args[:2] == ("tag", "--list") and "providers/[0-9]*-*-*" in args:
            return list(wave_tags)
        if args[:2] == ("tag", "--merged"):
            return list(provider_tags)
        raise AssertionError(f"unexpected git invocation: {args}")

    return runner


WAVE_TAGS = [
    "providers/2026-04-21",
    "providers/2026-04-12",
    "providers/2026-03-17",
]

WAVE_PROVIDER_TAGS = [
    "providers-amazon/9.26.0",
    "providers-amazon/9.26.0rc1",
    "providers-google/21.2.0",
    "providers-google/21.2.0rc1",
    "providers-vespa/0.1.0",
    "providers-microsoft-azure/13.1.2",
    "providers-microsoft-azure/13.1.2rc1",
]


@pytest.mark.parametrize(
    ("include_docs", "ref", "expected_providers", "expected_full_build", "log_substr"),
    [
        # Standard wave dispatch: derives from git tags.
        (
            "all-providers apache-airflow-providers",
            "providers/2026-04-21",
            "amazon google microsoft-azure vespa",
            False,
            "Derived wave providers (providers/2026-04-12 -> providers/2026-04-21)",
        ),
        # Meta-token but ref is RC-suffixed (not a wave tag).
        (
            "all-providers apache-airflow-providers",
            "providers/2026-04-21-rc1",
            "",
            True,
            "not a wave-tag pattern",
        ),
        # Meta-token but ref is main.
        ("all-providers", "main", "", True, "not a wave-tag pattern"),
        # Meta-token, ref is sha.
        ("apache-airflow-providers", "f8a258069f", "", True, "not a wave-tag pattern"),
        # Explicit packages -- incremental path.
        ("amazon google", "providers/2026-04-21", "amazon google", False, ""),
        # Explicit with full-package-name token.
        (
            "apache-airflow-providers-amazon",
            "providers-amazon/9.27.0",
            "amazon",
            False,
            "",
        ),
        # Explicit with dot-to-hyphen conversion.
        ("common.ai", "any", "common-ai", False, ""),
        # Explicit with non-provider tokens stripped.
        ("apache-airflow apache-airflow-mypy helm-chart docker-stack", "any", "", False, ""),
        # Explicit dedup.
        ("amazon amazon google", "any", "amazon google", False, ""),
        # Empty.
        ("", "any", "", False, ""),
        # Language-SDK doc packages are distributions, not providers. Passing one
        # through as a provider ID fails registry extraction (`provider(s) not found
        # in provider.yaml files`) and blocks the docs publish.
        ("ts-sdk", "any", "", False, ""),
        ("java-sdk ts-sdk task-sdk", "any", "", False, ""),
        # A real provider alongside an SDK token still rebuilds, minus the SDK.
        ("ts-sdk amazon", "any", "amazon", False, ""),
    ],
)
def test_derive(include_docs, ref, expected_providers, expected_full_build, log_substr):
    git = fake_git(wave_tags=WAVE_TAGS, provider_tags=WAVE_PROVIDER_TAGS)
    providers, full_build, log = derive(include_docs, ref, git_runner=git)
    assert providers == expected_providers
    assert full_build is expected_full_build
    if log_substr:
        assert log_substr in log


def test_first_wave_no_predecessor():
    # Ref IS a wave tag but there's no predecessor in the list.
    git = fake_git(wave_tags=["providers/2026-01-15"], provider_tags=[])
    providers, full_build, log = derive("all-providers", "providers/2026-01-15", git_runner=git)
    assert providers == ""
    assert full_build is True
    assert "::warning::No predecessor wave tag found" in log


def test_wave_with_no_new_provider_tags():
    # Ref is a known wave tag with predecessor, but no per-provider tags
    # between them (suspect: race with tag push, or no-op republish).
    git = fake_git(
        wave_tags=["providers/2026-05-19", "providers/2026-04-21"],
        provider_tags=[],
    )
    providers, full_build, log = derive("all-providers", "providers/2026-05-19", git_runner=git)
    assert providers == ""
    assert full_build is True
    assert "::warning::Wave ref providers/2026-05-19 has no new per-provider tags" in log


def test_only_rc_tags_between_waves():
    # All per-provider tags between the two waves are RCs -- treated as no
    # new finals, falls back to full rebuild with warning.
    git = fake_git(
        wave_tags=["providers/2026-05-19", "providers/2026-04-21"],
        provider_tags=["providers-amazon/9.27.0rc1", "providers-google/21.3.0rc1"],
    )
    providers, full_build, log = derive("all-providers", "providers/2026-05-19", git_runner=git)
    assert providers == ""
    assert full_build is True
    assert "::warning::" in log


def test_meta_token_with_explicit_provider():
    # Meta-token wins -- explicit provider tokens are dropped.
    git = fake_git(wave_tags=WAVE_TAGS, provider_tags=WAVE_PROVIDER_TAGS)
    providers, full_build, log = derive("amazon all-providers", "providers/2026-04-21", git_runner=git)
    assert providers == "amazon google microsoft-azure vespa"
    assert full_build is False


def test_glob_matches_non_wave_tag_is_filtered_out():
    # The git glob `providers/[0-9]*-*-*` is broader than the strict
    # `providers/YYYY-MM-DD` regex and matches release-with-date tags like
    # `providers/2.11/2026-02-16` (Airflow 2.11 release on that date).
    # Predecessor lookup must skip those so it picks the actual prior wave.
    git = fake_git(
        wave_tags=[
            "providers/2026-02-20",
            "providers/2.11/2026-02-16",  # noise: not a wave tag
            "providers/2026-02-10",
        ],
        provider_tags=["providers-amazon/9.20.0"],
    )
    providers, full_build, log = derive("all-providers", "providers/2026-02-20", git_runner=git)
    # Predecessor must be the real prior wave (2026-02-10), not the noise.
    assert providers == "amazon"
    assert full_build is False
    assert "providers/2026-02-10 -> providers/2026-02-20" in log


def _string_collection_constant(path: Path, name: str) -> set[str]:
    """Parse a collection-of-strings constant out of a Python file via AST.

    `derive_wave_providers.py` runs under bare `python3` on the runner, where
    breeze is not installed, so these constants are parsed rather than imported.

    Anything but a flat literal of string constants raises. Skipping an element
    we cannot read would shrink both sides of the comparisons below in step, so
    the guard would pass vacuously on exactly the drift it exists to catch.
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            targets: list[ast.expr] = list(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        if not any(isinstance(target, ast.Name) and target.id == name for target in targets):
            continue
        if not isinstance(node.value, (ast.List, ast.Tuple, ast.Set)):
            raise AssertionError(f"{name} in {path} is not a list/tuple/set literal")
        values = set()
        for elt in node.value.elts:
            if not isinstance(elt, ast.Constant) or not isinstance(elt.value, str):
                raise AssertionError(f"{name} in {path} has a non-literal element: {ast.unparse(elt)}")
            values.add(elt.value)
        return values
    raise AssertionError(f"{name} not found in {path}")


def _workflow_non_provider_tokens() -> set[str]:
    """The NON_PROVIDER_TOKENS bash array from publish-docs-to-s3.yml."""
    match = re.search(
        r"^\s*NON_PROVIDER_TOKENS=\((.*?)\)\s*$",
        PUBLISH_DOCS_WORKFLOW.read_text(),
        re.MULTILINE | re.DOTALL,
    )
    if not match:
        raise AssertionError(f"NON_PROVIDER_TOKENS array not found in {PUBLISH_DOCS_WORKFLOW}")
    return set(re.findall(r'"([^"]+)"', match.group(1)))


def test_non_provider_tokens_match_breeze_doc_packages():
    # A doc package added to breeze but not here is parsed as a provider ID and
    # fails registry extraction, which is how ts-sdk (#70812) broke docs publishing.
    regular_doc_packages = _string_collection_constant(GLOBAL_CONSTANTS, "REGULAR_DOC_PACKAGES")
    assert regular_doc_packages - META_TOKENS == NON_PROVIDER_TOKENS


def test_workflow_non_provider_list_matches_script():
    assert _workflow_non_provider_tokens() == NON_PROVIDER_TOKENS


def test_breeze_publish_docs_non_short_names_match_script():
    # A third copy, with the same "not in the list means expand to
    # apache-airflow-providers-{}" semantics. In sync today; pinned so it stays that way.
    non_short_name_packages = _string_collection_constant(PUBLISH_DOCS_TO_S3, "NON_SHORT_NAME_PACKAGES")
    assert non_short_name_packages == NON_PROVIDER_TOKENS

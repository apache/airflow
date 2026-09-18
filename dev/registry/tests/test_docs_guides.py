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

from unittest.mock import patch

import pytest
from extract_parameters import read_guide_docs as read_guide_docs_from_worktree
from extract_versions import read_guide_docs as read_guide_docs_from_tag
from registry_tools.docs_guides import (
    attach_guide_urls,
    collect_guide_anchors,
    is_guide_page,
    slugify_section_anchor,
)

TOOLSETS_GUIDE = """
.. _howto/toolsets:

Toolsets: Airflow hooks as AI agent tools
==========================================

Intro prose.

``HookToolset``
---------------

How to use it.

Guidelines
^^^^^^^^^^

More prose.

.. _bounded-query-results:

Bounded query results
^^^^^^^^^^^^^^^^^^^^^

``SQLToolset`` bounds that.

``DataFusionToolset``
---------------------

Another one.
"""


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        # Verified against the published guide: the section titled ``HookToolset``
        # is served at .../toolsets.html#hooktoolset.
        ("HookToolset", "hooktoolset"),
        ("AgentSkillsToolset", "agentskillstoolset"),
        ("Bounded query results", "bounded-query-results"),
        ("Agent_Skills", "agent-skills"),
        ("Direct PydanticAI MCP toolsets", "direct-pydanticai-mcp-toolsets"),
        ("``AgentOperator`` & ``@task.agent``", "agentoperator-task-agent"),
    ],
)
def test_slugify_section_anchor_matches_sphinx_ids(title, expected):
    assert slugify_section_anchor(title) == expected


def test_collect_guide_anchors_finds_class_named_sections_at_any_depth():
    anchors = collect_guide_anchors({"toolsets.rst": TOOLSETS_GUIDE})

    assert anchors == {
        "HookToolset": "toolsets.html#hooktoolset",
        "DataFusionToolset": "toolsets.html#datafusiontoolset",
    }


def test_collect_guide_anchors_ignores_prose_headings():
    anchors = collect_guide_anchors({"toolsets.rst": TOOLSETS_GUIDE})

    # "Guidelines" is shaped like a class name but isn't marked up as one.
    assert "Guidelines" not in anchors
    assert "Bounded query results" not in anchors


def test_collect_guide_anchors_ignores_classes_only_mentioned_in_prose():
    # SQLToolset appears in the guide's body but has no section of its own, so
    # there is no anchor to link to.
    assert "SQLToolset" not in collect_guide_anchors({"toolsets.rst": TOOLSETS_GUIDE})


def test_collect_guide_anchors_keeps_nested_page_paths():
    guide = "``AgentOperator``\n-----------------\n\nProse.\n"

    assert collect_guide_anchors({"operators/agent.rst": guide}) == {
        "AgentOperator": "operators/agent.html#agentoperator"
    }


def test_collect_guide_anchors_prefers_first_page_in_sorted_order():
    guide = "``SQLToolset``\n--------------\n\nProse.\n"

    anchors = collect_guide_anchors({"toolsets.rst": guide, "operators/sql.rst": guide})

    assert anchors["SQLToolset"] == "operators/sql.html#sqltoolset"


def test_collect_guide_anchors_handles_a_title_covering_more_than_the_class():
    # Verified against the published guide: this heading is served at
    # .../operators/agent.html#agentoperator-task-agent, so the anchor comes from
    # the whole title while both the operator and its decorator get linked to it.
    guide = "``AgentOperator`` & ``@task.agent``\n===================================\n\nProse.\n"

    assert collect_guide_anchors({"operators/agent.rst": guide}) == {
        "AgentOperator": "operators/agent.html#agentoperator-task-agent",
        "@task.agent": "operators/agent.html#agentoperator-task-agent",
    }


def test_collect_guide_anchors_links_a_decorator_name_with_underscores():
    # ``@task.llm_file_analysis`` is the real decorator name for the common.ai
    # provider's LLMFileAnalysisOperator; the leading-literal charset must admit
    # "@" and "." for it to ever get a link.
    guide = (
        "``LLMFileAnalysisOperator`` & ``@task.llm_file_analysis``\n"
        "==========================================================\n\n"
        "Prose.\n"
    )

    assert collect_guide_anchors({"operators/llm_file_analysis.rst": guide}) == {
        "LLMFileAnalysisOperator": (
            "operators/llm_file_analysis.html#llmfileanalysisoperator-task-llm-file-analysis"
        ),
        "@task.llm_file_analysis": (
            "operators/llm_file_analysis.html#llmfileanalysisoperator-task-llm-file-analysis"
        ),
    }


def test_collect_guide_anchors_ignores_a_prose_title_mentioning_a_literal():
    # The title doesn't *open* with the literal, so nothing after "Using" should
    # ever be scanned for further inline literals -- a prose heading that happens
    # to mention one in passing must not produce a link.
    guide = "Using ``foo`` in a pipeline\n============================\n\nProse.\n"

    assert collect_guide_anchors({"toolsets.rst": guide}) == {}


def test_collect_guide_anchors_requires_a_long_enough_underline():
    # An underline shorter than the title isn't a section in reST, so it must not
    # produce a link to an anchor Sphinx never emitted.
    assert collect_guide_anchors({"toolsets.rst": "``HookToolset``\n---\n\nProse.\n"}) == {}


def test_attach_guide_urls_only_links_documented_classes():
    modules = [
        {"name": "HookToolset", "docs_url": "https://example.test/_api/hook/index.html"},
        {"name": "UndocumentedToolset", "docs_url": "https://example.test/_api/other/index.html"},
    ]

    attached = attach_guide_urls(
        modules,
        {"HookToolset": "toolsets.html#hooktoolset"},
        "https://airflow.apache.org/docs/apache-airflow-providers-common-ai/0.7.0",
    )

    assert attached == 1
    assert modules[0]["guide_url"] == (
        "https://airflow.apache.org/docs/apache-airflow-providers-common-ai/0.7.0/toolsets.html#hooktoolset"
    )
    assert "guide_url" not in modules[1]


def test_attach_guide_urls_does_not_double_up_the_base_separator():
    modules = [{"name": "HookToolset"}]

    attach_guide_urls(modules, {"HookToolset": "toolsets.html#hooktoolset"}, "https://example.test/docs/")

    assert modules[0]["guide_url"] == "https://example.test/docs/toolsets.html#hooktoolset"


@pytest.mark.parametrize(
    ("relative_path", "expected"),
    [
        # A `_`-prefixed path segment marks autoapi/partial content, at any
        # depth. Mutation canary: removing the `_` check turns these four
        # from False back to True.
        ("_api/index.rst", False),
        ("_api/hook/index.rst", False),
        ("operators/_partials/foo.rst", False),
        ("_partials/foo.rst", False),
        # Real, built release-note pages, not how-to guides. Mutation canary:
        # removing the changelog/commits check turns these two from False
        # back to True.
        ("changelog.rst", False),
        ("commits.rst", False),
        ("toolsets.rst", True),
        ("operators/agent.rst", True),
    ],
)
def test_is_guide_page(relative_path, expected):
    assert is_guide_page(relative_path) == expected


def test_readers_agree_on_which_pages_are_guides(tmp_path):
    """Both `read_guide_docs` implementations delegate to `is_guide_page`, so a
    working-tree read and a git-tag read of the same paths must end up with the
    same set of pages -- regardless of which source produced them."""
    relative_paths = ["_api/index.rst", "changelog.rst", "commits.rst", "toolsets.rst"]
    for relative in relative_paths:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("Prose.\n")

    from_worktree = read_guide_docs_from_worktree(tmp_path)

    docs_prefix = "providers/test/docs/"
    with (
        patch(
            "extract_versions.git_ls_tree",
            autospec=True,
            return_value=[docs_prefix + relative for relative in relative_paths],
        ),
        patch("extract_versions.git_show", autospec=True, return_value="Prose.\n"),
    ):
        from_tag = read_guide_docs_from_tag("providers-test/1.0.0", "new", "test")

    # Mutation canary: reverting the `is_guide_page` call in only one of the
    # two readers (e.g. extract_parameters.read_guide_docs but not
    # extract_versions.read_guide_docs) fails this assertion even though each
    # reader's own test (test_extract_parameters / test_extract_versions) may
    # still pass on its own.
    assert set(from_worktree) == set(from_tag) == {"toolsets.rst"}

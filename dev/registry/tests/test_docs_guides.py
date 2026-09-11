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

import pytest
from registry_tools.docs_guides import (
    attach_guide_urls,
    collect_guide_anchors,
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
    # the whole title while the class name comes from the leading literal.
    guide = "``AgentOperator`` & ``@task.agent``\n===================================\n\nProse.\n"

    assert collect_guide_anchors({"operators/agent.rst": guide}) == {
        "AgentOperator": "operators/agent.html#agentoperator-task-agent"
    }


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

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
"""Example DAGs demonstrating Agent Skills with ``AgentOperator``.

`Agent Skills <https://agentskills.io>`__ are ``SKILL.md`` bundles the model
discovers and loads on demand (progressive disclosure). They are passed to the
agent as an ``AgentSkillsToolset`` in the operator's ``toolsets=`` list. Skill
sources are resolved when the task runs, on the worker (not while the DAG
processor parses the file), so a Git token resolved from an Airflow connection
is never baked into the serialized DAG.

These DAGs need the optional ``skills`` extra::

    pip install "apache-airflow-providers-common-ai[skills]"
"""

from __future__ import annotations

from pathlib import Path

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.skills import GitSkills
from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import dag

# Skills ship next to this DAG file; resolve relative to __file__ so the path
# holds regardless of the dag-processor's working directory.
SKILLS_DIR = Path(__file__).parent / "skills"


# ---------------------------------------------------------------------------
# 1. Local filesystem skills (a directory of SKILL.md bundles)
# ---------------------------------------------------------------------------


# [START howto_operator_agent_skills_local]
@dag(tags=["example"])
def example_agent_skills_local():
    AgentOperator(
        task_id="reporter",
        prompt="How many orders did our top 5 customers place last month?",
        llm_conn_id="pydanticai_default",
        system_prompt="You are a data analyst. Consult your skills before writing SQL.",
        toolsets=[
            AgentSkillsToolset(sources=[str(SKILLS_DIR)]),
            SQLToolset(
                db_conn_id="postgres_default",
                allowed_tables=["customers", "orders"],
                max_rows=50,
            ),
        ],
    )


# [END howto_operator_agent_skills_local]

example_agent_skills_local()


# ---------------------------------------------------------------------------
# 2. Remote skills from a Git repo, credentials from an Airflow connection
# ---------------------------------------------------------------------------
# ``github_skills`` is a git connection (HTTPS token in the password, or an SSH
# key in the extra). The DAG only references it by id; no credential is inlined.


# [START howto_operator_agent_skills_git]
@dag(tags=["example"])
def example_agent_skills_git():
    AgentOperator(
        task_id="support_agent",
        prompt="Summarize our refund policy and apply it to order 12345.",
        llm_conn_id="pydanticai_default",
        system_prompt="You are a support agent. Load the relevant skill before answering.",
        toolsets=[
            AgentSkillsToolset(
                sources=[
                    GitSkills(
                        repo_url="https://github.com/my-org/agent-skills",
                        conn_id="github_skills",
                        path="skills",
                    ),
                ],
            ),
        ],
    )


# [END howto_operator_agent_skills_git]

example_agent_skills_git()

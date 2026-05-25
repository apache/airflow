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
"""Example DAGs demonstrating Strands agent hooks, AgentOperator, and skills."""

from __future__ import annotations

from airflow.providers.common.ai.hooks.base_ai import AgentRunRequest, BaseAIHook, SkillSpec
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import Variable, dag, task

# Requires: pip install 'apache-airflow-providers-common-ai[strands]'
# Connection: strands_default (conn_type selects backend — e.g. strands-gemini for Google Gemini)
# Variable: strands_skill_path — filesystem path to a skill directory (contains SKILL.md)

# ---------------------------------------------------------------------------
# 1. Basic AgentOperator with Strands
# ---------------------------------------------------------------------------


# [START howto_operator_strands_basic]
@dag(schedule=None, tags=["example"])
def example_strands_basic():
    AgentOperator(
        task_id="summarize",
        prompt="Summarize the key benefits of using Airflow for data pipelines.",
        llm_conn_id="strands_default",
        system_prompt="You are a concise technical writer.",
    )


# [END howto_operator_strands_basic]

example_strands_basic()


# ---------------------------------------------------------------------------
# 2. AgentOperator with inline skills + skills_params
# ---------------------------------------------------------------------------


# [START howto_operator_strands_skills]
@dag(schedule=None, tags=["example"])
def example_strands_skills():
    AgentOperator(
        task_id="process_pdf",
        prompt="Extract tables from report.pdf and summarize the findings.",
        llm_conn_id="strands_default",
        system_prompt="You are a document processing assistant.",
        skills=[
            SkillSpec(
                name="pdf-processing",
                description="Extract and summarize structured content from PDF documents.",
                instructions=(
                    "Identify tables and key sections first. Summarize findings in plain language."
                ),
            )
        ],
        skills_params={"strict": True, "max_resource_files": 20},
    )


# [END howto_operator_strands_skills]

example_strands_skills()


# ---------------------------------------------------------------------------
# 3. AgentOperator with filesystem skill path (Airflow Variable)
# ---------------------------------------------------------------------------


# [START howto_operator_strands_skills_path]
@dag(schedule=None, tags=["example"])
def example_strands_skills_path():
    AgentOperator(
        task_id="research",
        prompt="Summarize recent developments in workflow orchestration.",
        llm_conn_id="strands_default",
        system_prompt="You are a helpful research assistant.",
        skills=[Variable.get("strands_skill_path")],
        skills_params={"strict": True},
    )


# [END howto_operator_strands_skills_path]

example_strands_skills_path()


# ---------------------------------------------------------------------------
# 4. Inline SkillSpec + SQL toolset
# ---------------------------------------------------------------------------


# [START howto_operator_strands_skill_spec]
@dag(schedule=None, tags=["example"])
def example_strands_skill_spec():
    AgentOperator(
        task_id="sql_analyst",
        prompt="What are the top 5 customers by order count?",
        llm_conn_id="strands_default",
        system_prompt="You are a SQL analyst. Use tools to explore the schema and answer with data.",
        skills=[
            SkillSpec(
                name="sql-analyst",
                description="Analyze relational data with SQL best practices.",
                instructions=(
                    "Always list tables before querying. Prefer read-only SELECT statements. "
                    "Summarize results in plain language."
                ),
            )
        ],
        toolsets=[
            SQLToolset(
                db_conn_id="postgres_default",
                allowed_tables=["customers", "orders"],
                max_rows=20,
            )
        ],
    )


# [END howto_operator_strands_skill_spec]

example_strands_skill_spec()


# ---------------------------------------------------------------------------
# 5. Direct hook usage with AgentRunRequest
# ---------------------------------------------------------------------------


# [START howto_hook_strands_basic]
@dag(schedule=None, tags=["example"])
def example_strands_hook():
    @task
    def ask_agent(question: str) -> str:
        hook = BaseAIHook.get_agent_hook("strands_default")
        request = AgentRunRequest(
            prompt=question,
            instructions="Answer concisely in two sentences or fewer.",
        )
        agent = hook.create_agent(request)
        result = hook.run_agent(agent, request)
        return result.output

    ask_agent("What is Apache Airflow used for?")


# [END howto_hook_strands_basic]

example_strands_hook()


# ---------------------------------------------------------------------------
# 6. @task.agent decorator with Strands
# ---------------------------------------------------------------------------


# [START howto_decorator_strands]
@dag(schedule=None, tags=["example"])
def example_strands_decorator():
    @task.agent(
        llm_conn_id="strands_default",
        system_prompt="You are a helpful research assistant.",
        skills=[
            SkillSpec(
                name="web-research",
                description="Research topics using structured, concise summaries.",
                instructions=(
                    "Break the question into subtopics. Cite assumptions clearly and keep answers factual."
                ),
            )
        ],
    )
    def research(question: str):
        return f"Research and answer: {question}"

    research("What are the latest trends in workflow orchestration?")


# [END howto_decorator_strands]

example_strands_decorator()

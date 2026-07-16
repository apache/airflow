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
"""Example DAGs demonstrating AgentOperator, @task.agent, and toolsets."""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.hook import HookToolset
from airflow.providers.common.compat.sdk import ObjectStoragePath, dag, task

try:
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
except Exception:
    SQLToolset = None  # type: ignore[assignment,misc]


# [START howto_decorator_agent_structured_output_class]
# Pydantic output classes must be defined at module scope so downstream
# tasks can re-import them when deserializing the XCom payload.
class Analysis(BaseModel):
    """Structured analysis output for the agent example."""

    summary: str
    top_items: list[str]
    row_count: int


# [END howto_decorator_agent_structured_output_class]


# ---------------------------------------------------------------------------
# 1. SQL Agent: answer a question using database tools
# ---------------------------------------------------------------------------


# [START howto_operator_agent_sql]
if SQLToolset is not None:

    @dag(tags=["example"])
    def example_agent_operator_sql():
        AgentOperator(
            task_id="analyst",
            prompt="What are the top 5 customers by order count?",
            llm_conn_id="pydanticai_default",
            system_prompt=(
                "You are a SQL analyst. Use the available tools to explore "
                "the schema and answer the question with data."
            ),
            toolsets=[
                SQLToolset(
                    db_conn_id="postgres_default",
                    allowed_tables=["customers", "orders"],
                    max_rows=20,
                )
            ],
        )

    # [END howto_operator_agent_sql]

    example_agent_operator_sql()


# ---------------------------------------------------------------------------
# 2. Hook-based tools: wrap an existing hook for the agent
# ---------------------------------------------------------------------------


# [START howto_operator_agent_hook]
@dag(tags=["example"])
def example_agent_operator_hook():
    from airflow.providers.http.hooks.http import HttpHook

    http_hook = HttpHook(http_conn_id="my_api")

    AgentOperator(
        task_id="api_explorer",
        prompt="What endpoints are available and what does /status return?",
        llm_conn_id="pydanticai_default",
        system_prompt="You are an API explorer. Use the tools to discover and call endpoints.",
        toolsets=[
            HookToolset(
                http_hook,
                allowed_methods=["run"],
                tool_name_prefix="http_",
            )
        ],
    )


# [END howto_operator_agent_hook]

example_agent_operator_hook()


# ---------------------------------------------------------------------------
# 2b. Hook-based tools against a GET-only endpoint (self-hosted models tutorial)
# ---------------------------------------------------------------------------


# [START howto_agent_self_hosted]
@dag(tags=["example"])
def example_agent_self_hosted():
    from airflow.providers.http.hooks.http import HttpHook

    # The OpenAI-compatible model list (GET /v1/models) is GET-only; HttpHook defaults to POST.
    http_hook = HttpHook(http_conn_id="my_api", method="GET")

    AgentOperator(
        task_id="list_models",
        prompt="Which models are available?",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are an API assistant. Use the tools to answer questions; "
            "the server's model list is served at GET /v1/models."
        ),
        toolsets=[
            HookToolset(
                http_hook,
                allowed_methods=["run"],
                tool_name_prefix="http_",
            )
        ],
    )


# [END howto_agent_self_hosted]

example_agent_self_hosted()


# ---------------------------------------------------------------------------
# 3. @task.agent decorator with dynamic prompt
# ---------------------------------------------------------------------------


# [START howto_decorator_agent]
if SQLToolset is not None:

    @dag(tags=["example"])
    def example_agent_decorator():
        @task.agent(
            llm_conn_id="pydanticai_default",
            system_prompt="You are a data analyst. Use tools to answer questions.",
            toolsets=[
                SQLToolset(
                    db_conn_id="postgres_default",
                    allowed_tables=["orders"],
                )
            ],
        )
        def analyze(question: str):
            return f"Answer this question about our orders data: {question}"

        analyze("What was our total revenue last month?")

    # [END howto_decorator_agent]

    example_agent_decorator()


# ---------------------------------------------------------------------------
# 4. Structured output — agent returns a Pydantic model
# ---------------------------------------------------------------------------


# [START howto_decorator_agent_structured]
if SQLToolset is not None:

    @dag(tags=["example"])
    def example_agent_structured_output():
        @task.agent(
            llm_conn_id="pydanticai_default",
            system_prompt="You are a data analyst. Return structured results.",
            output_type=Analysis,
            toolsets=[SQLToolset(db_conn_id="postgres_default")],
        )
        def analyze(question: str):
            return f"Analyze: {question}"

        analyze("What are the trending products this week?")

    # [END howto_decorator_agent_structured]

    example_agent_structured_output()


# ---------------------------------------------------------------------------
# 5. Chaining: agent output feeds into downstream tasks via XCom
# ---------------------------------------------------------------------------


# [START howto_agent_chain]
if SQLToolset is not None:

    @dag(tags=["example"])
    def example_agent_chain():
        @task.agent(
            llm_conn_id="pydanticai_default",
            system_prompt="You are a SQL analyst.",
            toolsets=[SQLToolset(db_conn_id="postgres_default", allowed_tables=["orders"])],
        )
        def investigate(question: str):
            return f"Investigate: {question}"

        @task
        def send_report(analysis: str):
            """Send the agent's analysis to a downstream system."""
            print(f"Report: {analysis}")
            return analysis

        result = investigate("Summarize order trends for last quarter")
        send_report(result)

    # [END howto_agent_chain]

    example_agent_chain()


# ---------------------------------------------------------------------------
# 6. HITL Review: human approves/rejects agent output before downstream runs
# ---------------------------------------------------------------------------


# [START howto_operator_agent_hitl_review]
@dag(tags=["example"])
def example_agent_operator_hitl_review():
    """AgentOperator with HITL review — a human approves output via hitl-review plugin UI."""
    AgentOperator(
        task_id="summarize_with_review",
        prompt="Summarize the Q4 sales report in 3 bullet points.",
        llm_conn_id="pydantic_ai_default",
        system_prompt="You are a concise business analyst.",
        enable_hitl_review=True,
        max_hitl_iterations=5,
        hitl_timeout=timedelta(minutes=30),
        hitl_poll_interval=10.0,
    )


# [END howto_operator_agent_hitl_review]

example_agent_operator_hitl_review()


# ---------------------------------------------------------------------------
# 7. Code mode: the model writes Python that calls tools, run in the Monty sandbox
# ---------------------------------------------------------------------------


# [START howto_operator_agent_code_mode]
if SQLToolset is not None:

    @dag(tags=["example"])
    def example_agent_operator_code_mode():
        AgentOperator(
            task_id="code_mode_analyst",
            prompt="For the top 3 customers by order count, what was each one's total spend?",
            llm_conn_id="pydanticai_default",
            system_prompt="You are a SQL analyst. Write Python that calls the tools to answer.",
            toolsets=[SQLToolset(db_conn_id="postgres_default", allowed_tables=["customers", "orders"])],
            # Requires the `code-mode` extra:
            #   pip install "apache-airflow-providers-common-ai[code-mode]"
            code_mode=True,
        )

    # [END howto_operator_agent_code_mode]

    example_agent_operator_code_mode()


# ---------------------------------------------------------------------------
# 8. Multi-turn session — resume a conversation across DAG runs
# ---------------------------------------------------------------------------


# [START howto_agent_session]
@dag(tags=["example"], params={"session_id": "demo-session"})
def example_agent_session():
    """Resume a conversation across runs via ``message_history``.

    The agent step seeds itself with the prior transcript and re-emits the
    updated transcript to XCom (key ``message_history``). Loading and storing
    that transcript under a session key is the DAG's job -- here, a JSON file in
    object storage keyed by ``session_id``. Swap the path for ``s3://`` /
    ``gs://`` in a deployment.
    """
    sessions_root = ObjectStoragePath("file:///tmp/airflow_agent_sessions")

    @task
    def load_history(session_id: str) -> str:
        path = sessions_root / f"{session_id}.json"
        # First turn: no file yet -> start a fresh session (empty transcript).
        return path.read_text() if path.exists() else "[]"

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a helpful assistant. Use the earlier turns for context.",
        # The XComArg both wires the dependency and resolves to the JSON transcript.
        message_history=load_history("{{ params.session_id }}"),
    )
    def ask(question: str) -> str:
        return question

    @task
    def save_history(session_id: str, transcript: str) -> None:
        # Local/fsspec object storage does not auto-create parent dirs on write.
        sessions_root.mkdir(parents=True, exist_ok=True)
        (sessions_root / f"{session_id}.json").write_text(transcript)

    answer = ask("And what did I ask you a moment ago?")
    saved = save_history(
        "{{ params.session_id }}",
        # The agent step pushes the post-run transcript under this XCom key.
        "{{ ti.xcom_pull(task_ids='ask', key='message_history') }}",
    )
    # save runs after the agent so the pulled transcript is the fresh one.
    answer >> saved


# [END howto_agent_session]

example_agent_session()


# ---------------------------------------------------------------------------
# 9. Dynamic system prompt: template system_prompt from an upstream task's XCom
# ---------------------------------------------------------------------------


# [START howto_agent_dynamic_system_prompt]
@dag(tags=["example"])
def example_agent_dynamic_system_prompt():
    @task
    def classify(ticket: str) -> dict:
        category = "shipping" if "order" in ticket.lower() else "other"
        return {"priority": "high", "category": category}

    @task.agent(
        llm_conn_id="pydanticai_default",
        # system_prompt is a templated field -- Jinja renders it at task-run
        # time, pulling the classification an upstream task already computed.
        system_prompt=(
            "You are handling a {{ ti.xcom_pull(task_ids='classify')['priority'] }}-priority "
            "'{{ ti.xcom_pull(task_ids='classify')['category'] }}' ticket. "
            "Draft a concise, friendly reply."
        ),
    )
    def draft_reply(ticket: str, triage: dict) -> str:
        # `triage` creates the task dependency; its content also flows into
        # system_prompt via Jinja above. The returned string is the *prompt*
        # sent to the agent -- the drafted reply is this task's XCom output.
        return f"Draft a reply for: {ticket}"

    ticket = "Where is my order? It still hasn't shipped."
    draft_reply(ticket, classify(ticket))


# [END howto_agent_dynamic_system_prompt]

example_agent_dynamic_system_prompt()

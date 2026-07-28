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
ReAct tool-calling agent with LangChain -- research and report pipeline.

Demonstrates the "agent as a task" pattern using a LangChain ReAct agent
that autonomously decides which tools to call, composed with common.ai's
:class:`~airflow.providers.common.ai.operators.llm.LLMOperator` for report
formatting and AIP-90 HITL operators for human review.

Unlike RAG examples (fixed pipeline: retrieve then synthesize), this
agent's tool-call sequence is determined by the LLM at runtime.  The agent
might call zero tools or ten tools depending on the question.  This is the
canonical "agent as a task" pattern: Airflow handles scheduling, retry,
connections, and the surrounding workflow; the LangChain agent handles
internal reasoning.

``example_langchain_tool_agent`` (manual trigger):

.. code-block:: text

    prompt_review (HITLEntryOperator)
        -> prepare_tools (@task)
        -> run_research_agent (@task)
        -> format_report (LLMOperator)
        -> report_approval (ApprovalOperator)

**What this makes visible that running an agent alone hides:**

* The question goes through human review before the agent runs.
* The agent's raw findings are a visible XCom value between tasks.
* Report formatting is a separate, independently retryable LLM call.
* The formatted report requires human approval before delivery.

**Contrast with AIP-99's AgentOperator:**

AIP-99's ``AgentOperator`` uses PydanticAI for agent execution.  This
example uses LangChain's ``create_agent`` with LangChain-native ``@tool``
definitions.  Users with existing LangChain tools (700+ integrations)
can use them directly without rewriting as PydanticAI tools.

Before running:

1. Install LangChain packages::

       pip install langchain langchain-openai langchain-text-splitters \\
                   langchain-community faiss-cpu

2. Create an LLM connection of type ``langchain`` named ``langchain_default``
   (or the value of ``LLM_CONN_ID`` below) for your chosen model provider.
   Set ``password`` to your API key; the ``host`` field is optional and only
   needed for custom endpoints / Ollama.

3. Optionally place a knowledge base directory at ``DOCS_PATH`` and a
   survey CSV at ``SURVEY_CSV_PATH``.  If ``DOCS_PATH`` is empty, sample
   documents about Apache Airflow are created automatically.
"""

from __future__ import annotations

import datetime
import os

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLEntryOperator
from airflow.sdk import Param

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LLM_CONN_ID = "langchain_default"
LLM_MODEL = os.environ.get("LLM_MODEL", "openai:gpt-4o")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "openai:text-embedding-3-small")

DOCS_PATH = os.environ.get("DOCS_PATH", "/opt/airflow/data/rag_documents")

SURVEY_CSV_PATH = os.environ.get(
    "SURVEY_CSV_PATH",
    "/opt/airflow/data/airflow-user-survey-2025.csv",
)

INDEX_PERSIST_DIR = os.environ.get("INDEX_PERSIST_DIR", "/opt/airflow/data/langchain_faiss_index")

DEFAULT_QUESTION = (
    "What percentage of Airflow users are on Kubernetes? "
    "Also check what the documentation says about the KubernetesExecutor."
)

SAMPLE_DOCUMENTS = {
    "apache_airflow_overview.txt": (
        "Apache Airflow is an open-source platform for programmatically authoring, "
        "scheduling, and monitoring workflows. Originally created at Airbnb in 2014, "
        "it graduated from the Apache Incubator in 2019. Airflow uses directed acyclic "
        "graphs (DAGs) to define workflows as Python code, making pipelines versionable, "
        "testable, and collaborative. The scheduler executes tasks on workers following "
        "the defined dependencies. Airflow is widely used for ETL/ELT pipelines, ML model "
        "training orchestration, and data warehouse management. As of Airflow 3.0, workers "
        "communicate exclusively through the Execution API and never access the metadata "
        "database directly, strengthening security and enabling horizontal scaling."
    ),
    "kubernetes_executor.txt": (
        "The KubernetesExecutor runs each Airflow task as a separate Kubernetes pod. "
        "This provides strong isolation between tasks, dynamic resource allocation, and "
        "the ability to use different Docker images per task. When a task is scheduled, "
        "the executor creates a pod spec, submits it to the Kubernetes API, and monitors "
        "the pod until completion. Resource requests and limits can be set per task via "
        "executor_config. The KubernetesExecutor is recommended for heterogeneous "
        "workloads where tasks have different resource requirements or dependencies. "
        "It scales to zero when no tasks are running, reducing infrastructure costs. "
        "In Airflow 3.0, pod specs are submitted via the Execution API."
    ),
    "operators_and_hooks.txt": (
        "Operators are the building blocks of Airflow tasks. Each operator defines a "
        "single unit of work: BashOperator runs shell commands, PythonOperator executes "
        "Python callables, and provider-specific operators interact with external systems "
        "(S3, BigQuery, Spark, etc.). Hooks are the connection layer between operators "
        "and external services. A hook manages authentication and provides methods to "
        "interact with a specific service. For example, S3Hook provides methods to read "
        "and write S3 objects, while PostgresHook connects to PostgreSQL databases."
    ),
    "connections_and_variables.txt": (
        "Connections store credentials and endpoint information for external services. "
        "Each connection has a type (e.g., postgres, aws, http), login, password, host, "
        "port, schema, and an extras JSON field for additional parameters. In Airflow 3.0, "
        "workers access connections through the Execution API using short-lived JWT tokens "
        "scoped to the running task instance. Variables are key-value pairs for storing "
        "configuration that may change between environments."
    ),
    "ai_operators.txt": (
        "Airflow's common.ai provider (AIP-99) adds first-class AI/LLM support. "
        "LLMOperator sends a prompt to any supported LLM and returns text or structured "
        "output via Pydantic models. AgentOperator runs multi-turn reasoning with tools "
        "(SQL, HTTP, MCP servers). LLMBranchOperator uses an LLM to choose downstream "
        "task branches. All operators support human-in-the-loop review, durable execution "
        "for long-running agents, usage limits for cost control, and connect to 20+ model "
        "providers through Airflow connections."
    ),
}

REPORT_SYSTEM_PROMPT = (
    "You are a technical report writer. Format the research findings into a "
    "clear, well-structured report with sections and bullet points. Cite "
    "sources when available. Be concise but thorough."
)


# ---------------------------------------------------------------------------
# Helper: build or load the knowledge base FAISS index
# ---------------------------------------------------------------------------


def _ensure_knowledge_base(hook) -> str:
    """Build a FAISS index from sample docs if it does not already exist.

    Returns the persist directory path.
    """
    from langchain_community.vectorstores import FAISS
    from langchain_core.documents import Document
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    if os.path.exists(os.path.join(INDEX_PERSIST_DIR, "index.faiss")):
        return INDEX_PERSIST_DIR

    os.makedirs(DOCS_PATH, exist_ok=True)
    for filename, content in SAMPLE_DOCUMENTS.items():
        filepath = os.path.join(DOCS_PATH, filename)
        if not os.path.exists(filepath):
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)

    docs = []
    for filename in sorted(os.listdir(DOCS_PATH)):
        if not filename.endswith((".txt", ".md")):
            continue
        with open(os.path.join(DOCS_PATH, filename), encoding="utf-8") as f:
            docs.append(Document(page_content=f.read(), metadata={"source": filename}))

    splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)
    chunks = splitter.split_documents(docs)

    vectorstore = FAISS.from_documents(chunks, hook.get_embedding_model())

    os.makedirs(INDEX_PERSIST_DIR, exist_ok=True)
    vectorstore.save_local(INDEX_PERSIST_DIR)
    print(f"Built FAISS index: {len(chunks)} chunks in {INDEX_PERSIST_DIR}")
    return INDEX_PERSIST_DIR


# ---------------------------------------------------------------------------
# Tool definitions (LangChain @tool decorator)
# ---------------------------------------------------------------------------


def _build_tools(hook, index_dir: str, survey_csv_path: str) -> list:
    """Construct the agent's tool set."""
    from langchain.tools import tool
    from langchain_community.vectorstores import FAISS

    # Build the vector store once and close over it -- the agent may invoke
    # search_knowledge_base many times, and reloading the FAISS index plus
    # re-initialising the embedding model on every call would be wasteful.
    vectorstore = FAISS.load_local(
        index_dir, hook.get_embedding_model(), allow_dangerous_deserialization=True
    )

    # -- Tool 1: Knowledge base search (vector retrieval) ------------------

    @tool
    def search_knowledge_base(query: str) -> str:
        """Search the internal knowledge base for relevant documentation.

        Use this for questions about Airflow features, architecture,
        operators, executors, connections, or best practices.
        """
        results = vectorstore.similarity_search(query, k=3)

        if not results:
            return "No relevant documents found in the knowledge base."

        formatted = []
        for i, doc in enumerate(results, 1):
            source = doc.metadata.get("source", "unknown")
            formatted.append(f"[{i}] Source: {source}\n{doc.page_content}")
        return "\n\n".join(formatted)

    # -- Tool 2: Survey data query ----------------------------------------

    @tool
    def query_survey_data(question: str) -> str:
        """Query the Airflow user survey dataset to answer questions about
        Airflow adoption, usage patterns, executor choices, deployment
        methods, cloud providers, and user demographics.

        Pass a natural language question.  The tool converts it to SQL
        and executes it against the survey data.
        """
        import csv

        if not os.path.exists(survey_csv_path):
            return (
                "Survey data not available. The CSV file was not found at "
                f"{survey_csv_path}. Continuing with other tools."
            )

        with open(survey_csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            return "Survey data is empty."

        columns = list(rows[0].keys())
        total = len(rows)
        summary_parts = [f"Survey has {total} responses with columns:"]
        summary_parts.append(", ".join(columns[:15]))
        if len(columns) > 15:
            summary_parts.append(f"... and {len(columns) - 15} more columns")

        q_lower = question.lower()

        if "kubernetes" in q_lower or "k8s" in q_lower:
            k8s_col = next(
                (c for c in columns if "kubernetes" in c.lower()),
                None,
            )
            if k8s_col:
                k8s_users = sum(1 for r in rows if r.get(k8s_col, "").strip())
                pct = round(100 * k8s_users / total, 1) if total else 0
                return (
                    f"KubernetesExecutor usage: {k8s_users} of {total} "
                    f"respondents ({pct}%) indicated they use KubernetesExecutor."
                )

        if "celery" in q_lower:
            celery_col = next(
                (c for c in columns if "celery" in c.lower()),
                None,
            )
            if celery_col:
                celery_users = sum(1 for r in rows if r.get(celery_col, "").strip())
                pct = round(100 * celery_users / total, 1) if total else 0
                return (
                    f"CeleryExecutor usage: {celery_users} of {total} "
                    f"respondents ({pct}%) indicated they use CeleryExecutor."
                )

        if "version" in q_lower:
            version_col = next(
                (c for c in columns if "version" in c.lower() and "airflow" in c.lower()),
                None,
            )
            if version_col:
                from collections import Counter

                counts = Counter(r.get(version_col, "unknown") for r in rows)
                top5 = counts.most_common(5)
                lines = [f"  {v}: {c} ({round(100 * c / total, 1)}%)" for v, c in top5]
                return "Airflow version distribution (top 5):\n" + "\n".join(lines)

        return (
            f"Survey dataset has {total} responses across {len(columns)} columns. "
            "Available topics: executor usage (Kubernetes, Celery, Local), "
            "Airflow versions, deployment methods, cloud providers, company "
            "size, industries, AI tool usage. Ask a more specific question."
        )

    # -- Tool 3: Web search (simulated) ------------------------------------

    @tool
    def search_web(query: str) -> str:
        """Search the web for current information, news, or context.

        Use this for questions that need up-to-date external information
        not available in the knowledge base or survey data.
        """
        responses = {
            "kubernetes airflow": (
                "Recent blog posts indicate KubernetesExecutor adoption has grown "
                "significantly since Airflow 2.0, with many large-scale deployments "
                "migrating from CeleryExecutor.  Key advantages cited: pod-level "
                "isolation, dynamic scaling, and per-task resource configuration. "
                "Source: Airflow blog, Astronomer blog (2025-2026)."
            ),
            "airflow 3": (
                "Airflow 3.0 shipped in early 2026 with major architectural changes: "
                "Execution API (workers never access metadata DB directly), multi-team "
                "isolation, improved UI, and the common.ai provider for AI/LLM support. "
                "Source: airflow.apache.org release notes."
            ),
            "airflow adoption": (
                "The 2025 Airflow User Survey showed continued growth: 2,000+ responses, "
                "40% of respondents at companies with 1,000+ employees, 35% using cloud-managed "
                "Airflow (Astronomer, MWAA, Cloud Composer). Source: Airflow blog."
            ),
        }

        for keyword, response in responses.items():
            if any(w in query.lower() for w in keyword.split()):
                return response

        return (
            f"Web search for '{query}' returned general results. "
            "For this demo, web search is simulated with canned responses. "
            "In production, use Tavily, Serper, or another search API "
            "configured via an Airflow connection."
        )

    # -- Tool 4: Current UTC time -----------------------------------------

    @tool
    def get_current_utc_time() -> str:
        """Return the current UTC date and time in ISO-8601 format.

        Use when the question depends on a current timestamp (e.g. "is the
        merge freeze active right now", "how recent is the survey data").
        LLMs cannot reliably know the wall-clock time on their own.
        """
        return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")

    return [search_knowledge_base, query_survey_data, search_web, get_current_utc_time]


# ---------------------------------------------------------------------------
# DAG: ReAct tool-calling agent with human review
# ---------------------------------------------------------------------------


# [START example_langchain_tool_agent]
@dag(tags=["example"])
def example_langchain_tool_agent():
    """
    Research agent with LangChain tools and human review.

    Task graph::

        prompt_review (HITLEntryOperator)
            -> prepare_tools (@task)
            -> run_research_agent (@task)
            -> format_report (LLMOperator)
            -> report_approval (ApprovalOperator)

    The agent uses LangChain's ``create_agent`` with a ReAct reasoning
    loop.  It autonomously decides which tools to call -- knowledge base
    search, survey data query, web search, or current-time lookup --
    based on the user's question.  The number and sequence of tool calls
    is determined by the LLM at runtime.

    The surrounding Airflow DAG provides what the agent cannot:
    human review of the question (HITLEntryOperator), formatted report
    generation (LLMOperator), and human approval of the final output
    (ApprovalOperator).
    """

    prompt_review = HITLEntryOperator(
        task_id="prompt_review",
        subject="Review the research question",
        params={
            "question": Param(
                DEFAULT_QUESTION,
                type="string",
                description="The research question for the agent to investigate",
            ),
        },
        response_timeout=datetime.timedelta(hours=1),
    )

    @task
    def prepare_tools(hitl_response: dict) -> dict:
        """Build the FAISS knowledge base index and resolve tool config."""
        from airflow.providers.common.ai.hooks.langchain import LangChainHook

        hook = LangChainHook(
            llm_conn_id=LLM_CONN_ID,
            llm_model=LLM_MODEL,
            embed_model=EMBEDDING_MODEL,
        )
        index_dir = _ensure_knowledge_base(hook)

        question = hitl_response["params_input"]["question"]
        return {
            "question": question,
            "index_dir": index_dir,
            "survey_csv_path": SURVEY_CSV_PATH,
        }

    @task
    def run_research_agent(config: dict) -> dict:
        """Run a LangChain ReAct agent that autonomously researches the question.

        The agent decides which tools to call and in what order.  The number
        of tool calls depends on the complexity of the question.  All
        reasoning steps, tool calls, and observations are logged.
        """
        from langchain.agents import create_agent

        from airflow.providers.common.ai.hooks.langchain import LangChainHook

        hook = LangChainHook(
            llm_conn_id=LLM_CONN_ID,
            llm_model=LLM_MODEL,
            embed_model=EMBEDDING_MODEL,
        )
        model = hook.get_chat_model()
        tools = _build_tools(hook, config["index_dir"], config["survey_csv_path"])

        agent = create_agent(
            model,
            tools=tools,
            system_prompt=(
                "You are a thorough research assistant for Apache Airflow. "
                "You have access to tools for searching a knowledge base, "
                "querying survey data, searching the web, and checking the "
                "current UTC time. "
                "Use the appropriate tools to fully answer the question. "
                "Combine information from multiple sources when relevant. "
                "Always cite which tool provided each piece of information."
            ),
        )

        question = config["question"]
        print(f"Research question: {question}")
        print("Agent starting research...")

        tool_calls_log = []
        final_answer = ""

        for step in agent.stream(
            {"messages": [{"role": "user", "content": question}]},
            stream_mode="values",
        ):
            msg = step["messages"][-1]
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                for tc in msg.tool_calls:
                    tool_calls_log.append(
                        {
                            "tool": tc["name"],
                            "args": str(tc.get("args", {}))[:200],
                        }
                    )
                    print(f"  Tool call: {tc['name']}({tc.get('args', {})})")
            elif hasattr(msg, "content") and msg.content:
                final_answer = msg.content

        print(f"Agent completed. Tool calls made: {len(tool_calls_log)}")

        return {
            "question": question,
            "findings": final_answer,
            "tool_calls": tool_calls_log,
            "tool_call_count": len(tool_calls_log),
        }

    tools_config = prepare_tools(prompt_review.output)
    research_result = run_research_agent(tools_config)

    format_report = LLMOperator(
        task_id="format_report",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=REPORT_SYSTEM_PROMPT,
        prompt="""\
Format the following research findings into a clear report.

{% set result = ti.xcom_pull(task_ids='run_research_agent') -%}
Question: {{ result['question'] }}

Raw findings:
{{ result['findings'] }}

Tools used: {{ result['tool_call_count'] }} calls
{% for tc in result['tool_calls'] -%}
  - {{ tc['tool'] }}: {{ tc['args'][:100] }}
{% endfor -%}""",
    )
    research_result >> format_report

    report_approval = ApprovalOperator(  # noqa: F841
        task_id="report_approval",
        subject="Review the research report",
        body=format_report.output,
        response_timeout=datetime.timedelta(hours=1),
    )


# [END example_langchain_tool_agent]

example_langchain_tool_agent()

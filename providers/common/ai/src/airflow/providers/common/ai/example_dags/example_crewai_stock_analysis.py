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
Multi-agent stock analysis with CrewAI and Dynamic Task Mapping.

Demonstrates how a multi-agent CrewAI crew runs inside Airflow tasks,
composed with HITL operators, Dynamic Task Mapping, and LLMOperator
synthesis -- all features that CrewAI's standalone Flows cannot provide
with the same observability and retry guarantees.

A human submits one or more stock tickers.  Each ticker fans out to an
independent ``@task`` that runs a 3-agent CrewAI crew (researcher,
analyst, writer).  The per-ticker reports are collected and synthesized
into a portfolio-level recommendation by ``LLMOperator``, then reviewed
by a human before the DAG completes.

``example_crewai_stock_analysis`` (manual trigger):

.. code-block:: text

    select_tickers  (HITLEntryOperator)
        → parse_tickers  (@task)
        → analyze_stock  (@task, mapped ×N via Dynamic Task Mapping)
        → collect_reports  (@task)
        → synthesize_portfolio  (LLMOperator)
        → report_approval  (ApprovalOperator)

**What this makes visible that a standalone CrewAI Flow hides:**

* Each ticker's crew run is a separate task instance -- if AAPL fails,
  MSFT and GOOGL results are preserved in XCom and only AAPL retries.
* The synthesis step's inputs are auditable XCom values, not an opaque
  continuation of an agent reasoning loop.
* HITL is UI-based with approval workflows, not a console prompt.
* Token usage per ticker is tracked and visible in task logs.

Before running:

1. Create a CrewAI connection named ``crewai_default`` (or the value of
   ``CREWAI_CONN_ID``) with ``conn_type=crewai``. The password field should
   hold your API key (e.g. OpenAI API key). CrewAI uses LiteLLM ``provider/name``
   format for model identifiers (e.g. ``openai/gpt-4o``).
2. Create a pydantic-ai connection named ``pydanticai_default`` (or the value
   of ``PYDANTICAI_CONN_ID``) with ``conn_type=pydanticai`` for the
   ``synthesize_portfolio`` step that runs through ``LLMOperator``. Same API
   key is fine; the two hooks need different ``conn_type`` because they use
   different model-string conventions (LiteLLM slash vs pydantic-ai colon).
3. ``pip install apache-airflow-providers-common-ai[crewai]``
4. Optionally ``pip install crewai-tools`` and set ``SERPER_API_KEY``
   to enable live web search for the research agent.
"""

from __future__ import annotations

import datetime
import json
import os

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import Param, dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLEntryOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CREWAI_CONN_ID = "crewai_default"
PYDANTICAI_CONN_ID = "pydanticai_default"

# CrewAI uses LiteLLM slash format (provider/name); pydantic-ai uses colon
# (provider:name). Each conn type's model field documents its own convention.
CREWAI_MODEL = os.environ.get("CREWAI_LLM_MODEL", "openai/gpt-4o")
PYDANTICAI_MODEL = os.environ.get("PYDANTICAI_LLM_MODEL", "openai:gpt-4o")

SYNTHESIS_SYSTEM_PROMPT = """\
You are a senior portfolio strategist. Given individual stock analysis
reports, synthesize a concise portfolio-level recommendation. Compare
the stocks, highlight relative strengths and risks, and give a clear
allocation suggestion. Write for a sophisticated investor audience."""


def _get_search_tools() -> list:
    """Return web search tools if crewai-tools and SERPER_API_KEY are available."""
    if not os.environ.get("SERPER_API_KEY"):
        return []
    try:
        from crewai_tools import SerperDevTool

        return [SerperDevTool()]
    except ImportError:
        return []


# ---------------------------------------------------------------------------
# DAG: CrewAI Stock Analysis with Dynamic Task Mapping
# ---------------------------------------------------------------------------


# [START example_crewai_stock_analysis]
@dag
def example_crewai_stock_analysis():
    """
    Fan-out a multi-agent crew across N stock tickers, then synthesize.

    Task graph::

        select_tickers  (HITLEntryOperator)
            → parse_tickers  (@task)
            → analyze_stock  (@task ×N, via Dynamic Task Mapping)
            → collect_reports  (@task)
            → synthesize_portfolio  (LLMOperator)
            → report_approval  (ApprovalOperator)
    """

    # ------------------------------------------------------------------
    # Step 1: Human selects which tickers to analyze.
    # ------------------------------------------------------------------
    select_tickers = HITLEntryOperator(
        task_id="select_tickers",
        subject="Select stock tickers for analysis",
        params={
            "tickers": Param(
                default="AAPL, MSFT, GOOGL",
                type="string",
                description="Comma-separated stock tickers to analyze",
            ),
        },
        response_timeout=datetime.timedelta(hours=1),
    )

    # ------------------------------------------------------------------
    # Step 2: Parse the comma-separated input into a list for fan-out.
    # ------------------------------------------------------------------
    @task
    def parse_tickers(hitl_output: dict) -> list[str]:
        raw = hitl_output["params_input"]["tickers"]
        tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
        print(f"Tickers to analyze: {tickers}")
        return tickers

    # ------------------------------------------------------------------
    # Step 3: Run a 3-agent CrewAI crew for each ticker.
    # Dynamic Task Mapping creates N independent task instances.
    # Each crew run is independently retryable -- if AAPL fails, MSFT
    # and GOOGL results are preserved.
    # ------------------------------------------------------------------
    @task(
        retries=1,
        retry_delay=datetime.timedelta(seconds=30),
        # Bound multi-turn agent loops -- a stuck ticker shouldn't hold the
        # worker slot indefinitely. CrewAI's LLM call has its own per-request
        # timeout, but the crew can chain many such calls.
        execution_timeout=datetime.timedelta(minutes=10),
    )
    def analyze_stock(ticker: str) -> dict:
        """Run a 3-agent investment research crew for a single ticker."""
        from crewai import Agent, Crew, Process, Task

        from airflow.providers.common.ai.hooks.crewai import CrewAIHook

        hook = CrewAIHook(llm_conn_id=CREWAI_CONN_ID, llm_model=CREWAI_MODEL)
        llm = hook.get_llm()
        tools = _get_search_tools()

        researcher = Agent(
            role="Stock Researcher",
            goal=(
                f"Research {ticker}'s recent performance, news, competitive position, and market sentiment"
            ),
            backstory=(
                "Senior equity researcher with 15 years of experience "
                "covering technology and growth stocks. Known for thorough "
                "fundamental analysis and identifying catalysts."
            ),
            llm=llm,
            tools=tools,
            verbose=False,
            allow_delegation=False,
        )

        analyst = Agent(
            role="Financial Analyst",
            goal=(
                f"Analyze {ticker}'s financial health, valuation, and risk "
                f"factors to produce a Buy/Hold/Sell recommendation"
            ),
            backstory=(
                "CFA charterholder and former sell-side analyst specializing "
                "in quantitative valuation models. Focuses on earnings "
                "quality, margin trends, and balance sheet strength."
            ),
            llm=llm,
            verbose=False,
            allow_delegation=False,
        )

        writer = Agent(
            role="Investment Report Writer",
            goal=(
                f"Synthesize the research and analysis into a clear, "
                f"actionable investment report for {ticker}"
            ),
            backstory=(
                "Financial journalist who spent a decade at a major "
                "financial publication. Translates complex analysis into "
                "concise, readable reports for institutional investors."
            ),
            llm=llm,
            verbose=False,
            allow_delegation=False,
        )

        research_task = Task(
            description=(
                f"Research {ticker} stock. Cover: (1) recent price action "
                f"and trading volume trends, (2) latest earnings results "
                f"and guidance, (3) key news and catalysts in the past "
                f"quarter, (4) competitive positioning. Provide specific "
                f"data points where possible."
            ),
            expected_output=(
                "Structured research summary with sections for price "
                "action, financials, news/catalysts, and competitive position"
            ),
            agent=researcher,
        )

        analysis_task = Task(
            description=(
                f"Based on the research, analyze {ticker}'s investment "
                f"case. Evaluate: (1) valuation relative to peers and "
                f"historical range, (2) earnings growth trajectory, "
                f"(3) key risk factors, (4) potential catalysts. "
                f"Conclude with a Buy/Hold/Sell recommendation and "
                f"a 12-month price target rationale."
            ),
            expected_output=(
                "Investment analysis with valuation assessment, growth "
                "outlook, risk factors, and a clear recommendation "
                "(Buy/Hold/Sell) with supporting rationale"
            ),
            agent=analyst,
            context=[research_task],
        )

        report_task = Task(
            description=(
                f"Write a concise investment report for {ticker} that "
                f"combines the research and analysis. Structure: "
                f"Executive Summary (2-3 sentences), Key Findings, "
                f"Recommendation, Risk Factors. Keep it under 500 words. "
                f"Lead with the recommendation."
            ),
            expected_output=(
                "Professional investment report under 500 words with "
                "executive summary, key findings, recommendation, and risks"
            ),
            agent=writer,
            context=[research_task, analysis_task],
        )

        crew = Crew(
            agents=[researcher, analyst, writer],
            tasks=[research_task, analysis_task, report_task],
            process=Process.sequential,
            verbose=False,
        )

        result = crew.kickoff()

        token_usage = {
            "total_tokens": result.token_usage.total_tokens,
            "prompt_tokens": result.token_usage.prompt_tokens,
            "completion_tokens": result.token_usage.completion_tokens,
            "successful_requests": result.token_usage.successful_requests,
        }
        print(f"[{ticker}] Token usage: {json.dumps(token_usage)}")

        return {
            "ticker": ticker,
            "report": result.raw,
            "token_usage": token_usage,
        }

    # ------------------------------------------------------------------
    # Step 4: Collect all per-ticker reports into a single dict.
    # Airflow preserves index order for mapped task outputs.
    # ------------------------------------------------------------------
    @task
    def collect_reports(reports: list[dict]) -> dict:
        collected = {}
        total_tokens = 0
        for report in reports:
            ticker = report["ticker"]
            collected[ticker] = report["report"]
            total_tokens += report["token_usage"]["total_tokens"]
            print(
                f"[{ticker}] Report length: {len(report['report'])} chars, "
                f"tokens: {report['token_usage']['total_tokens']}"
            )
        print(f"Total tokens across all tickers: {total_tokens}")
        return collected

    # ------------------------------------------------------------------
    # Step 5: Synthesize per-ticker reports into a portfolio view.
    # The LLM sees all reports together and produces a cross-ticker
    # comparison and allocation recommendation.
    # ------------------------------------------------------------------
    synthesize_portfolio = LLMOperator(
        task_id="synthesize_portfolio",
        llm_conn_id=PYDANTICAI_CONN_ID,
        model_id=PYDANTICAI_MODEL,
        system_prompt=SYNTHESIS_SYSTEM_PROMPT,
        prompt="""\
Given the following individual stock analysis reports, write a
portfolio-level recommendation. Compare the stocks on valuation,
growth, and risk. Suggest a relative allocation weighting.

Reports:
{{ ti.xcom_pull(task_ids='collect_reports') }}""",
    )

    # ------------------------------------------------------------------
    # Step 6: Human reviews the portfolio recommendation.
    # ------------------------------------------------------------------
    report_approval = ApprovalOperator(  # noqa: F841
        task_id="report_approval",
        subject="Review portfolio analysis and recommendation",
        body=synthesize_portfolio.output,
        response_timeout=datetime.timedelta(hours=1),
    )

    # -- Wire the DAG --
    tickers = parse_tickers(select_tickers.output)
    reports = analyze_stock.expand(ticker=tickers)
    collected = collect_reports(reports)
    collected >> synthesize_portfolio


# [END example_crewai_stock_analysis]

example_crewai_stock_analysis()

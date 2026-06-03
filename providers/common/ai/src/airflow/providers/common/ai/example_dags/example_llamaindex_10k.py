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
SEC 10-K financial analysis -- LlamaIndex RAG with live SEC EDGAR data.

Two production-shaped Dags that demonstrate a multi-company financial
research pipeline using real SEC 10-K filings fetched from the EDGAR
public API: one fetches and indexes filings on a schedule, the other
decomposes a comparison question at runtime and fans out retrieval via
Dynamic Task Mapping.

Filings are fetched from the SEC EDGAR public API (free, no auth
required) using stock ticker symbols.  Any US publicly-traded company
is supported -- configure via the ``tickers`` Dag parameter.

``example_llamaindex_10k_index`` (weekly schedule):

.. code-block:: text

    fetch_filings (@task, live from SEC EDGAR)
        -> build_index (LlamaIndexEmbeddingOperator, mapped x N companies)

``example_llamaindex_10k_analysis`` (manual trigger):

.. code-block:: text

    analyst_question  (HITLEntryOperator)
        -> get_question        (@task)
        -> get_tickers         (@task)
        -> decompose_question  (@task.llm, structured output)
        -> extract_sub_questions (@task)
        -> build_retrieval_kwargs (@task)
        -> retrieve             (LlamaIndexRetrievalOperator x N, DTM)
        -> collect_results      (@task)
        -> synthesize_report    (LLMOperator, UsageLimits + structured output)
        -> format_report        (@task, readable text for reviewer)
        -> review_report        (ApprovalOperator)

**What this makes visible that a notebook hides:**

* The LLM decides how many sub-questions to create -- N is unknown at
  parse time, determined at runtime via Dynamic Task Mapping.
* Each retrieval is an independent task instance: if one company's index
  is unavailable, only that instance retries.
* The synthesis step's token budget (``UsageLimits``) and output schema
  (``AnalysisReport``) are auditable in XCom.
* An analyst reviews the report before it reaches the investment committee.

Before running:

1. Create an LLM connection ``pydanticai_default`` for synthesis and
   decomposition, and ``llamaindex_default`` for embedding and retrieval.
2. Update ``EDGAR_USER_AGENT`` with your name and email (SEC requires a
   descriptive User-Agent header on all EDGAR API requests).
3. Run ``example_llamaindex_10k_index`` once to fetch filings and build
   the vector indexes.
4. Trigger ``example_llamaindex_10k_analysis`` to run the query pipeline.
"""

from __future__ import annotations

import json
import re
import urllib.request
from datetime import timedelta
from typing import Any

from pydantic import BaseModel
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.operators.llamaindex_embedding import LlamaIndexEmbeddingOperator
from airflow.providers.common.ai.operators.llamaindex_retrieval import LlamaIndexRetrievalOperator
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLEntryOperator
from airflow.sdk import Param

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LLM_CONN_ID = "pydanticai_default"
LLAMAINDEX_CONN_ID = "llamaindex_default"
INDEX_BASE_DIR = "/opt/airflow/data/indexes/10k"
DEFAULT_TICKERS = "AAPL,MSFT,UBER,LYFT,AMZN"

# SEC EDGAR requires a descriptive User-Agent header with a contact email
# on every request.  Update this before running in production.
EDGAR_USER_AGENT = "Apache Airflow Example dev@airflow.apache.org"

# ---------------------------------------------------------------------------
# Structured output models
# ---------------------------------------------------------------------------

# [START 10k_structured_output]


class SubQuestion(BaseModel):
    """One sub-question targeting a specific company."""

    sub_question: str
    ticker: str


class DecomposedQuestion(BaseModel):
    """LLM-produced decomposition of the analyst's question."""

    sub_questions: list[SubQuestion]


class AnalysisReport(BaseModel):
    """Structured financial comparison report."""

    executive_summary: str
    company_findings: list[dict]
    key_risks: list[str]
    recommendations: list[str]


# [END 10k_structured_output]

# ---------------------------------------------------------------------------
# SEC EDGAR helpers
# ---------------------------------------------------------------------------


def _edgar_get_json(url: str) -> Any:
    """GET a JSON endpoint from SEC EDGAR (public, no auth)."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": EDGAR_USER_AGENT, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _edgar_get_text(url: str) -> str:
    """GET an HTML/text document from SEC EDGAR."""
    req = urllib.request.Request(url, headers={"User-Agent": EDGAR_USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _strip_html_tags(html: str) -> str:
    """Remove HTML tags, returning plain text."""
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _resolve_ticker(ticker: str) -> tuple[int, str]:
    """Look up a company's CIK number and name from its stock ticker.

    Returns (cik, company_name).
    """
    data = _edgar_get_json("https://www.sec.gov/files/company_tickers.json")
    for entry in data.values():
        if entry["ticker"].upper() == ticker.upper():
            return int(entry["cik_str"]), entry["title"]
    raise ValueError(f"Ticker {ticker!r} not found in SEC EDGAR. Check the ticker symbol and try again.")


def _find_latest_10k(cik: int) -> tuple[str, str]:
    """Find the latest 10-K filing URL and date for a given CIK.

    Returns (document_url, filing_date).
    """
    padded = str(cik).zfill(10)
    submissions = _edgar_get_json(f"https://data.sec.gov/submissions/CIK{padded}.json")
    recent = submissions["filings"]["recent"]
    for i, form in enumerate(recent["form"]):
        if form == "10-K":
            accession = recent["accessionNumber"][i].replace("-", "")
            primary_doc = recent["primaryDocument"][i]
            filing_date = recent["filingDate"][i]
            url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary_doc}"
            return url, filing_date
    raise ValueError(f"No 10-K filing found for CIK {padded}")


def _extract_filing_sections(full_text: str, ticker: str, company_name: str, filing_date: str) -> list[dict]:
    """Split 10-K text into section-aware documents for embedding.

    Attempts to extract Item 1A (Risk Factors) and Item 7 (MD&A).
    Falls back to the first 40 000 characters if section markers
    are not found.
    """
    metadata_base = {"ticker": ticker, "company": company_name, "filing_date": filing_date}
    sections = []

    section_patterns = [
        ("risk_factors", r"(?i)(item\s+1a\b.{0,40}risk\s+factors.*?)(?=\bitem\s+1b\b|\bitem\s+2\b)"),
        ("mda", r"(?i)(item\s+7\b.{0,60}management.*?)(?=\bitem\s+7a\b|\bitem\s+8\b)"),
    ]

    for section_name, pattern in section_patterns:
        matches = list(re.finditer(pattern, full_text, re.DOTALL))
        if matches:
            best = max(matches, key=lambda m: len(m.group(1)))
            section_text = best.group(1).strip()[:20_000]
            if len(section_text) > 200:
                sections.append(
                    {
                        "text": section_text,
                        "metadata": {**metadata_base, "section": section_name},
                    }
                )

    if not sections:
        sections.append(
            {
                "text": full_text[:40_000],
                "metadata": {**metadata_base, "section": "full_filing"},
            }
        )

    return sections


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

DECOMPOSE_SYSTEM_PROMPT = """\
You are a financial research assistant. Given a comparison question and a \
list of companies (identified by stock ticker), decompose it into specific \
sub-questions, one per company. Each sub-question should target information \
that would be found in the company's 10-K filing (Risk Factors or MD&A \
sections). Use the exact ticker symbol in the ``ticker`` field."""

SYNTHESIS_SYSTEM_PROMPT = """\
You are a senior financial analyst. Given retrieval results from multiple \
companies' 10-K filings, synthesize a structured comparison report. \
Cite specific data points from the source text. Be precise about which \
company each finding relates to."""

DEFAULT_QUESTION = (
    "Compare the risk factors and revenue trends across these companies. "
    "Which company faces the most concentrated risk and which shows the "
    "strongest growth trajectory?"
)

# =========================================================================
# DAG 1: Fetch and index filings (scheduled)
# =========================================================================


# [START example_llamaindex_10k_index]
@dag(
    schedule="@weekly",
    catchup=False,
    params={
        "tickers": Param(
            DEFAULT_TICKERS,
            type="string",
            description="Comma-separated stock tickers to fetch and index (e.g. AAPL,MSFT,UBER)",
        ),
    },
    tags=["example", "llamaindex", "10k"],
)
def example_llamaindex_10k_index():
    """
    Fetch 10-K filings from SEC EDGAR and build per-company vector indexes.

    Runs weekly to refresh indexes when new filings arrive.  Each company
    gets its own persisted index via Dynamic Task Mapping.

    Task graph::

        fetch_filings (@task, live from SEC EDGAR)
            -> build_index (LlamaIndexEmbeddingOperator x N companies)
    """

    # [START 10k_index_dtm]
    @task
    def fetch_filings(params: dict) -> list[dict]:
        tickers = [t.strip().upper() for t in params["tickers"].split(",") if t.strip()]
        result = []
        for ticker in tickers:
            cik, company_name = _resolve_ticker(ticker)
            doc_url, filing_date = _find_latest_10k(cik)
            html = _edgar_get_text(doc_url)
            plain_text = _strip_html_tags(html)
            documents = _extract_filing_sections(plain_text, ticker, company_name, filing_date)
            result.append(
                {
                    "documents": documents,
                    "persist_dir": f"{INDEX_BASE_DIR}/{ticker.lower()}",
                }
            )
        return result

    LlamaIndexEmbeddingOperator.partial(
        task_id="build_index",
        embed_model="text-embedding-3-small",
        llm_conn_id=LLAMAINDEX_CONN_ID,
        chunk_size=512,
        chunk_overlap=50,
    ).expand_kwargs(fetch_filings())
    # [END 10k_index_dtm]


# [END example_llamaindex_10k_index]

example_llamaindex_10k_index()


# =========================================================================
# DAG 2: Analyst query pipeline (on-demand)
# =========================================================================


# [START example_llamaindex_10k_analysis]
@dag(
    schedule=None,
    catchup=False,
    params={
        "tickers": Param(
            DEFAULT_TICKERS,
            type="string",
            description="Comma-separated stock tickers (must match indexed companies)",
        ),
    },
    tags=["example", "llamaindex", "10k"],
)
def example_llamaindex_10k_analysis():
    """
    Multi-company financial comparison via LLM-driven sub-question decomposition.

    An analyst submits a comparison question.  The LLM decomposes it into
    company-specific sub-questions (N decided at runtime), each sub-question
    retrieves from the appropriate company's vector index in parallel via
    Dynamic Task Mapping, and the results are synthesized into a structured
    report for human review.

    Task graph::

        analyst_input     (HITLEntryOperator, tickers + question)
            -> get_question        (@task)
            -> get_tickers         (@task)
            -> decompose_question  (@task.llm, structured output)
            -> extract_sub_questions (@task)
            -> build_retrieval_kwargs (@task)
            -> retrieve             (LlamaIndexRetrievalOperator x N, DTM)
            -> collect_results      (@task)
            -> synthesize_report    (LLMOperator, UsageLimits + AnalysisReport)
            -> format_report        (@task, readable text for reviewer)
            -> review_report        (ApprovalOperator)
    """

    # ------------------------------------------------------------------
    # Step 1: Analyst submits the comparison question via HITL.
    # ------------------------------------------------------------------
    # [START 10k_hitl_entry]
    analyst_input = HITLEntryOperator(
        task_id="analyst_input",
        subject="Enter a 10-K comparison question",
        params={
            "tickers": Param(
                DEFAULT_TICKERS,
                type="string",
                description="Comma-separated stock tickers to compare (must match indexed companies)",
            ),
            "question": Param(
                DEFAULT_QUESTION,
                type="string",
                description="Financial comparison question across the selected companies",
            ),
        },
        response_timeout=timedelta(hours=1),
    )
    # [END 10k_hitl_entry]

    @task
    def get_question(hitl_response: dict) -> str:
        return hitl_response["params_input"]["question"]

    question = get_question(analyst_input.output)

    @task
    def get_tickers(hitl_response: dict) -> str:
        return hitl_response["params_input"]["tickers"]

    tickers = get_tickers(analyst_input.output)

    # ------------------------------------------------------------------
    # Step 2: LLM decomposes the question into company-specific
    # sub-questions.  N is decided by the LLM at runtime -- this is the
    # dynamic adaptation that a static Dag cannot express.
    # ------------------------------------------------------------------
    # [START 10k_decompose]
    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=DECOMPOSE_SYSTEM_PROMPT,
        output_type=DecomposedQuestion,
        # Push the structured output to XCom as a dict so the example runs on
        # every supported Airflow version (the model-instance form needs 3.3+).
        serialize_output=True,
    )
    def decompose_question(question: str, tickers: str) -> str:
        return (
            f"Decompose this question into company-specific sub-questions.\n"
            f"Available companies (by ticker): {tickers}\n\n"
            f"Question: {question}"
        )

    decomposed = decompose_question(question, tickers)
    # [END 10k_decompose]

    @task
    def extract_sub_questions(decomposed: dict) -> list[dict]:
        return decomposed["sub_questions"]

    sub_questions = extract_sub_questions(decomposed)

    # ------------------------------------------------------------------
    # Step 3: Map sub-questions to LlamaIndexRetrievalOperator kwargs.
    # Each sub-question targets a specific company's pre-built index.
    # ------------------------------------------------------------------
    @task
    def build_retrieval_kwargs(sub_questions: list[dict]) -> list[dict]:
        return [
            {
                "query": sq["sub_question"],
                "index_persist_dir": f"{INDEX_BASE_DIR}/{sq['ticker'].lower()}",
            }
            for sq in sub_questions
        ]

    retrieval_kwargs = build_retrieval_kwargs(sub_questions)

    # ------------------------------------------------------------------
    # Step 4: Retrieve relevant chunks for each sub-question.
    # Dynamic Task Mapping fans out one LlamaIndexRetrievalOperator
    # per sub-question, each targeting the company's vector index.
    # ------------------------------------------------------------------
    # [START 10k_dtm_retrieval]
    retrieval_results = LlamaIndexRetrievalOperator.partial(
        task_id="retrieve",
        embed_model="text-embedding-3-small",
        llm_conn_id=LLAMAINDEX_CONN_ID,
        top_k=5,
    ).expand_kwargs(retrieval_kwargs)
    # [END 10k_dtm_retrieval]

    # ------------------------------------------------------------------
    # Step 5: Collect all retrieval results into a single context.
    # Mapped outputs preserve input order, so zip with sub_questions
    # re-associates each result with its company.
    # ------------------------------------------------------------------
    @task
    def collect_results(sub_questions: list[dict], results: list[dict]) -> str:
        sections = []
        for sq, r in zip(sub_questions, results):
            chunks_text = "\n".join(
                f"  [{i + 1}] (score {c.get('score') or 0.0:.2f}) {c['text']}"
                for i, c in enumerate(r["chunks"])
            )
            sections.append(f"## {sq['ticker']} -- {sq['sub_question']}\n{chunks_text}")
        return "\n\n".join(sections)

    collected = collect_results(sub_questions, retrieval_results.output)

    # ------------------------------------------------------------------
    # Step 6: Synthesize a structured comparison report.
    # UsageLimits caps the token spend; output_type=AnalysisReport
    # enforces the Pydantic schema on the LLM response.
    # ------------------------------------------------------------------
    # [START 10k_synthesis]
    synthesize = LLMOperator(
        task_id="synthesize_report",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=SYNTHESIS_SYSTEM_PROMPT,
        prompt="""\
Synthesize a cross-company financial comparison from these retrieval results.
Cite specific data points and scores.

{{ ti.xcom_pull(task_ids='collect_results') }}""",
        output_type=AnalysisReport,
        serialize_output=True,
        usage_limits=UsageLimits(
            request_limit=10,
            input_tokens_limit=50_000,
            output_tokens_limit=16_000,
        ),
    )
    # [END 10k_synthesis]
    collected >> synthesize

    # ------------------------------------------------------------------
    # Step 7: Format the structured report into readable text for the
    # human reviewer.  The LLM produced a dict (via output_type=
    # AnalysisReport with serialize_output); this task renders it as clean prose.
    # ------------------------------------------------------------------
    @task
    def format_report(report: dict) -> str:
        lines = [f"# Executive Summary\n\n{report['executive_summary']}"]

        if report.get("company_findings"):
            lines.append("\n# Company Findings")
            for finding in report["company_findings"]:
                company = finding.get("company") or finding.get("ticker", "Unknown")
                lines.append(f"\n## {company}")
                for key, value in finding.items():
                    if key not in ("company", "ticker"):
                        lines.append(f"- **{key}**: {value}")

        if report.get("key_risks"):
            lines.append("\n# Key Risks")
            for risk in report["key_risks"]:
                lines.append(f"- {risk}")

        if report.get("recommendations"):
            lines.append("\n# Recommendations")
            for rec in report["recommendations"]:
                lines.append(f"- {rec}")

        return "\n".join(lines)

    review_body = format_report(synthesize.output)

    # ------------------------------------------------------------------
    # Step 8: Analyst reviews the report before it reaches the
    # investment committee.
    # ------------------------------------------------------------------
    # [START 10k_hitl_approval]
    ApprovalOperator(
        task_id="review_report",
        subject="Review 10-K comparison report before sharing",
        body=review_body,
        response_timeout=timedelta(hours=24),
    )
    # [END 10k_hitl_approval]


# [END example_llamaindex_10k_analysis]

example_llamaindex_10k_analysis()

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
SEC 10-K financial analysis -- LangChain RAG with Dynamic Task Mapping.

Two production-shaped Dags that demonstrate a multi-company financial
research pipeline: one builds per-company FAISS vector indexes on a
schedule, the other decomposes a comparison question at runtime and fans
out retrieval via Dynamic Task Mapping.

This is the LangChain counterpart to ``example_llamaindex_10k.py``.
Both share the same DAG shape (decompose -> fan-out retrieval -> collect
-> synthesize -> approve) and the same sample filing data, demonstrating
that the framework choice is a swappable implementation detail while
Airflow provides the orchestration.

``example_langchain_10k_index`` (weekly schedule):

.. code-block:: text

    prepare_filings (@task)
        -> build_index (@task, mapped x N companies)
            Uses LangChainHook + RecursiveCharacterTextSplitter + FAISS

``example_langchain_10k_analysis`` (manual trigger):

.. code-block:: text

    analyst_question  (HITLEntryOperator)
        -> decompose_question  (@task.llm, structured output)
        -> extract_sub_questions (@task)
        -> build_retrieval_kwargs (@task)
        -> retrieve             (@task, mapped x N sub-questions)
        -> collect_results      (@task)
        -> synthesize_report    (LLMOperator, UsageLimits + structured output)
        -> review_report        (ApprovalOperator)

**What this makes visible that a notebook hides:**

* The LLM decides how many sub-questions to create -- N is unknown at
  parse time, determined at runtime via Dynamic Task Mapping.
* Each retrieval is an independent task instance: if one company's index
  is unavailable, only that instance retries.
* The synthesis step's token budget (``UsageLimits``) and output schema
  (``AnalysisReport``) are auditable in XCom.
* An analyst reviews the report before it reaches the investment committee.

**LangChain-specific components used:**

* ``LangChainHook`` -- vendor-agnostic model dispatch via ``init_embeddings``
* ``RecursiveCharacterTextSplitter`` -- character-based chunking
* ``FAISS`` -- in-process vector store (no external server)

**Pattern difference from the LlamaIndex counterpart:**

The LlamaIndex indexing DAG passes document dicts via XCom to
``LlamaIndexEmbeddingOperator``.  Here, the indexing DAG passes a company
key and each ``@task`` re-reads from the ``SAMPLE_FILINGS`` global,
because LangChain's ``FAISS.from_documents`` requires ``Document``
objects that are not JSON-serializable for XCom.  In production, both
patterns converge: ``DocumentLoaderOperator`` produces serializable dicts
that either framework can consume.

Before running:

1. Install LangChain packages::

       pip install langchain langchain-openai langchain-text-splitters \\
                   langchain-community faiss-cpu

2. Create an LLM connection ``pydanticai_default`` for synthesis and
   decomposition, and ``langchain_default`` for embedding and retrieval.
3. Run ``example_langchain_10k_index`` once to build the sample indexes.
4. Trigger ``example_langchain_10k_analysis`` to run the query pipeline.
"""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLEntryOperator
from airflow.sdk import Param

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LLM_CONN_ID = "pydanticai_default"
LANGCHAIN_CONN_ID = "langchain_default"
EMBEDDING_MODEL = "openai:text-embedding-3-small"
INDEX_BASE_DIR = "/opt/airflow/data/indexes/10k_langchain"

COMPANY_LIST = [
    "acme_manufacturing",
    "globex_financial",
    "initech_software",
    "umbrella_biotech",
    "stark_energy",
]

# ---------------------------------------------------------------------------
# Structured output models
# ---------------------------------------------------------------------------


class SubQuestion(BaseModel):
    """One sub-question targeting a specific company."""

    sub_question: str
    company: str


class DecomposedQuestion(BaseModel):
    """LLM-produced decomposition of the analyst's question."""

    sub_questions: list[SubQuestion]


class AnalysisReport(BaseModel):
    """Structured financial comparison report."""

    executive_summary: str
    company_findings: list[dict]
    key_risks: list[str]
    recommendations: list[str]


# ---------------------------------------------------------------------------
# Sample 10-K filing data (inline, self-contained)
#
# Each company has Risk Factors and MD&A narrative text mimicking real
# SEC 10-K sections.  Same data as the LlamaIndex counterpart -- the
# framework-agnostic story is: same input, same DAG shape, different
# framework handling the chunking and retrieval.
#
# For production use, replace with DocumentLoaderOperator pointed at
# actual filings:
#
#     DocumentLoaderOperator(
#         task_id="load_filings",
#         source_path="/data/filings/acme_manufacturing/*.pdf",
#     )
# ---------------------------------------------------------------------------

SAMPLE_FILINGS: dict[str, list[dict]] = {
    "acme_manufacturing": [
        {
            "text": (
                "Risk Factors: Acme Manufacturing relies on a global supply chain "
                "spanning 14 countries. Disruptions from geopolitical tensions, "
                "port congestion, or raw material shortages could materially affect "
                "production timelines. The company's largest supplier accounts for "
                "23% of total input costs, creating single-source concentration risk. "
                "Tariff escalation in key markets remains a persistent threat to "
                "operating margins."
            ),
            "metadata": {"company": "acme_manufacturing", "section": "risk_factors"},
        },
        {
            "text": (
                "MD&A: Revenue for fiscal 2025 increased 8% year-over-year to "
                "$4.2 billion, driven by industrial automation demand. Gross margins "
                "contracted 120 basis points to 34.1% due to elevated steel and "
                "semiconductor costs. The company invested $380M in two new "
                "manufacturing facilities in Mexico and Vietnam to diversify its "
                "production footprint away from single-region concentration."
            ),
            "metadata": {"company": "acme_manufacturing", "section": "mda"},
        },
    ],
    "globex_financial": [
        {
            "text": (
                "Risk Factors: Globex Financial operates under regulatory frameworks "
                "in 28 jurisdictions. Changes in capital adequacy requirements, "
                "anti-money laundering rules, or data residency laws could require "
                "significant compliance investment. Interest rate volatility directly "
                "impacts the fixed-income portfolio, which represents 62% of assets "
                "under management. Cybersecurity threats to trading infrastructure "
                "pose systemic risk."
            ),
            "metadata": {"company": "globex_financial", "section": "risk_factors"},
        },
        {
            "text": (
                "MD&A: Total revenue reached $2.8 billion, up 12% year-over-year. "
                "Net interest income grew 18% as the rate environment favoured the "
                "loan portfolio mix. Fee income from wealth management grew 9%, "
                "offsetting a 5% decline in trading revenue due to lower volatility. "
                "The efficiency ratio improved to 58.3% from 61.1% through "
                "operational automation and branch consolidation."
            ),
            "metadata": {"company": "globex_financial", "section": "mda"},
        },
    ],
    "initech_software": [
        {
            "text": (
                "Risk Factors: Initech Software depends on annual subscription "
                "renewals for 78% of revenue. Customer churn in the SMB segment "
                "increased to 14% as competitors launched lower-cost alternatives. "
                "Rapid AI integration into the product suite introduces model "
                "reliability and hallucination risks that could erode customer trust. "
                "Key-person dependency on the founding engineering team remains high."
            ),
            "metadata": {"company": "initech_software", "section": "risk_factors"},
        },
        {
            "text": (
                "MD&A: ARR surpassed $1.1 billion, a 22% increase. Enterprise "
                "segment net retention reached 118%, driven by AI-powered analytics "
                "upsells. R&D spend increased to 28% of revenue as the company "
                "accelerated LLM integration across the platform. Free cash flow "
                "margin expanded to 19% despite the investment increase, reflecting "
                "operating leverage in the cloud infrastructure."
            ),
            "metadata": {"company": "initech_software", "section": "mda"},
        },
    ],
    "umbrella_biotech": [
        {
            "text": (
                "Risk Factors: Umbrella BioTech's pipeline is concentrated in "
                "oncology, with three candidates in Phase II trials. Failure of the "
                "lead compound UB-401 would eliminate 45% of projected 2028 revenue. "
                "FDA regulatory timelines are unpredictable and recent guidance "
                "changes on companion diagnostics may delay approval pathways. "
                "Patent cliff on the existing portfolio begins in 2027."
            ),
            "metadata": {"company": "umbrella_biotech", "section": "risk_factors"},
        },
        {
            "text": (
                "MD&A: Total revenue was $890 million, down 3% as the legacy "
                "immunology franchise faced biosimilar competition. R&D expenditure "
                "rose 31% to $420 million to fund three pivotal oncology trials. "
                "The company secured a $500 million licensing deal for UB-401 rights "
                "in Asia-Pacific, providing non-dilutive funding through 2027. "
                "Cash runway is 34 months at current burn rate."
            ),
            "metadata": {"company": "umbrella_biotech", "section": "mda"},
        },
    ],
    "stark_energy": [
        {
            "text": (
                "Risk Factors: Stark Energy's project pipeline depends on federal "
                "tax credits (ITC/PTC) that face political uncertainty. Permitting "
                "delays for utility-scale solar and wind projects averaged 18 months "
                "in 2025, up from 12 months in 2023. Grid interconnection queues "
                "exceed 2,500 GW nationally, creating bottlenecks that may strand "
                "permitted projects. Lithium and rare-earth supply constraints "
                "threaten battery storage deployment timelines."
            ),
            "metadata": {"company": "stark_energy", "section": "risk_factors"},
        },
        {
            "text": (
                "MD&A: Revenue grew 35% to $1.6 billion as 4.2 GW of solar and "
                "wind capacity reached commercial operation. EBITDA margins expanded "
                "to 28% from 22% as scale effects reduced per-MW installation "
                "costs. The company added 8 GWh of contracted battery storage "
                "backlog, now totalling 14 GWh. Capital expenditure of $1.1 billion "
                "was funded through project finance and a $400 million green bond "
                "issuance at 5.2% yield."
            ),
            "metadata": {"company": "stark_energy", "section": "mda"},
        },
    ],
}

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

DECOMPOSE_SYSTEM_PROMPT = """\
You are a financial research assistant. Given a comparison question and a \
list of companies, decompose it into specific sub-questions, one per \
company. Each sub-question should target information that would be found \
in the company's 10-K filing (Risk Factors or MD&A sections)."""

SYNTHESIS_SYSTEM_PROMPT = """\
You are a senior financial analyst. Given retrieval results from multiple \
companies' 10-K filings, synthesize a structured comparison report. \
Cite specific data points from the source text. Be precise about which \
company each finding relates to."""

DEFAULT_QUESTION = (
    "Compare the risk factors and revenue trends across all five companies. "
    "Which company faces the most concentrated risk and which shows the "
    "strongest growth trajectory?"
)

# =========================================================================
# DAG 1: Build per-company FAISS indexes (scheduled)
# =========================================================================


# [START example_langchain_10k_index]
@dag(schedule="@weekly", catchup=False, tags=["example", "langchain", "10k"])
def example_langchain_10k_index():
    """
    Build per-company FAISS vector indexes from 10-K filing text.

    Runs weekly to refresh indexes when new filings arrive.  Each company
    gets its own persisted FAISS index via Dynamic Task Mapping.

    Uses LangChain's ``RecursiveCharacterTextSplitter`` for chunking and
    ``LangChainHook.get_embedding_model()`` for vendor-agnostic embedding
    via ``init_embeddings``.

    Task graph::

        prepare_filings (@task)
            -> build_index (@task x N companies)
    """

    # [START 10k_langchain_index_dtm]
    @task
    def prepare_filings() -> list[dict]:
        return [
            {
                "company": company,
                "persist_dir": f"{INDEX_BASE_DIR}/{company}",
            }
            for company in COMPANY_LIST
        ]

    @task
    def build_index(company: str, persist_dir: str) -> dict:
        import os

        from langchain_community.vectorstores import FAISS
        from langchain_core.documents import Document
        from langchain_text_splitters import RecursiveCharacterTextSplitter

        from airflow.providers.common.ai.hooks.langchain import LangChainHook

        hook = LangChainHook(
            llm_conn_id=LANGCHAIN_CONN_ID,
            embed_model=EMBEDDING_MODEL,
        )
        embeddings = hook.get_embedding_model()

        docs = [
            Document(page_content=filing["text"], metadata=filing["metadata"])
            for filing in SAMPLE_FILINGS[company]
        ]

        splitter = RecursiveCharacterTextSplitter(chunk_size=512, chunk_overlap=50)
        chunks = splitter.split_documents(docs)

        vectorstore = FAISS.from_documents(chunks, embeddings)

        os.makedirs(persist_dir, exist_ok=True)
        vectorstore.save_local(persist_dir)

        return {
            "company": company,
            "document_count": len(docs),
            "chunk_count": len(chunks),
            "persist_dir": persist_dir,
        }

    build_index.expand_kwargs(prepare_filings())
    # [END 10k_langchain_index_dtm]


# [END example_langchain_10k_index]

example_langchain_10k_index()


# =========================================================================
# DAG 2: Analyst query pipeline (on-demand)
# =========================================================================


# [START example_langchain_10k_analysis]
@dag(schedule=None, catchup=False, tags=["example", "langchain", "10k"])
def example_langchain_10k_analysis():
    """
    Multi-company financial comparison via LLM-driven sub-question decomposition.

    An analyst submits a comparison question.  The LLM decomposes it into
    company-specific sub-questions (N decided at runtime), each sub-question
    retrieves from the appropriate company's FAISS index in parallel via
    Dynamic Task Mapping, and the results are synthesized into a structured
    report for human review.

    Uses ``LangChainHook`` for vendor-agnostic embedding dispatch and
    LangChain's ``FAISS.load_local`` / ``similarity_search_with_score``
    for retrieval.

    Task graph::

        analyst_question  (HITLEntryOperator)
            -> decompose_question  (@task.llm, structured output)
            -> extract_sub_questions (@task)
            -> build_retrieval_kwargs (@task)
            -> retrieve             (@task x N, Dynamic Task Mapping)
            -> collect_results      (@task)
            -> synthesize_report    (LLMOperator, UsageLimits + AnalysisReport)
            -> review_report        (ApprovalOperator)
    """

    # ------------------------------------------------------------------
    # Step 1: Analyst submits the comparison question via HITL.
    # ------------------------------------------------------------------
    # [START 10k_langchain_hitl_entry]
    analyst_question = HITLEntryOperator(
        task_id="analyst_question",
        subject="Enter a 10-K comparison question",
        params={
            "question": Param(
                DEFAULT_QUESTION,
                type="string",
                description="Financial comparison question across the five companies",
            ),
        },
        response_timeout=timedelta(hours=1),
    )
    # [END 10k_langchain_hitl_entry]

    @task
    def get_question(hitl_response: dict) -> str:
        return hitl_response["params_input"]["question"]

    question = get_question(analyst_question.output)

    # ------------------------------------------------------------------
    # Step 2: LLM decomposes the question into company-specific
    # sub-questions.  N is decided by the LLM at runtime.
    # ------------------------------------------------------------------
    # [START 10k_langchain_decompose]
    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=DECOMPOSE_SYSTEM_PROMPT,
        output_type=DecomposedQuestion,
    )
    def decompose_question(question: str) -> str:
        companies = ", ".join(COMPANY_LIST)
        return (
            f"Decompose this question into company-specific sub-questions.\n"
            f"Available companies: {companies}\n\n"
            f"Question: {question}"
        )

    decomposed = decompose_question(question)
    # [END 10k_langchain_decompose]

    @task
    def extract_sub_questions(decomposed: dict) -> list[dict]:
        return decomposed["sub_questions"]

    sub_questions = extract_sub_questions(decomposed)

    # ------------------------------------------------------------------
    # Step 3: Map sub-questions to retrieval kwargs.
    # Each sub-question targets a specific company's FAISS index.
    # ------------------------------------------------------------------
    @task
    def build_retrieval_kwargs(sub_questions: list[dict]) -> list[dict]:
        return [
            {
                "query": sq["sub_question"],
                "index_dir": f"{INDEX_BASE_DIR}/{sq['company']}",
            }
            for sq in sub_questions
        ]

    retrieval_kwargs = build_retrieval_kwargs(sub_questions)

    # ------------------------------------------------------------------
    # Step 4: Retrieve relevant chunks for each sub-question.
    # Dynamic Task Mapping fans out one retrieval task per sub-question,
    # each loading the company's FAISS index via LangChainHook.
    # ------------------------------------------------------------------
    # [START 10k_langchain_dtm_retrieval]
    @task
    def retrieve(query: str, index_dir: str) -> dict:
        from langchain_community.vectorstores import FAISS

        from airflow.providers.common.ai.hooks.langchain import LangChainHook

        hook = LangChainHook(
            llm_conn_id=LANGCHAIN_CONN_ID,
            embed_model=EMBEDDING_MODEL,
        )
        embeddings = hook.get_embedding_model()

        vectorstore = FAISS.load_local(
            index_dir, embeddings, allow_dangerous_deserialization=True
        )
        results = vectorstore.similarity_search_with_score(query, k=5)

        # FAISS returns L2 distance (lower = more similar).  Normalize to
        # 0-1 similarity so the synthesis LLM interprets scores correctly.
        return {
            "query": query,
            "chunks": [
                {
                    "text": doc.page_content,
                    "score": round(1.0 / (1.0 + float(score)), 4),
                    "metadata": doc.metadata,
                }
                for doc, score in results
            ],
        }

    retrieval_results = retrieve.expand_kwargs(retrieval_kwargs)
    # [END 10k_langchain_dtm_retrieval]

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
            sections.append(f"## {sq['company']} -- {sq['sub_question']}\n{chunks_text}")
        return "\n\n".join(sections)

    collected = collect_results(sub_questions, retrieval_results)

    # ------------------------------------------------------------------
    # Step 6: Synthesize a structured comparison report.
    # UsageLimits caps the token spend; output_type=AnalysisReport
    # enforces the Pydantic schema on the LLM response.
    # ------------------------------------------------------------------
    # [START 10k_langchain_synthesis]
    synthesize = LLMOperator(
        task_id="synthesize_report",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=SYNTHESIS_SYSTEM_PROMPT,
        prompt="""\
Synthesize a cross-company financial comparison from these retrieval results.
Cite specific data points and scores.

{{ ti.xcom_pull(task_ids='collect_results') }}""",
        output_type=AnalysisReport,
        usage_limits=UsageLimits(
            request_limit=10,
            input_tokens_limit=50_000,
            output_tokens_limit=8_000,
        ),
    )
    # [END 10k_langchain_synthesis]
    collected >> synthesize
     
    @task
    def format_report_for_review(report: dict) -> str:
        return json.dumps(report, indent=2)

    review_body = format_report_for_review(synthesize.output)
    

    # ------------------------------------------------------------------
    # Step 7: Analyst reviews the report before it reaches the
    # investment committee.
    # ------------------------------------------------------------------
    # [START 10k_langchain_hitl_approval]
    ApprovalOperator(
        task_id="review_report",
        subject="Review 10-K comparison report before sharing",
        body=synthesize.output,
        response_timeout=timedelta(hours=24),
    )
    # [END 10k_langchain_hitl_approval]


# [END example_langchain_10k_analysis]

example_langchain_10k_analysis()

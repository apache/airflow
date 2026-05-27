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
AIP progress tracker -- multi-source data fusion with common.ai operators.

Demonstrates Dynamic Task Mapping, structured LLM output, cost-controlled
synthesis, and HITL approval using only ``LLMOperator`` -- no LlamaIndex or
LangChain dependency required.

For each active Airflow Improvement Proposal the Dag gathers evidence from
two sources (Confluence spec text, GitHub PRs and commits), asks an LLM to
assess spec-vs-implementation progress, then synthesizes a cross-AIP report
for maintainer review.

``example_aip_progress_tracker`` (manual trigger):

.. code-block:: text

    fetch_aip_list (@task)
        → gather_aip_evidence  (@task, mapped ×N AIPs)
        → format_analysis_prompt (@task, mapped ×N)
        → analyze_aip          (LLMOperator, mapped ×N)
        → collect_analyses     (@task)
        → synthesize_report    (LLMOperator, with UsageLimits)
        → review_report        (ApprovalOperator)

**What this makes visible that a notebook hides:**

* Each AIP investigation is a named, logged task instance with its own
  retry behaviour -- not a loop iteration buried inside one cell.
* If the GitHub API is rate-limited for one AIP, only that mapped
  instance retries; the others preserve their XCom results.
* The synthesis step's inputs and token budget are fully auditable.
* A maintainer reviews the report before it goes to the dev list.

Before running:

1. Create an LLM connection named ``pydanticai_default`` (or the value of
   ``LLM_CONN_ID``) for your chosen model provider.
2. Set ``USE_SAMPLE_DATA = False`` in the DAG file to fetch live data
   from the Apache Confluence wiki and GitHub API.
"""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from datetime import timedelta

from pydantic import BaseModel
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LLM_CONN_ID = "pydanticai_default"

# Confluence wiki -- the AIP listing page is public, no auth required.
# https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals
CONFLUENCE_BASE_URL = "https://cwiki.apache.org/confluence"
AIP_LISTING_PAGE_ID = "89066602"
GITHUB_REPO = "apache/airflow"

# When True the Dag runs on built-in sample data (self-contained, no network).
# Set to False to fetch live data from Confluence and GitHub.
USE_SAMPLE_DATA = True

# ---------------------------------------------------------------------------
# Structured output model -- enforces a schema on the per-AIP LLM response
# ---------------------------------------------------------------------------

# [START aip_tracker_structured_output]


class AIPStatus(BaseModel):
    """Per-AIP analysis produced by the LLM."""

    aip_number: int
    title: str
    spec_summary: str
    implementation_status: str
    key_prs: list[str]
    blockers: list[str]
    next_steps: list[str]
    completion_pct: int


# [END aip_tracker_structured_output]

# ---------------------------------------------------------------------------
# Sample data -- replace with Confluence / GitHub API calls for production
# ---------------------------------------------------------------------------

SAMPLE_AIPS: list[dict] = [
    {"aip_number": 76, "title": "Asset Partitions"},
    {"aip_number": 99, "title": "Common Data Access Pattern + AI"},
    {"aip_number": 103, "title": "Task State Management"},
    {"aip_number": 105, "title": "LLM Retry Policy"},
    {"aip_number": 108, "title": "Language Coordinator Layer"},
]

SAMPLE_EVIDENCE: dict[int, dict] = {
    76: {
        "spec_text": (
            "AIP-76 adds partition awareness to Airflow assets. Instead of "
            "triggering on any update to an asset, Dags can depend on specific "
            "partitions (e.g. a date-based slice of a dataset). The scheduler "
            "tracks which partitions have been produced and only triggers "
            "downstream Dags when the required partitions are available."
        ),
        "prs": [
            "#62400 -- Asset partition model and metadata schema",
            "#63900 -- Partition-aware scheduling in the DagRun creator",
            "#65100 -- UI: partition status badges on Asset views",
        ],
        "commits": [
            "Add AssetPartition model with composite key",
            "Extend DagScheduleAssetReference for partition filters",
            "Show partition status in React Asset detail view",
        ],
    },
    99: {
        "spec_text": (
            "AIP-99 adds first-class AI/ML operators to the common.ai provider. "
            "LLMOperator wraps pydantic-ai for structured LLM calls with retries. "
            "AgentOperator enables multi-turn ReAct agents with tool use. "
            "LangChain and LlamaIndex hooks bridge framework models to Airflow "
            "connections. DocumentLoaderOperator parses files for RAG pipelines."
        ),
        "prs": [
            "#61200 -- LLMOperator and PydanticAIHook",
            "#62800 -- AgentOperator with tool calling and HITL",
            "#64100 -- LangChain hook and integration",
            "#65500 -- LlamaIndex embedding and retrieval operators",
            "#66300 -- DocumentLoaderOperator for multi-format parsing",
        ],
        "commits": [
            "Add LLMOperator with structured output support",
            "Add AgentOperator with ReAct loop and durable execution",
            "Add LangChainHook bridging langchain models to connections",
            "Add LlamaIndex embedding and retrieval operators",
            "Add DocumentLoaderOperator with PDF and DOCX support",
        ],
    },
    103: {
        "spec_text": (
            "AIP-103 introduces task-level state persistence via "
            "context['task_state']. Tasks can checkpoint intermediate results "
            "that survive retries and restarts. The state backend stores "
            "key-value pairs scoped to a task instance, enabling long-running "
            "tasks to resume from the last checkpoint rather than starting "
            "from scratch."
        ),
        "prs": [
            "#65000 -- Task state storage backend and API",
            "#65800 -- context['task_state'] integration in Task SDK",
            "#66700 -- State-aware retry logic for LLM tasks",
        ],
        "commits": [
            "Add TaskState model with key-value storage",
            "Expose task_state in TaskInstanceContext",
            "Add state checkpoint and restore in retry path",
        ],
    },
    105: {
        "spec_text": (
            "AIP-105 introduces LLMRetryPolicy, an intelligent retry mechanism "
            "that uses an LLM to classify task failures before deciding whether "
            "to retry, fail fast, or back off. Instead of static exception-type "
            "matching, the policy sends the error context to an LLM that "
            "determines the appropriate action: rate-limit errors trigger "
            "exponential backoff, auth errors fail immediately, transient "
            "network errors retry with a short delay."
        ),
        "prs": [
            "#64800 -- RetryPolicy base class in Task SDK (AIP-105 prerequisite)",
            "#65600 -- LLMRetryPolicy with pydantic-ai error classification",
            "#66200 -- Integration tests for LLM-classified retry scenarios",
        ],
        "commits": [
            "Add RetryPolicy protocol and RetryRule dataclass",
            "Implement LLMRetryPolicy with structured ErrorClassification output",
            "Wire RetryPolicy into task runner retry loop",
        ],
    },
    108: {
        "spec_text": (
            "AIP-108 defines a language coordinator layer that enables tasks "
            "written in Java, Go, and TypeScript to run alongside Python tasks. "
            "A lightweight coordinator process manages the non-Python runtime "
            "lifecycle, handles serialization between the task and the Execution "
            "API, and provides the same guarantees (heartbeat, state, XCom) that "
            "Python tasks get from the Task SDK."
        ),
        "prs": [
            "#66100 -- Coordinator protocol specification and protobuf schema",
            "#66800 -- Java Task SDK with coordinator bridge",
            "#67200 -- Go Task SDK initial implementation",
        ],
        "commits": [
            "Define coordinator gRPC protocol for multi-language tasks",
            "Add Java Task SDK with Maven build and coordinator client",
            "Scaffold Go Task SDK with coordinator handshake",
        ],
    },
}

# ---------------------------------------------------------------------------
# HTTP helpers -- used when USE_SAMPLE_DATA is False
# ---------------------------------------------------------------------------


def _confluence_rest_get(path: str) -> dict:
    """GET a Confluence REST API endpoint (public, no auth required)."""
    url = f"{CONFLUENCE_BASE_URL}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _github_api_get(path: str) -> dict:
    """GET a GitHub REST API endpoint (public, rate-limited to 10 req/min)."""
    url = f"https://api.github.com{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github.v3+json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _strip_html_tags(html: str) -> str:
    """Remove HTML/Confluence markup, returning plain text."""
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _parse_accepted_aips(listing_html: str) -> list[dict]:
    """Extract accepted AIPs from the rendered AIP listing page."""
    match = re.search(r"Accepted AIPs.*?(?=<h[1-3]|$)", listing_html, re.DOTALL | re.IGNORECASE)
    if not match:
        return []
    section = match.group()
    aips = []
    for m in re.finditer(r"<a[^>]+>([^<]*AIP-(\d+)[^<]*)</a>", section):
        title = re.sub(r"\s+", " ", m.group(1)).strip()
        aip_number = int(m.group(2))
        aips.append({"aip_number": aip_number, "title": title})
    return aips


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

ANALYSIS_SYSTEM_PROMPT = """\
You are an Airflow project analyst. Given an AIP specification and its \
GitHub evidence (pull requests and commits), produce a structured status \
assessment.

Be specific about what has been implemented versus what remains. Rate \
completion percentage based on the ratio of spec goals that have \
corresponding PRs or commits."""

SYNTHESIS_SYSTEM_PROMPT = """\
You are an Airflow release coordinator. Given individual AIP status \
assessments, produce a concise cross-AIP progress report.

Identify the top priorities, shared blockers across AIPs, and recommend \
where maintainer attention is most needed. Keep the report actionable \
and under 500 words."""


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


# [START example_aip_progress_tracker]
@dag(catchup=False, tags=["example", "aip_tracker", "common_ai"])
def example_aip_progress_tracker():
    """
    Track AIP progress by analysing Confluence specs against GitHub evidence.

    Task graph::

        fetch_aip_list (@task)
            → gather_aip_evidence    (@task ×N, via Dynamic Task Mapping)
            → format_analysis_prompt (@task ×N)
            → analyze_aip            (LLMOperator ×N, structured output)
            → collect_analyses       (@task)
            → synthesize_report      (LLMOperator, with UsageLimits)
            → review_report          (ApprovalOperator)
    """

    # ------------------------------------------------------------------
    # Step 1: Fetch the list of active AIPs to investigate.
    # The length of this list determines how many mapped instances are
    # created in the downstream steps -- N is decided at runtime.
    # ------------------------------------------------------------------
    @task
    def fetch_aip_list() -> list[dict]:
        if USE_SAMPLE_DATA:
            return SAMPLE_AIPS
        # Fetch the AIP listing page and extract the "Accepted" section.
        # https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals
        page = _confluence_rest_get(f"/rest/api/content/{AIP_LISTING_PAGE_ID}?expand=body.view")
        accepted = _parse_accepted_aips(page["body"]["view"]["value"])
        return accepted or SAMPLE_AIPS

    aip_list = fetch_aip_list()

    # ------------------------------------------------------------------
    # Step 2: Gather evidence for each AIP from multiple sources.
    # Each mapped instance fetches one AIP's spec text from the
    # Confluence wiki (cwiki.apache.org) and searches GitHub for
    # related PRs and commits.  If the GitHub API is rate-limited
    # for one AIP, only that instance retries.
    # ------------------------------------------------------------------
    @task
    def gather_aip_evidence(aip: dict) -> dict:
        aip_number = aip["aip_number"]
        if USE_SAMPLE_DATA:
            evidence = SAMPLE_EVIDENCE[aip_number]
            return {
                "aip_number": aip_number,
                "title": aip["title"],
                "spec_text": evidence["spec_text"],
                "prs": evidence["prs"],
                "commits": evidence["commits"],
            }
        # Fetch spec text from the AIP's Confluence wiki page via CQL search.
        # Example: https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-103
        cql = urllib.parse.quote(
            f'space="AIRFLOW" AND title~"AIP-{aip_number}" AND ancestor={AIP_LISTING_PAGE_ID}'
        )
        results = _confluence_rest_get(f"/rest/api/content/search?cql={cql}&expand=body.view&limit=1")
        spec_text = ""
        if results.get("results"):
            raw_html = results["results"][0]["body"]["view"]["value"]
            spec_text = _strip_html_tags(raw_html)[:3000]
        # Search GitHub for related PRs.
        pr_query = urllib.parse.quote(f"AIP-{aip_number} repo:{GITHUB_REPO} is:pr")
        pr_data = _github_api_get(f"/search/issues?q={pr_query}&per_page=10")
        prs = [f"#{it['number']} -- {it['title']}" for it in pr_data.get("items", [])]
        # Search GitHub for related commits.
        commit_query = urllib.parse.quote(f"AIP-{aip_number} repo:{GITHUB_REPO}")
        commit_data = _github_api_get(f"/search/commits?q={commit_query}&per_page=10")
        commits = [it["commit"]["message"].split("\n")[0] for it in commit_data.get("items", [])]
        return {
            "aip_number": aip_number,
            "title": aip["title"],
            "spec_text": spec_text,
            "prs": prs,
            "commits": commits,
        }

    evidence = gather_aip_evidence.expand(aip=aip_list)

    # ------------------------------------------------------------------
    # Step 3: Format the gathered evidence into an LLM analysis prompt.
    # Separating formatting from data gathering keeps each task focused
    # and makes prompt iteration independent of API logic.
    # ------------------------------------------------------------------
    @task
    def format_analysis_prompt(evidence: dict) -> str:
        prs_text = "\n".join(f"  - {pr}" for pr in evidence["prs"])
        commits_text = "\n".join(f"  - {c}" for c in evidence["commits"])
        return (
            f"Analyze AIP-{evidence['aip_number']}: {evidence['title']}\n\n"
            f"Specification:\n{evidence['spec_text']}\n\n"
            f"Pull Requests:\n{prs_text}\n\n"
            f"Recent Commits:\n{commits_text}"
        )

    prompts = format_analysis_prompt.expand(evidence=evidence)

    # ------------------------------------------------------------------
    # Step 4: Analyze each AIP with a structured LLM call.
    # Dynamic Task Mapping creates one LLMOperator instance per AIP.
    # output_type=AIPStatus enforces the Pydantic schema on the response.
    # ------------------------------------------------------------------
    # [START aip_tracker_dtm_analysis]
    analyses = LLMOperator.partial(
        task_id="analyze_aip",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=ANALYSIS_SYSTEM_PROMPT,
        output_type=AIPStatus,
    ).expand(prompt=prompts)
    # [END aip_tracker_dtm_analysis]

    # ------------------------------------------------------------------
    # Step 5: Collect all per-AIP analyses into a single context string
    # for the synthesis step.
    # ------------------------------------------------------------------
    @task
    def collect_analyses(analyses: list) -> str:
        sections = []
        for raw in analyses:
            a = json.loads(raw) if isinstance(raw, str) else raw
            blockers = ", ".join(a["blockers"]) if a["blockers"] else "None identified"
            next_steps = ", ".join(a["next_steps"]) if a["next_steps"] else "N/A"
            sections.append(
                f"## AIP-{a['aip_number']}: {a['title']}\n"
                f"Status: {a['implementation_status']} "
                f"({a['completion_pct']}% complete)\n"
                f"Summary: {a['spec_summary']}\n"
                f"Key PRs: {', '.join(a['key_prs'])}\n"
                f"Blockers: {blockers}\n"
                f"Next steps: {next_steps}"
            )
        return "\n\n".join(sections)

    collected = collect_analyses(analyses.output)

    # ------------------------------------------------------------------
    # Step 6: Synthesize a cross-AIP progress report.
    # UsageLimits caps the token spend so a runaway prompt cannot
    # exhaust the API budget in a single Dag run.
    # ------------------------------------------------------------------
    # [START aip_tracker_synthesis]
    synthesize = LLMOperator(
        task_id="synthesize_report",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=SYNTHESIS_SYSTEM_PROMPT,
        prompt="""\
Create a cross-AIP progress report from these individual assessments.
Prioritize AIPs that are close to completion or have shared blockers.

{{ ti.xcom_pull(task_ids='collect_analyses') }}""",
        usage_limits=UsageLimits(
            request_limit=5,
            input_tokens_limit=20_000,
            output_tokens_limit=4_000,
        ),
    )
    # [END aip_tracker_synthesis]
    collected >> synthesize

    # ------------------------------------------------------------------
    # Step 7: A maintainer reviews the synthesized report before it is
    # shared on the dev list.  The Dag pauses here until the human
    # approves, requests changes, or the timeout expires.
    # ------------------------------------------------------------------
    # [START aip_tracker_hitl]
    ApprovalOperator(
        task_id="review_report",
        subject="Review AIP Progress Report before sharing",
        body=synthesize.output,
        response_timeout=timedelta(hours=24),
    )
    # [END aip_tracker_hitl]


# [END example_aip_progress_tracker]

example_aip_progress_tracker()

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
AIP progress tracker -- two approaches to the same problem with common.ai.

This file contains **two DAGs** that solve the same use case -- tracking
Airflow Improvement Proposal implementation progress -- using different
architectural patterns. Comparing them illustrates the tradeoff between
deterministic control and agent autonomy.

``example_aip_progress_tracker`` (pipeline approach):

.. code-block:: text

    fetch_aip_list  (@task)  ─┐
                               ├─> gather_aip_evidence  (@task, mapped ×N AIPs)
    fetch_repo_tree (@task)  ─┘    → format_analysis_prompt (@task, mapped ×N)
                                   → analyze_aip           (LLMOperator, mapped ×N)
                                   → collect_analyses      (@task)
                                   → format_report         (@task)
                                   → synthesize_report     (LLMOperator, UsageLimits)
                                   → validate_report       (LLMOperator, hallucination check)
                                   → apply_validation      (@task, deterministic corrections)
                                   → build_review_body     (@task)
                                   → review_report         (ApprovalOperator)

The pipeline gathers evidence deterministically, then uses LLMs only for
analysis and synthesis -- with a three-layer quality pipeline (structured
output → AI validation → arithmetic correction) to prevent hallucination.

``example_aip_progress_tracker_skills`` (skills approach):

.. code-block:: text

    track_aip_progress  (AgentOperator + AgentSkillsToolset)
        → review_report (ApprovalOperator)

The agent loads the ``aip-tracker`` skill (an `agentskills.io
<https://agentskills.io>`__ ``SKILL.md`` bundle) which teaches it how to
assess AIP progress. Custom tools give it access to Confluence and GitHub
APIs. The agent decides its own evidence-gathering strategy -- simpler DAG,
but less control over accuracy.

**When to use which:**

* **Pipeline** when accuracy is critical and you want full auditability of
  every evidence source and LLM judgment.
* **Agent** when you want a quick assessment and trust the model to follow
  skill instructions, or when the problem is too open-ended for a fixed
  pipeline.

Before running either DAG:

1. Create an LLM connection named ``pydanticai_default`` (or the value of
   ``LLM_CONN_ID``) for your chosen model provider.
2. Optionally set a ``GITHUB_TOKEN`` environment variable for higher API
   rate limits (unauthenticated: 10 req/min; authenticated: 5,000 req/hr).
3. Trigger the DAG with the default ``aip_numbers`` param or edit it to
   choose which AIPs to investigate.
4. The agent DAG requires the ``skills`` extra:
   ``pip install "apache-airflow-providers-common-ai[skills]"``.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.toolsets.skills import AgentSkillsToolset
from airflow.providers.common.compat.sdk import dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Param

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LLM_CONN_ID = "pydanticai_default"
CONFLUENCE_BASE_URL = "https://cwiki.apache.org/confluence"
GITHUB_REPO = "apache/airflow"
GITHUB_API_DELAY = 7  # seconds between unauthenticated GitHub API calls
DEFAULT_AIP_NUMBERS = "76,99,103,105,108"

# ---------------------------------------------------------------------------
# AIP Registry -- page IDs, search aliases, and codebase paths
#
# Each entry enables multi-strategy evidence gathering:
# - page_id: direct Confluence fetch (no CQL search needed)
# - search_terms: additional GitHub search keywords beyond "AIP-{N}"
# - codebase_paths: directory prefixes to look for in the GitHub file tree
# ---------------------------------------------------------------------------

# [START aip_registry]
AIP_REGISTRY: dict[int, dict] = {
    76: {
        "page_id": "311626969",
        "topic": "Asset Partitions",
        "search_terms": ["asset partition", "PartitionMapper", "PartitionedAsset"],
        "codebase_paths": [
            "airflow-core/src/airflow/models/asset.py",
            "task-sdk/src/airflow/sdk/definitions/partition_mappers",
            "task-sdk/src/airflow/sdk/definitions/timetables/assets.py",
            "airflow-core/src/airflow/migrations/versions/0095_",
            "airflow-core/src/airflow/migrations/versions/0106_",
            "airflow-core/src/airflow/migrations/versions/0107_",
        ],
    },
    99: {
        "page_id": "406618285",
        "topic": "Common AI Operators",
        "search_terms": ["LLMOperator", "AgentOperator", "common.ai", "LangChainHook"],
        "codebase_paths": [
            "providers/common/ai/src/airflow/providers/common/ai/operators",
            "providers/common/ai/src/airflow/providers/common/ai/toolsets",
            "providers/common/ai/src/airflow/providers/common/ai/hooks",
            "providers/common/ai/src/airflow/providers/common/ai/decorators",
            "providers/common/ai/src/airflow/providers/common/ai/durable",
            "providers/common/ai/src/airflow/providers/common/ai/example_dags",
        ],
    },
    103: {
        "page_id": "406623137",
        "topic": "Task State Management",
        "search_terms": ["task_state_store", "asset_state_store", "state_store", "TaskStateStoreAccessor"],
        "codebase_paths": [
            "airflow-core/src/airflow/models/task_state_store.py",
            "airflow-core/src/airflow/models/asset_state_store.py",
            "airflow-core/src/airflow/state",
            "shared/state/src/airflow_shared/state",
            "airflow-core/src/airflow/api_fastapi/execution_api/routes/task_state_store.py",
            "airflow-core/src/airflow/api_fastapi/execution_api/routes/asset_state_store.py",
        ],
    },
    105: {
        "page_id": "421955342",
        "topic": "Pluggable Retry Policies",
        "search_terms": ["RetryPolicy", "retry_policy", "ExceptionRetryPolicy"],
        "codebase_paths": [
            "task-sdk/src/airflow/sdk/definitions/retry_policy.py",
            "airflow-core/src/airflow/migrations/versions/0113_",
        ],
    },
    108: {
        "page_id": "421957285",
        "topic": "Language Task SDK + Coordinator",
        "search_terms": ["coordinator", "Go-SDK", "Go SDK", "Java SDK", "SubprocessCoordinator"],
        "codebase_paths": [
            "task-sdk/src/airflow/sdk/coordinators",
            "task-sdk/src/airflow/sdk/execution_time/coordinator.py",
            "go-sdk",
            "java-sdk",
        ],
    },
}
# [END aip_registry]

# ---------------------------------------------------------------------------
# Structured output models
# ---------------------------------------------------------------------------

# [START aip_tracker_structured_output]


class DeliverableStatus(BaseModel):
    """Status of a single deliverable from the AIP spec."""

    name: str
    status: Literal["shipped", "in_progress", "not_started", "beyond_spec", "unclear"]
    evidence: str
    confidence: Literal["high", "medium", "low"]


class AIPStatus(BaseModel):
    """Per-AIP analysis produced by the LLM."""

    aip_number: int
    title: str
    spec_summary: str
    deliverables: list[DeliverableStatus]
    shipped_count: int
    total_count: int
    key_prs: list[str]
    confluence_update_needed: bool
    notes: str


# [END aip_tracker_structured_output]

# [START aip_tracker_validation_output]


class ClaimValidation(BaseModel):
    """A single claim from the report checked against evidence."""

    claim: str
    grounded: bool
    evidence_found: str
    correction: str


class ValidationResult(BaseModel):
    """Result of the AI hallucination validation step."""

    overall_verdict: Literal["pass", "pass_with_warnings", "fail"]
    ungrounded_claims: list[ClaimValidation]
    hallucination_risk: Literal["low", "medium", "high"]


# [END aip_tracker_validation_output]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _github_headers() -> dict[str, str]:
    """Build GitHub API headers, using a token if available."""
    headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": "airflow-aip-tracker/1.0"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _github_api_get(path: str, *, delay: bool = True) -> dict:
    """GET a GitHub REST API endpoint with rate-limit awareness."""
    url = f"https://api.github.com{path}"
    req = urllib.request.Request(url, headers=_github_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            remaining = resp.headers.get("X-RateLimit-Remaining")
            if delay and not os.environ.get("GITHUB_TOKEN") and remaining:
                if int(remaining) < 5:
                    time.sleep(GITHUB_API_DELAY)
            return result
    except urllib.error.HTTPError as e:
        if e.code == 403:
            return {}
        raise


def _confluence_rest_get(path: str) -> dict:
    """GET a Confluence REST API endpoint (public, no auth required)."""
    url = f"{CONFLUENCE_BASE_URL}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _strip_html_tags(html: str) -> str:
    """Remove HTML/Confluence markup, returning plain text."""
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _extract_spec_sections(html: str) -> dict:
    """Parse Confluence HTML into structured sections by heading."""
    sections: dict[str, str] = {}
    current_heading = "introduction"
    current_text: list[str] = []

    for part in re.split(r"(<h[1-4][^>]*>.*?</h[1-4]>)", html, flags=re.IGNORECASE | re.DOTALL):
        heading_match = re.match(r"<h[1-4][^>]*>(.*?)</h[1-4]>", part, re.IGNORECASE | re.DOTALL)
        if heading_match:
            if current_text:
                sections[current_heading] = _strip_html_tags(" ".join(current_text))
            current_heading = _strip_html_tags(heading_match.group(1)).lower().strip()
            current_text = []
        else:
            current_text.append(part)

    if current_text:
        sections[current_heading] = _strip_html_tags(" ".join(current_text))

    return sections


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

ANALYSIS_SYSTEM_PROMPT = """\
You are an Airflow project analyst assessing AIP implementation progress.

DELIVERABLE EXTRACTION:
Extract deliverables from the specification's own structure. Use these \
sources in priority order:
1. Numbered completion criteria (e.g. "Definition of Done", "Completion \
Criteria") -- each numbered item is one deliverable.
2. Phase definitions -- each bullet or item under a phase heading is one \
deliverable.
3. Explicitly enumerated components (classes, operators, API endpoints, \
CLI commands, UI features) listed in the spec.
Do NOT split a single spec item into multiple deliverables. Do NOT merge \
multiple spec items into one. Use the spec's own granularity.

ASSESSMENT RULES:
1. For each deliverable, you MUST cite specific evidence (a PR number, commit \
message, or file path from the provided data). If no evidence exists, set \
status to "not_started" or "unclear" and confidence to "low".
2. Do NOT guess completion percentages. Instead, count shipped vs total \
deliverables. shipped_count and total_count must match the deliverables list.
3. Do NOT invent blockers. Use the notes field for genuine uncertainties only.
4. If codebase evidence shows shipped work NOT mentioned in the spec, add \
those as deliverables with status "beyond_spec" and set \
confluence_update_needed to true.
5. PR numbers must come from the Pull Requests section of the input. Do not \
invent PR numbers."""

SYNTHESIS_SYSTEM_PROMPT = """\
You are an Airflow release coordinator. Given individual AIP status \
assessments, produce a concise cross-AIP progress report.

RULES:
1. Use ONLY the data from the individual assessments. Do not add information \
not present in the inputs.
2. Always write progress as "X/Y deliverables shipped" (e.g. "8/14 shipped"). \
NEVER convert to percentages. Do not write "57%" or "90%" or any other \
percentage. The fraction form is the only acceptable format.
3. Identify AIPs where confluence_update_needed is true.
4. Flag deliverables with confidence="low" as needing manual verification.
5. Do NOT characterize AIPs as "near completion" or "minimal blockers" unless \
the evidence explicitly supports that. Use the fraction (e.g. "9/10 shipped, \
1 in progress") and let the reader draw conclusions.
6. Keep the report actionable and under 500 words."""

VALIDATION_SYSTEM_PROMPT = """\
You are a fact-checker for an AIP progress report. You receive two inputs:
1. A synthesized cross-AIP progress report
2. The raw per-AIP evidence that the report was derived from

Your ONLY job is to verify claims. A separate downstream step applies your \
corrections, so each correction must be a self-contained replacement string \
that can be substituted for the original claim text.

RULES:
1. Any deliverable status, PR number, shipped count, or recommendation in \
the report must have a corresponding entry in the raw evidence.
2. If a claim has no supporting evidence, set grounded=false and provide a \
correction. The "claim" field must contain the EXACT text from the report \
(so it can be found by string search). The "correction" field must contain \
the replacement text, or "REMOVE" if the claim should be deleted entirely.
3. Flag invented blockers, fabricated statistics, and PR numbers not in the \
evidence.
4. Flag any percentages (e.g. "57%", "90%") as ungrounded. Progress must be \
expressed as fractions ("8/14 shipped"), never as percentages.
5. Flag vague characterizations ("near completion", "minimal blockers", \
"requires foundational work") that editorialize beyond the evidence. Provide \
a factual replacement using data from the evidence.
6. Set overall_verdict to "fail" if any high-confidence claims are ungrounded, \
"pass_with_warnings" if only low-confidence claims are flagged, "pass" if \
all claims are grounded."""


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


# [START example_aip_progress_tracker]
@dag(
    schedule=None,
    catchup=False,
    params={
        "aip_numbers": Param(
            DEFAULT_AIP_NUMBERS,
            type="string",
            description="Comma-separated AIP numbers to investigate (e.g. 76,99,103,105,108)",
        ),
    },
    tags=["example", "aip_tracker", "common_ai"],
)
def example_aip_progress_tracker():
    """
    Track AIP progress by analyzing Confluence specs against GitHub evidence.

    Task graph::

        fetch_aip_list  ─┐
                          ├─> gather_aip_evidence   (@task ×N, Dynamic Task Mapping)
        fetch_repo_tree ─┘    → format_analysis_prompt (@task ×N)
                              → analyze_aip            (LLMOperator ×N, structured output)
                              → collect_analyses       (@task)
                              → format_report          (@task)
                              → synthesize_report      (LLMOperator, with UsageLimits)
                              → validate_report        (LLMOperator, hallucination check)
                              → apply_validation       (@task, deterministic corrections)
                              → build_review_body      (@task)
                              → review_report          (ApprovalOperator)
    """

    # ------------------------------------------------------------------
    # Step 1: Build the AIP list from the registry, using Confluence page
    # IDs for direct spec fetching (no CQL search needed).
    # ------------------------------------------------------------------
    @task
    def fetch_aip_list(params: dict) -> list[dict]:
        aip_numbers = [int(n.strip()) for n in params["aip_numbers"].split(",") if n.strip()]
        aips = []
        for num in aip_numbers:
            registry_entry = AIP_REGISTRY.get(num)
            if registry_entry:
                aips.append(
                    {
                        "aip_number": num,
                        "title": f"AIP-{num}: {registry_entry['topic']}",
                        "page_id": registry_entry["page_id"],
                        "search_terms": registry_entry["search_terms"],
                        "codebase_paths": registry_entry["codebase_paths"],
                    }
                )
            else:
                aips.append(
                    {
                        "aip_number": num,
                        "title": f"AIP-{num}",
                        "page_id": None,
                        "search_terms": [],
                        "codebase_paths": [],
                    }
                )
        return aips

    aip_list = fetch_aip_list()

    # ------------------------------------------------------------------
    # Step 2: Fetch the repo file tree from GitHub once, shared across
    # all mapped AIP tasks.  Falls back to per-path directory listings
    # if the tree is truncated (common for large repos).
    # ------------------------------------------------------------------
    @task
    def fetch_repo_tree() -> list[str]:
        data = _github_api_get(f"/repos/{GITHUB_REPO}/git/trees/main?recursive=1", delay=False)
        if not data:
            return []

        if not data.get("truncated"):
            return [item["path"] for item in data.get("tree", []) if item["type"] == "blob"]

        # Tree was truncated -- fetch directory listings for known paths
        known_dirs: set[str] = set()
        for entry in AIP_REGISTRY.values():
            for p in entry["codebase_paths"]:
                parts = p.rstrip("/").split("/")
                if len(parts) >= 2:
                    known_dirs.add("/".join(parts[:3]))

        all_files: list[str] = []
        for dir_path in sorted(known_dirs):
            contents = _github_api_get(f"/repos/{GITHUB_REPO}/contents/{dir_path}")
            if isinstance(contents, list):
                for item in contents:
                    if item["type"] == "file":
                        all_files.append(item["path"])
                    elif item["type"] == "dir":
                        sub = _github_api_get(f"/repos/{GITHUB_REPO}/contents/{item['path']}")
                        if isinstance(sub, list):
                            all_files.extend(s["path"] for s in sub if s["type"] == "file")
        return all_files

    repo_tree = fetch_repo_tree()

    # ------------------------------------------------------------------
    # Step 3: Gather evidence from Confluence, GitHub, and the file tree.
    # Each mapped instance fetches one AIP's data.  Multi-strategy GitHub
    # search uses both the AIP tag and topic-specific keywords.
    # ------------------------------------------------------------------
    @task
    def gather_aip_evidence(aip: dict, repo_tree: list[str]) -> dict:
        aip_number = aip["aip_number"]

        # --- Confluence spec ---
        spec_sections: dict[str, str] = {}
        last_modified = ""
        if aip.get("page_id"):
            page = _confluence_rest_get(f"/rest/api/content/{aip['page_id']}?expand=body.storage,version")
            raw_html = page.get("body", {}).get("storage", {}).get("value", "")
            spec_sections = _extract_spec_sections(raw_html)
            version_info = page.get("version", {})
            last_modified = version_info.get("when", "")
        else:
            cql = urllib.parse.quote(f'space="AIRFLOW" AND title~"AIP-{aip_number}"')
            results = _confluence_rest_get(f"/rest/api/content/search?cql={cql}&expand=body.storage&limit=1")
            if results.get("results"):
                raw_html = results["results"][0].get("body", {}).get("storage", {}).get("value", "")
                spec_sections = _extract_spec_sections(raw_html)

        # Build spec text with section budgets
        spec_parts = []
        for heading in (
            "status",
            "deliverables",
            "milestones",
            "phases",
            "scope",
            "completion criteria",
            "definition of done",
        ):
            for key, text in spec_sections.items():
                if heading in key:
                    spec_parts.append(f"[{key}]: {text[:4000]}")
                    break

        remaining_budget = 8000 - sum(len(p) for p in spec_parts)
        for key, text in spec_sections.items():
            if not any(key in p for p in spec_parts):
                chunk = text[: min(1000, max(200, remaining_budget))]
                spec_parts.append(f"[{key}]: {chunk}")
                remaining_budget -= len(chunk)
                if remaining_budget <= 0:
                    break

        # --- GitHub: multi-strategy PR and commit search ---
        seen_pr_numbers: set[int] = set()
        prs: list[str] = []
        seen_commit_shas: set[str] = set()
        commits: list[str] = []

        search_queries = [f"AIP-{aip_number}"]
        for term in aip.get("search_terms", [])[:2]:
            search_queries.append(term)

        for query_term in search_queries:
            # PR search
            pr_query = urllib.parse.quote(f"{query_term} repo:{GITHUB_REPO} is:pr")
            pr_data = _github_api_get(f"/search/issues?q={pr_query}&per_page=10")
            for item in pr_data.get("items", []):
                if item["number"] not in seen_pr_numbers:
                    seen_pr_numbers.add(item["number"])
                    state = "merged" if item.get("pull_request", {}).get("merged_at") else item["state"]
                    prs.append(f"#{item['number']} ({state}) -- {item['title']}")

            time.sleep(GITHUB_API_DELAY if not os.environ.get("GITHUB_TOKEN") else 1)

            # Commit search
            commit_query = urllib.parse.quote(f"{query_term} repo:{GITHUB_REPO}")
            commit_data = _github_api_get(f"/search/commits?q={commit_query}&per_page=10")
            for item in commit_data.get("items", []):
                sha = item["sha"][:7]
                if sha not in seen_commit_shas:
                    seen_commit_shas.add(sha)
                    commits.append(f"{sha} -- {item['commit']['message'].split(chr(10))[0]}")

            time.sleep(GITHUB_API_DELAY if not os.environ.get("GITHUB_TOKEN") else 1)

        # --- File tree evidence ---
        file_evidence: list[str] = []
        for path_prefix in aip.get("codebase_paths", []):
            matching = [f for f in repo_tree if f.startswith(path_prefix)]
            if matching:
                test_files = [f for f in matching if "/test_" in f or "/tests/" in f]
                file_evidence.append(
                    f"{path_prefix}: {len(matching)} files"
                    + (f" ({len(test_files)} test files)" if test_files else "")
                )
                for f in matching[:5]:
                    file_evidence.append(f"  - {f}")
                if len(matching) > 5:
                    file_evidence.append(f"  ... and {len(matching) - 5} more")

        return {
            "aip_number": aip_number,
            "title": aip["title"],
            "last_modified": last_modified,
            "spec_sections": spec_parts,
            "prs": prs,
            "commits": commits,
            "file_evidence": file_evidence,
        }

    evidence = gather_aip_evidence.partial(repo_tree=repo_tree).expand(aip=aip_list)

    # ------------------------------------------------------------------
    # Step 4: Format the gathered evidence into an LLM analysis prompt.
    # Clear section headers help the LLM distinguish spec claims from
    # implementation evidence.
    # ------------------------------------------------------------------
    @task
    def format_analysis_prompt(evidence: dict) -> str:
        spec_text = "\n".join(evidence.get("spec_sections", [])) or "(spec not available)"
        prs_text = "\n".join(f"  - {pr}" for pr in evidence["prs"]) or "  (none found)"
        commits_text = "\n".join(f"  - {c}" for c in evidence["commits"]) or "  (none found)"
        files_text = "\n".join(evidence.get("file_evidence", [])) or "  (none found)"

        modified = (
            f"\nLast Confluence update: {evidence['last_modified']}" if evidence.get("last_modified") else ""
        )

        return (
            f"Analyze {evidence['title']}{modified}\n\n"
            f"=== SPECIFICATION (from Confluence) ===\n{spec_text}\n\n"
            f"=== PULL REQUESTS (from GitHub, {len(evidence['prs'])} found) ===\n{prs_text}\n\n"
            f"=== COMMITS (from GitHub, {len(evidence['commits'])} found) ===\n{commits_text}\n\n"
            f"=== CODEBASE FILES (from GitHub tree) ===\n{files_text}"
        )

    prompts = format_analysis_prompt.expand(evidence=evidence)

    # ------------------------------------------------------------------
    # Step 5: Analyze each AIP with a structured LLM call.
    # Dynamic Task Mapping creates one LLMOperator instance per AIP.
    # output_type=AIPStatus enforces the Pydantic schema.
    # ------------------------------------------------------------------
    # [START aip_tracker_dtm_analysis]
    analyses = LLMOperator.partial(
        task_id="analyze_aip",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=ANALYSIS_SYSTEM_PROMPT,
        output_type=AIPStatus,
        serialize_output=True,
        agent_params={"model_settings": {"temperature": 0}},
    ).expand(prompt=prompts)
    # [END aip_tracker_dtm_analysis]

    # ------------------------------------------------------------------
    # Step 6: Collect all per-AIP analyses into a combined context.
    # ------------------------------------------------------------------
    @task
    def collect_analyses(analyses: list) -> list[dict]:
        result = []
        for raw in analyses:
            if hasattr(raw, "model_dump"):
                result.append(raw.model_dump())
            elif isinstance(raw, str):
                result.append(json.loads(raw))
            else:
                result.append(raw)
        return result

    collected = collect_analyses(analyses.output)

    # ------------------------------------------------------------------
    # Step 7: Format the structured analyses as readable markdown.
    # This serves as a non-LLM audit artifact and provides cleaner
    # input for the synthesis and validation steps.
    # ------------------------------------------------------------------
    @task
    def format_report(analyses: list[dict]) -> str:
        sections = []
        for a in analyses:
            shipped = [d for d in a["deliverables"] if d["status"] == "shipped"]
            beyond = [d for d in a["deliverables"] if d["status"] == "beyond_spec"]
            in_progress = [d for d in a["deliverables"] if d["status"] == "in_progress"]
            not_started = [d for d in a["deliverables"] if d["status"] == "not_started"]
            unclear = [d for d in a["deliverables"] if d["status"] == "unclear"]

            lines = [
                f"## AIP-{a['aip_number']}: {a['title']}",
                f"Progress: {a['shipped_count']}/{a['total_count']} deliverables shipped",
                f"Confluence update needed: {'YES' if a['confluence_update_needed'] else 'No'}",
                f"\n{a['spec_summary']}",
            ]
            if shipped:
                lines.append(f"\n**Shipped ({len(shipped)}):**")
                for d in shipped:
                    lines.append(f"  - {d['name']}: {d['evidence']} [{d['confidence']}]")
            if beyond:
                lines.append(f"\n**Beyond spec ({len(beyond)}):**")
                for d in beyond:
                    lines.append(f"  - {d['name']}: {d['evidence']} [{d['confidence']}]")
            if in_progress:
                lines.append(f"\n**In progress ({len(in_progress)}):**")
                for d in in_progress:
                    lines.append(f"  - {d['name']}: {d['evidence']} [{d['confidence']}]")
            if not_started:
                lines.append(f"\n**Not started ({len(not_started)}):**")
                for d in not_started:
                    lines.append(f"  - {d['name']}")
            if unclear:
                lines.append(f"\n**Unclear ({len(unclear)}):**")
                for d in unclear:
                    lines.append(f"  - {d['name']}: {d['evidence']} [{d['confidence']}]")
            if a.get("notes"):
                lines.append(f"\n**Notes:** {a['notes']}")
            if a["key_prs"]:
                lines.append(f"\n**Key PRs:** {', '.join(a['key_prs'])}")
            sections.append("\n".join(lines))

        return "\n\n---\n\n".join(sections)

    formatted = format_report(collected)

    # ------------------------------------------------------------------
    # Step 8: Synthesize a cross-AIP progress report.
    # UsageLimits caps the token spend so a runaway prompt cannot
    # exhaust the API budget in a single DAG run.
    # ------------------------------------------------------------------
    # [START aip_tracker_synthesis]
    synthesize = LLMOperator(
        task_id="synthesize_report",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=SYNTHESIS_SYSTEM_PROMPT,
        prompt="""\
Create a cross-AIP progress report from these individual assessments.
Prioritize AIPs that are close to completion or have shared dependencies.
Use only the data below -- do not add external information.

{{ ti.xcom_pull(task_ids='format_report') }}""",
        agent_params={"model_settings": {"temperature": 0}},
        usage_limits=UsageLimits(
            request_limit=5,
            input_tokens_limit=20_000,
            output_tokens_limit=4_000,
        ),
    )
    # [END aip_tracker_synthesis]
    formatted >> synthesize

    # ------------------------------------------------------------------
    # Step 9: AI-powered hallucination validation.
    # A second LLM checks every claim in the synthesized report against
    # the raw per-AIP evidence.  Its only job is to judge and propose
    # corrections -- a separate deterministic step applies them.
    # ------------------------------------------------------------------
    # [START aip_tracker_validation]
    validate = LLMOperator(
        task_id="validate_report",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=VALIDATION_SYSTEM_PROMPT,
        prompt="""\
Verify the following synthesized report against the raw per-AIP evidence.
Flag any claims not grounded in the evidence.

=== SYNTHESIZED REPORT ===
{{ ti.xcom_pull(task_ids='synthesize_report') }}

=== RAW PER-AIP EVIDENCE ===
{{ ti.xcom_pull(task_ids='format_report') }}""",
        output_type=ValidationResult,
        serialize_output=True,
        usage_limits=UsageLimits(
            request_limit=5,
            input_tokens_limit=30_000,
            output_tokens_limit=8_000,
        ),
        agent_params={"model_settings": {"temperature": 0}},
    )
    # [END aip_tracker_validation]
    synthesize >> validate

    # ------------------------------------------------------------------
    # Step 10: Apply validation corrections deterministically.
    # No LLM involved -- this is a mechanical find-and-replace using
    # the validator's claim/correction pairs.  Ensures every flagged
    # issue is actually fixed in the final report.
    # ------------------------------------------------------------------
    @task
    def apply_validation(report: str, validation: dict, analyses: list[dict]) -> dict:
        corrected = report
        applied = 0
        for claim in validation.get("ungrounded_claims", []):
            if claim["grounded"]:
                continue
            original = claim.get("claim", "")
            correction = claim.get("correction", "")
            if not original:
                continue
            if correction == "REMOVE":
                if original in corrected:
                    corrected = corrected.replace(original, "")
                    applied += 1
            elif correction and original in corrected:
                corrected = corrected.replace(original, correction)
                applied += 1

        # --- Arithmetic validation against ground-truth analysis data ---
        # Build lookup of correct counts from the structured per-AIP analyses.
        truth = {}
        total_shipped_all = 0
        total_all = 0
        for a in analyses:
            aip_num = a["aip_number"]
            shipped = a["shipped_count"]
            total = a["total_count"]
            truth[aip_num] = (shipped, total)
            total_shipped_all += shipped
            total_all += total

        arithmetic_fixes = 0

        # Fix per-AIP fractions: find "X/Y" near "AIP-{N}" and correct if wrong.
        for aip_num, (shipped, total) in truth.items():
            pattern = re.compile(rf"(AIP-{aip_num}\b[^\n]{{0,80}}?)\b(\d+)/(\d+)\b")
            for match in pattern.finditer(corrected):
                found_x, found_y = int(match.group(2)), int(match.group(3))
                if found_x != shipped or found_y != total:
                    old = match.group(0)
                    new = old[: match.start(2) - match.start(0)] + f"{shipped}/{total}"
                    corrected = corrected.replace(old, new, 1)
                    arithmetic_fixes += 1

        # Fix the cross-AIP summary total.
        summary_pattern = re.compile(r"(Five|Four|Three|\d+)\s+AIPs\s+tracked\s+across\s+(\d+)/(\d+)")
        summary_match = summary_pattern.search(corrected)
        if summary_match:
            found_shipped = int(summary_match.group(2))
            found_total = int(summary_match.group(3))
            if found_shipped != total_shipped_all or found_total != total_all:
                old_summary = summary_match.group(0)
                aip_count = len(truth)
                number_words = {3: "Three", 4: "Four", 5: "Five"}
                count_word = number_words.get(aip_count, str(aip_count))
                new_summary = f"{count_word} AIPs tracked across {total_shipped_all}/{total_all}"
                corrected = corrected.replace(old_summary, new_summary, 1)
                arithmetic_fixes += 1

        return {
            "corrected_report": corrected.strip(),
            "verdict": validation.get("overall_verdict", "unknown"),
            "hallucination_risk": validation.get("hallucination_risk", "unknown"),
            "total_claims_flagged": len(
                [c for c in validation.get("ungrounded_claims", []) if not c["grounded"]]
            ),
            "corrections_applied": applied,
            "arithmetic_fixes": arithmetic_fixes,
            "ungrounded_claims": validation.get("ungrounded_claims", []),
        }

    validated = apply_validation(synthesize.output, validate.output, collected)

    # ------------------------------------------------------------------
    # Step 11: Build the review body for the human reviewer, showing
    # the corrected report, validation verdict, and any flagged claims.
    # ------------------------------------------------------------------
    @task
    def build_review_body(validated: dict) -> str:
        verdict = validated["verdict"]
        risk = validated["hallucination_risk"]
        flagged = validated["total_claims_flagged"]
        applied = validated["corrections_applied"]
        arith = validated.get("arithmetic_fixes", 0)
        claims = validated["ungrounded_claims"]

        lines = [
            f"# AIP Progress Report (Validation: {verdict.upper()})",
            f"Hallucination risk: {risk}",
            f"Claims flagged: {flagged} | Corrections applied: {applied} | Arithmetic fixes: {arith}\n",
        ]

        if claims:
            ungrounded = [c for c in claims if not c["grounded"]]
            if ungrounded:
                lines.append(f"## {len(ungrounded)} Ungrounded Claim(s)\n")
                for claim in ungrounded:
                    lines.append(f"- **Original:** {claim['claim']}")
                    correction = claim.get("correction", "")
                    if correction == "REMOVE":
                        lines.append("  **Action:** Removed")
                    elif correction:
                        lines.append(f"  **Replaced with:** {correction}")
                lines.append("")

        lines.append("---\n")
        lines.append(validated["corrected_report"])

        return "\n".join(lines)

    review_body = build_review_body(validated)

    # ------------------------------------------------------------------
    # Step 12: A maintainer reviews the corrected report.  The DAG
    # pauses here until the human approves, requests changes, or the
    # timeout expires.
    # ------------------------------------------------------------------
    # [START aip_tracker_hitl]
    ApprovalOperator(
        task_id="review_report",
        subject="Review AIP Progress Report (AI-validated)",
        body=review_body,
        response_timeout=timedelta(hours=24),
    )
    # [END aip_tracker_hitl]


# [END example_aip_progress_tracker]

example_aip_progress_tracker()


# ===========================================================================
# DAG 2: Agent-based AIP tracker (AgentOperator + AgentSkillsToolset)
#
# Same use case, different architecture.  Instead of a 12-task deterministic
# pipeline, a single AgentOperator with the aip-tracker skill loaded via
# AgentSkillsToolset.  The agent discovers the skill's grounding rules and
# calls custom tools to gather evidence from Confluence and GitHub.
# ===========================================================================

SKILLS_DIR = str(Path(__file__).parent / "skills")

# ---------------------------------------------------------------------------
# Tool functions the agent can call to gather evidence.
# These are plain Python functions -- the agent sees their docstrings and
# decides when and how to call them based on the skill instructions.
# Reuses _github_headers and _safe_api_get defined above.
# ---------------------------------------------------------------------------


def _safe_api_get(url: str, headers: dict[str, str] | None = None) -> dict | list | str:
    """GET a URL, returning parsed JSON or an error string."""
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}: {e.reason}"
    except Exception as e:
        return f"Error: {e}"


def fetch_confluence_page(page_id: str) -> str:
    """Fetch an AIP specification page from the Apache Confluence wiki.

    Args:
        page_id: The Confluence page ID (e.g. "311626969" for AIP-76).

    Returns:
        The page title, last-modified date, and body content (HTML).
        Returns an error message if the page cannot be fetched.
    """
    url = f"{CONFLUENCE_BASE_URL}/rest/api/content/{page_id}?expand=body.storage,version"
    data = _safe_api_get(url)
    if not isinstance(data, dict):
        return str(data)

    title = data.get("title", "Unknown")
    version = data.get("version", {})
    modified = version.get("when", "unknown")
    body_html = data.get("body", {}).get("storage", {}).get("value", "")

    body_text = re.sub(r"<[^>]+>", " ", body_html)
    body_text = re.sub(r"\s+", " ", body_text).strip()
    if len(body_text) > 12000:
        body_text = body_text[:12000] + "... (truncated)"

    return f"Title: {title}\nLast modified: {modified}\n\n{body_text}"


def search_github_prs(query: str) -> str:
    """Search GitHub for pull requests and commits related to an AIP.

    Call this with the AIP number (e.g. "AIP-76") and also with topic
    keywords (e.g. "asset partition") to find commits not tagged with
    the AIP number.

    Args:
        query: Search query (e.g. "AIP-76" or "asset partition PartitionMapper").

    Returns:
        A list of matching PRs and commits with titles, numbers, and status.
    """
    headers = _github_headers()
    encoded = urllib.parse.quote(f"{query} repo:{GITHUB_REPO}")

    pr_url = f"https://api.github.com/search/issues?q={encoded}+type:pr&per_page=15&sort=updated"
    time.sleep(GITHUB_API_DELAY)
    pr_data = _safe_api_get(pr_url, headers)

    lines = []
    if isinstance(pr_data, dict):
        for item in pr_data.get("items", [])[:15]:
            state = item.get("state", "unknown")
            merged = ""
            if item.get("pull_request", {}).get("merged_at"):
                merged = " (merged)"
            lines.append(f"PR #{item['number']}: {item['title']} [{state}{merged}]")

    commit_url = f"https://api.github.com/search/commits?q={encoded}&per_page=10&sort=committer-date"
    time.sleep(GITHUB_API_DELAY)
    commit_data = _safe_api_get(commit_url, headers)

    if isinstance(commit_data, dict):
        for item in commit_data.get("items", [])[:10]:
            sha = item.get("sha", "")[:7]
            msg = item.get("commit", {}).get("message", "").split("\n")[0]
            lines.append(f"Commit {sha}: {msg}")

    return "\n".join(lines) if lines else "No results found."


def get_repo_file_tree(path_prefix: str) -> str:
    """List files in the Apache Airflow repository under a given path.

    Args:
        path_prefix: Directory path to list (e.g. "providers/common/ai/src/airflow/providers/common/ai/operators").

    Returns:
        A list of file paths under the given prefix, with counts of source
        and test files.
    """
    headers = _github_headers()
    url = f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/main?recursive=1"
    time.sleep(GITHUB_API_DELAY)
    data = _safe_api_get(url, headers)

    if not isinstance(data, dict):
        return str(data)

    tree = data.get("tree", [])
    matching = [
        item["path"] for item in tree if item.get("type") == "blob" and item["path"].startswith(path_prefix)
    ]

    if not matching:
        return f"No files found under {path_prefix}"

    source_files = [f for f in matching if f.endswith(".py") and "/tests/" not in f]
    test_files = [f for f in matching if "/tests/" in f]

    lines = [f"Found {len(matching)} files under {path_prefix}:"]
    lines.append(f"  Source files: {len(source_files)}, Test files: {len(test_files)}")
    for f in matching[:20]:
        lines.append(f"  - {f}")
    if len(matching) > 20:
        lines.append(f"  ... and {len(matching) - 20} more")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Agent system prompt -- reinforces the skill's rules at the system level.
# ---------------------------------------------------------------------------

AGENT_SYSTEM_PROMPT = """\
You are an Airflow project analyst assessing AIP implementation progress.

You have access to an aip-tracker skill that provides detailed assessment \
methodology. Load it first and follow its instructions precisely.

HARD RULES (these override any conflicting tendency):
1. Gather evidence from ALL three sources (Confluence spec, GitHub search, \
file tree) for every AIP before writing any assessment.
2. Extract deliverables from the spec's own structure -- completion criteria, \
phase definitions, or enumerated components. Do NOT split a single spec \
item into multiple deliverables (e.g. individual files within one component). \
Do NOT merge multiple spec items into one. Match the spec's granularity.
3. Always express progress as "X/Y deliverables shipped". NEVER convert to \
percentages. Do not write "73%" or "72%" or any percentage.
4. Every deliverable must cite evidence from tool results. If no evidence \
exists, mark it "not_started" or "unclear" -- do not guess.
5. Do NOT invent blockers, risks, or characterizations beyond the evidence. \
Do NOT editorialize with phrases like "near completion", "Advanced maturity", \
or "substantially implemented". State the fraction and let the reader judge.
6. PR numbers must come from search_github_prs results. Never fabricate them.
7. Before returning the report, run the self-verification checklist in the \
skill. Fix any violations before returning."""


# [START example_aip_progress_tracker_skills]
@dag(
    tags=["example", "ai", "skills"],
    params={
        "aip_numbers": Param(
            default=DEFAULT_AIP_NUMBERS,
            type="string",
            description="Comma-separated AIP numbers to track",
        ),
    },
)
def example_aip_progress_tracker_skills():
    """Skills-based AIP tracker using AgentSkillsToolset.

    Same use case as ``example_aip_progress_tracker`` but solved with a
    single ``AgentOperator`` that loads the ``aip-tracker`` skill and
    decides its own evidence-gathering strategy.
    """
    from pydantic_ai.toolsets import FunctionToolset

    aip_toolset = FunctionToolset(
        tools=[fetch_confluence_page, search_github_prs, get_repo_file_tree],
    )

    aip_info = "\n".join(
        f"- AIP-{num}: {info['topic']} (page_id={info['page_id']}, "
        f"paths: {', '.join(info['codebase_paths'][:3])})"
        for num, info in AIP_REGISTRY.items()
    )

    prompt = (
        "Track the implementation progress of these AIPs and produce a "
        "cross-AIP progress report:\n\n"
        f"{aip_info}\n\n"
        "Use the aip-tracker skill for detailed instructions on how to "
        "gather evidence and structure your assessment."
    )

    # [START aip_tracker_skills_operator]
    report = AgentOperator(
        task_id="track_aip_progress",
        llm_conn_id=LLM_CONN_ID,
        system_prompt=AGENT_SYSTEM_PROMPT,
        prompt=prompt,
        toolsets=[
            AgentSkillsToolset(sources=[SKILLS_DIR]),
            aip_toolset,
        ],
        agent_params={"model_settings": {"temperature": 0}},
        usage_limits=UsageLimits(
            request_limit=30,
            input_tokens_limit=200_000,
            output_tokens_limit=16_000,
        ),
    )
    # [END aip_tracker_skills_operator]

    # [START aip_tracker_skills_hitl]
    ApprovalOperator(
        task_id="review_report",
        subject="Review AIP Progress Report (Skills-generated)",
        body=report.output,
        response_timeout=timedelta(hours=24),
    )
    # [END aip_tracker_skills_hitl]


# [END example_aip_progress_tracker_skills]

example_aip_progress_tracker_skills()

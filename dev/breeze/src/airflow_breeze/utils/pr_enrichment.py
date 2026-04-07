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
"""PR enrichment, filtering, and categorization pipeline for auto-triage."""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.pr_display import pr_link
from airflow_breeze.utils.pr_github import COLLABORATOR_ASSOCIATIONS, graphql_request, is_bot_account
from airflow_breeze.utils.pr_models import PRData
from airflow_breeze.utils.shared_options import get_verbose

if TYPE_CHECKING:
    from airflow_breeze.utils.github import PRAssessment

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

READY_FOR_REVIEW_LABEL = "ready for maintainer review"
TRIAGE_COMMENT_MARKER = "Pull Request quality criteria"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DeterministicResult:
    """Result of deterministic PR assessment."""
    category: str  # "flagged", "draft_skipped", "grace_period", "in_progress", "pending_approval", "llm_candidate"
    assessment: PRAssessment | None = None


@dataclass
class CategorizationResult:
    """Result of categorizing all candidate PRs."""
    assessments: dict[int, PRAssessment] = field(default_factory=dict)
    llm_candidates: list[PRData] = field(default_factory=list)
    pending_approval: list[PRData] = field(default_factory=list)
    workflows_in_progress: list[PRData] = field(default_factory=list)
    skipped_drafts: list[PRData] = field(default_factory=list)
    recent_failures_skipped: list[PRData] = field(default_factory=list)
    deterministic_timings: dict[int, float] = field(default_factory=dict)
    total_flagged: int = 0


# ---------------------------------------------------------------------------
# Label filtering
# ---------------------------------------------------------------------------

def split_label_filters(
    labels: tuple[str, ...], exclude_labels: tuple[str, ...]
) -> tuple[tuple[str, ...], list[str], tuple[str, ...], list[str]]:
    """Split labels into (exact_labels, wildcard_labels, exact_exclude, wildcard_exclude)."""
    exact_labels = tuple(lbl for lbl in labels if "*" not in lbl and "?" not in lbl)
    wildcard_labels = [lbl for lbl in labels if "*" in lbl or "?" in lbl]
    exact_exclude_labels = tuple(lbl for lbl in exclude_labels if "*" not in lbl and "?" not in lbl)
    wildcard_exclude_labels = [lbl for lbl in exclude_labels if "*" in lbl or "?" in lbl]
    return exact_labels, wildcard_labels, exact_exclude_labels, wildcard_exclude_labels


def apply_wildcard_label_filters(
    prs: list[PRData], wildcard_labels: list[str], wildcard_exclude_labels: list[str]
) -> list[PRData]:
    """Filter PRs by wildcard include/exclude label patterns."""
    from fnmatch import fnmatch
    if wildcard_labels:
        prs = [pr for pr in prs if any(fnmatch(lbl, pat) for pat in wildcard_labels for lbl in pr.labels)]
    if wildcard_exclude_labels:
        prs = [pr for pr in prs if not any(fnmatch(lbl, pat) for pat in wildcard_exclude_labels for lbl in pr.labels)]
    return prs


# ---------------------------------------------------------------------------
# Assessment merging
# ---------------------------------------------------------------------------

def merge_pr_assessments(*assessments) -> object | None:
    """Merge multiple optional PRAssessments into one combined result."""
    violations: list = []
    summaries: list[str] = []
    for a in assessments:
        if a:
            violations.extend(a.violations)
            summaries.append(a.summary)
    if not violations and not summaries:
        return None
    from airflow_breeze.utils.github import PRAssessment as _PRAssessment
    return _PRAssessment(should_flag=True, violations=violations, summary=" ".join(summaries))


# ---------------------------------------------------------------------------
# Deterministic assessment
# ---------------------------------------------------------------------------

def assess_pr_deterministic(
    pr: PRData, *, token: str, github_repository: str,
) -> DeterministicResult:
    """Run deterministic checks on a single PR and return its categorization."""
    from airflow_breeze.utils.github import (
        assess_pr_checks, assess_pr_conflicts, assess_pr_ui_demo,
        assess_pr_unresolved_comments,
    )
    from airflow_breeze.utils.pr_checks import are_failures_recent
    from airflow_breeze.utils.pr_workflows import has_in_progress_workflows

    ci_assessment = assess_pr_checks(pr.number, pr.checks_state, pr.failed_checks)
    if ci_assessment and pr.head_sha and has_in_progress_workflows(token, github_repository, pr.head_sha):
        return DeterministicResult(category="in_progress")
    if (
        ci_assessment and pr.head_sha
        and are_failures_recent(token, github_repository, pr.head_sha, pr.ci_grace_period_hours)
    ):
        ci_assessment = None
        conflict_check = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
        comments_check = assess_pr_unresolved_comments(pr.number, pr.unresolved_review_comments, pr.unresolved_threads)
        if not conflict_check and not comments_check:
            return DeterministicResult(category="grace_period")

    conflict_assessment = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
    comments_assessment = assess_pr_unresolved_comments(pr.number, pr.unresolved_review_comments, pr.unresolved_threads)
    ui_demo_assessment = assess_pr_ui_demo(pr.number, pr.labels, pr.body, pr.author_association)

    if ci_assessment or conflict_assessment or comments_assessment or ui_demo_assessment:
        if pr.is_draft:
            return DeterministicResult(category="draft_skipped")
        merged = merge_pr_assessments(conflict_assessment, ci_assessment, comments_assessment, ui_demo_assessment)
        return DeterministicResult(category="flagged", assessment=merged)

    if pr.checks_state == "NOT_RUN":
        if pr.head_sha and has_in_progress_workflows(token, github_repository, pr.head_sha):
            return DeterministicResult(category="in_progress")
        return DeterministicResult(category="pending_approval")
    if pr.is_draft and pr.head_sha and has_in_progress_workflows(token, github_repository, pr.head_sha):
        return DeterministicResult(category="in_progress")
    return DeterministicResult(category="llm_candidate")


def categorize_all_candidates(
    candidate_prs: list[PRData], *, token: str, github_repository: str,
    run_api: bool, progress_cb: Callable | None = None,
) -> CategorizationResult:
    """Iterate over candidates and sort them into deterministic buckets."""
    from airflow_breeze.utils.pr_workflows import has_in_progress_workflows
    result = CategorizationResult()
    if run_api:
        for pr_idx, pr in enumerate(candidate_prs):
            if progress_cb:
                progress_cb(pr_idx, len(candidate_prs))
            t_det_start = time.monotonic()
            det = assess_pr_deterministic(pr, token=token, github_repository=github_repository)
            result.deterministic_timings[pr.number] = time.monotonic() - t_det_start
            if det.category == "flagged" and det.assessment:
                result.total_flagged += 1
                result.assessments[pr.number] = det.assessment
            elif det.category == "draft_skipped":
                result.skipped_drafts.append(pr)
            elif det.category == "grace_period":
                result.recent_failures_skipped.append(pr)
            elif det.category == "in_progress":
                result.workflows_in_progress.append(pr)
            elif det.category == "pending_approval":
                result.pending_approval.append(pr)
            elif det.category == "llm_candidate":
                result.llm_candidates.append(pr)
    else:
        for pr in candidate_prs:
            if pr.checks_state == "NOT_RUN":
                if pr.head_sha and has_in_progress_workflows(token, github_repository, pr.head_sha):
                    result.workflows_in_progress.append(pr)
                else:
                    result.pending_approval.append(pr)
            elif pr.is_draft and pr.head_sha and has_in_progress_workflows(token, github_repository, pr.head_sha):
                result.workflows_in_progress.append(pr)
            else:
                result.llm_candidates.append(pr)
    return result


# ---------------------------------------------------------------------------
# PR filtering
# ---------------------------------------------------------------------------

def filter_candidate_prs(
    all_prs: list[PRData], *,
    author_filter=None, include_drafts: bool = True,
    checks_state: str = "any", min_commits_behind: int = 0,
    max_num: int = 0, also_accepted: set[int] | None = None,
    quiet: bool = False,
) -> tuple[list[PRData], list[PRData], int, int, int]:
    """Filter PRs to candidates. Returns (candidates, accepted, skipped_collab, skipped_bot, skipped_accepted)."""
    candidate_prs: list[PRData] = []
    accepted_prs: list[PRData] = []
    total_skipped_collaborator = 0
    total_skipped_bot = 0
    total_skipped_accepted = 0
    verbose = get_verbose()
    for pr in all_prs:
        if not include_drafts and pr.is_draft:
            continue
        elif author_filter and not author_filter.should_include(pr.author_association):
            total_skipped_collaborator += 1
        elif is_bot_account(pr.author_login):
            total_skipped_bot += 1
        elif READY_FOR_REVIEW_LABEL in pr.labels or (also_accepted and pr.number in also_accepted):
            total_skipped_accepted += 1
            accepted_prs.append(pr)
        elif checks_state != "any" and pr.checks_state not in ("NOT_RUN",) and pr.checks_state.lower() != checks_state:
            continue
        elif min_commits_behind > 0 and pr.commits_behind < min_commits_behind:
            continue
        else:
            candidate_prs.append(pr)
    if max_num and len(candidate_prs) > max_num:
        candidate_prs = candidate_prs[:max_num]
    return candidate_prs, accepted_prs, total_skipped_collaborator, total_skipped_bot, total_skipped_accepted


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def enrich_candidate_details(
    token: str, github_repository: str, candidate_prs: list[PRData], *,
    run_api: bool, quiet: bool = False,
    on_check_progress: Callable[[int, int], None] | None = None,
    on_merge_progress: Callable[[int, int], None] | None = None,
    on_review_progress: Callable[[int, int], None] | None = None,
) -> None:
    """Fetch check details, resolve unknown mergeable status, and fetch review comments."""
    from airflow_breeze.utils.pr_checks import fetch_check_details_batch, fetch_failed_checks
    from airflow_breeze.utils.pr_fetching import fetch_unresolved_comments_batch
    if not candidate_prs:
        return
    fetch_check_details_batch(token, github_repository, candidate_prs, on_progress=on_check_progress)
    for pr in candidate_prs:
        if pr.checks_state == "FAILURE" and not pr.failed_checks and pr.head_sha:
            pr.failed_checks = fetch_failed_checks(token, github_repository, pr.head_sha)
    if run_api:
        fetch_unresolved_comments_batch(token, github_repository, candidate_prs, on_progress=on_review_progress)


def reevaluate_triaged_pr(
    pr: PRData, *, token: str, github_repository: str, run_api: bool,
) -> DeterministicResult:
    """Re-evaluate a previously triaged PR: enrich and run deterministic checks."""
    enrich_candidate_details(token, github_repository, [pr], run_api=run_api, quiet=True)
    if run_api:
        return assess_pr_deterministic(pr, token=token, github_repository=github_repository)
    if pr.checks_state == "NOT_RUN":
        return DeterministicResult(category="pending_approval")
    return DeterministicResult(category="llm_candidate")


def load_labels_from_boring_cyborg() -> list[str]:
    """Read labels from .github/boring-cyborg.yml."""
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
    boring_cyborg_path = AIRFLOW_ROOT_PATH / ".github" / "boring-cyborg.yml"
    if not boring_cyborg_path.exists():
        return []
    import yaml
    with open(boring_cyborg_path) as f:
        data = yaml.safe_load(f)
    labels: list[str] = []
    label_pr_section = data.get("labelPRBasedOnFilePath", {})
    for label_name in label_pr_section:
        labels.append(label_name)
    return sorted(labels)

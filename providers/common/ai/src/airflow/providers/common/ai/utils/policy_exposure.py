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
"""Shared models and helpers for configured policy exposure snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field
from pydantic_ai.toolsets.wrapper import WrapperToolset

XCOM_POLICY_EXPOSURE = "airflow_common_ai_policy_exposure"

PolicyRiskLevel = Literal["low", "medium", "high"]
ResourceCategory = Literal[
    "database",
    "table",
    "schema",
    "datasource",
    "uri",
    "hook_method",
    "mcp_server",
    "tool_prefix",
    "unknown",
]
AccessMode = Literal["read", "write", "read_write", "unknown"]


class TaskIdentity(BaseModel):
    """Identifying information for the task instance that produced the report."""

    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1
    operator_type: str


class LLMExposure(BaseModel):
    """Configuration-derived LLM access surface."""

    llm_conn_id: str
    connection_type: str | None = None
    model_id: str | None = None


class ApprovalExposure(BaseModel):
    """Review and approval controls applied to the task."""

    enable_hitl_review: bool = False
    max_hitl_iterations: int | None = None


class ResourceExposure(BaseModel):
    """A single configured resource or capability exposed to the agent."""

    category: ResourceCategory
    name: str
    access_mode: AccessMode = "unknown"
    details: dict[str, Any] = Field(default_factory=dict)


class ToolsetExposure(BaseModel):
    """Exposure summary for one configured toolset."""

    toolset_type: str
    toolset_id: str | None = None
    summary: str
    resources: list[ResourceExposure] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)


class PolicyRiskSummary(BaseModel):
    """High-level risk summary for the configured exposure snapshot."""

    level: PolicyRiskLevel
    reasons: list[str] = Field(default_factory=list)


class PolicyExposureReport(BaseModel):
    """Configured policy exposure snapshot for an AI task instance."""

    captured_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    task: TaskIdentity
    llm: LLMExposure
    approval: ApprovalExposure
    toolsets: list[ToolsetExposure] = Field(default_factory=list)
    runtime_notes: list[str] = Field(default_factory=list)
    risk: PolicyRiskSummary


def classify_policy_risk(
    *, llm: LLMExposure, toolsets: list[ToolsetExposure], runtime_notes: list[str]
) -> PolicyRiskSummary:
    """Classify overall risk using deterministic rules based on configured access."""
    reasons: list[str] = []
    has_write_access = any(
        resource.access_mode in {"write", "read_write"}
        for toolset in toolsets
        for resource in toolset.resources
    )

    for toolset in toolsets:
        reasons.extend(toolset.risk_flags)

    if has_write_access and not any(
        "write-capable" in reason or "write access" in reason for reason in reasons
    ):
        reasons.append("write-capable tool access configured")

    deduped_reasons = _dedupe_reasons(reasons)

    if any(
        reason in deduped_reasons
        for reason in ("unknown toolset exposure", "potentially mutating hook methods exposed")
    ):
        return PolicyRiskSummary(level="high", reasons=deduped_reasons or ["unknown toolset exposure"])

    if has_write_access:
        return PolicyRiskSummary(level="high", reasons=deduped_reasons)

    if deduped_reasons:
        return PolicyRiskSummary(level="medium", reasons=deduped_reasons)

    if runtime_notes:
        return PolicyRiskSummary(level="low", reasons=["configured access includes runtime controls"])

    return PolicyRiskSummary(level="low", reasons=["no external tool access configured"])


def unwrap_toolset(toolset: Any) -> Any:
    """Unwrap known wrapper-style toolsets to the base policy surface."""
    current = toolset
    seen: set[int] = set()

    while isinstance(current, WrapperToolset) and current.wrapped is not None:
        current_id = id(current)
        if current_id in seen:
            break
        seen.add(current_id)
        current = current.wrapped

    return current


def describe_toolset_exposure(toolset: Any) -> ToolsetExposure:
    """Describe a toolset's configured exposure with a safe fallback for unknown toolsets."""
    base_toolset = unwrap_toolset(toolset)
    describe = getattr(base_toolset, "describe_policy_exposure", None)
    if callable(describe):
        try:
            exposure = describe()
            if isinstance(exposure, ToolsetExposure):
                return exposure
            return ToolsetExposure(
                toolset_type=type(base_toolset).__name__,
                toolset_id=_get_toolset_id(base_toolset),
                summary="Toolset returned an invalid policy exposure report.",
                risk_flags=["invalid toolset exposure report"],
            )
        except Exception:
            return ToolsetExposure(
                toolset_type=type(base_toolset).__name__,
                toolset_id=_get_toolset_id(base_toolset),
                summary="Toolset exposure details are unavailable because report generation failed.",
                risk_flags=["toolset exposure report failed"],
            )

    return ToolsetExposure(
        toolset_type=type(base_toolset).__name__,
        toolset_id=_get_toolset_id(base_toolset),
        summary="Unknown toolset type; configured exposure details are unavailable.",
        risk_flags=["unknown toolset exposure"],
    )


def _get_toolset_id(toolset: Any) -> str | None:
    toolset_id = getattr(toolset, "id", None)
    return toolset_id if isinstance(toolset_id, str) else None


def _dedupe_reasons(reasons: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for reason in reasons:
        if reason and reason not in seen:
            seen.add(reason)
            ordered.append(reason)
    return ordered

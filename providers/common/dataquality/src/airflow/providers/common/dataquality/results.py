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
"""Result records produced by a data quality check run — immutable facts, JSON-serializable."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

PASS = "pass"
WARN = "warn"
FAIL = "fail"
ERROR = "error"

# Weight of a warn-severity failure in the run score, relative to an error-severity failure.
WARN_SCORE_WEIGHT = 0.25


@dataclass(frozen=True)
class RuleResult:
    """Outcome of evaluating one rule in one run."""

    rule_uid: str
    rule_name: str
    status: str  # pass | warn | fail | error
    observed_value: float | str | None = None
    condition: dict[str, Any] = field(default_factory=dict)
    dimension: str = "validity"
    severity: str = "error"
    duration_ms: float | None = None
    error_message: str | None = None
    description: str | None = None
    sql: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RuleResult:
        return cls(**data)


@dataclass(frozen=True)
class DQRun:
    """One execution of a ruleset by one task instance."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1
    map_index: int = -1
    run_uid: str = field(default_factory=lambda: uuid.uuid4().hex)
    ruleset_name: str | None = None
    table_ref: str | None = None
    asset_names: tuple[str, ...] = ()
    started_at: str | None = None
    finished_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["asset_names"] = list(self.asset_names)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DQRun:
        data = {**data, "asset_names": tuple(data.get("asset_names", ()))}
        return cls(**data)


def compute_score(results: list[RuleResult]) -> float | None:
    """
    Weighted pass rate for a run in [0, 1].

    Error-severity failures (and execution errors) count fully against the score;
    warn-severity failures count at ``WARN_SCORE_WEIGHT``.
    """
    if not results:
        return None
    penalty = 0.0
    for result in results:
        if result.status in (FAIL, ERROR):
            penalty += 1.0
        elif result.status == WARN:
            penalty += WARN_SCORE_WEIGHT
    return round(1.0 - penalty / len(results), 4)


def build_summary(*, run: DQRun, results: list[RuleResult]) -> dict[str, Any]:
    """Compact run summary attached to XCom and outlet asset events."""
    return {
        "run_uid": run.run_uid,
        "ruleset": run.ruleset_name,
        "table": run.table_ref,
        "score": compute_score(results),
        "passed": sum(1 for r in results if r.status == PASS),
        "warned": sum(1 for r in results if r.status == WARN),
        "failed": sum(1 for r in results if r.status == FAIL),
        "errored": sum(1 for r in results if r.status == ERROR),
        "failed_rules": sorted(r.rule_name for r in results if r.status in (FAIL, ERROR)),
        "warned_rules": sorted(r.rule_name for r in results if r.status == WARN),
    }

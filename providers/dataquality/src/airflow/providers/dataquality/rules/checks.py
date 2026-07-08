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
"""Built-in check catalog: one place for each check's SQL expression and metadata."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class Dimension(str, Enum):
    """Data quality dimension assigned to a rule result."""

    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    FRESHNESS = "freshness"
    VOLUME = "volume"
    CONSISTENCY = "consistency"


@dataclass(frozen=True)
class CheckSpec:
    """
    Metadata for one built-in check.

    :param expression: SQL expression template. Column-level checks are rendered with
        ``{column}``; table-level checks (``requires_column=False``) ignore it.
    :param dimension: Default value for ``DQRule.dimension`` when a rule doesn't set it
        explicitly. ``DQRule.dimension`` is a settable field; this is only the fallback.
    :param requires_column: Whether a rule using this check must set ``column``.
    :param default_condition: Default ``DQRule.condition`` (as a dict) for a rule that doesn't
        set one explicitly; ``None`` means the rule must always specify a condition.
    """

    expression: str
    dimension: Dimension = Dimension.VALIDITY
    requires_column: bool = True
    default_condition: dict[str, Any] | None = None


CHECK_SPECS: dict[str, CheckSpec] = {
    "null_count": CheckSpec(
        expression="SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)",
        dimension=Dimension.COMPLETENESS,
    ),
    "null_ratio": CheckSpec(
        expression="SUM(CASE WHEN {column} IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*)",
        dimension=Dimension.COMPLETENESS,
    ),
    "distinct_count": CheckSpec(
        expression="COUNT(DISTINCT {column})",
        dimension=Dimension.UNIQUENESS,
    ),
    "unique_violations": CheckSpec(
        expression="COUNT({column}) - COUNT(DISTINCT {column})",
        dimension=Dimension.UNIQUENESS,
    ),
    "min": CheckSpec(expression="MIN({column})"),
    "max": CheckSpec(expression="MAX({column})"),
    "mean": CheckSpec(expression="AVG({column})"),
    "row_count": CheckSpec(
        expression="COUNT(*)",
        dimension=Dimension.VOLUME,
        requires_column=False,
    ),
}

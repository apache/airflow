#
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
from __future__ import annotations

import datetime

from airflow.configuration import conf
from airflow.sdk.definitions._internal.abstractoperator import (
    AbstractOperator as AbstractOperator,
    NotMapped as NotMapped,  # Re-export this for compat
    TaskStateChangeCallback as TaskStateChangeCallback,
)
from airflow.ti_deps.deps.mapped_task_upstream_dep import MappedTaskUpstreamDep
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep

DEFAULT_OWNER: str = conf.get_mandatory_value("operators", "default_owner")
DEFAULT_QUEUE: str = conf.get_mandatory_value("operators", "default_queue")

DEFAULT_TASK_EXECUTION_TIMEOUT: datetime.timedelta | None = conf.gettimedelta(
    "core", "default_task_execution_timeout"
)

DEFAULT_OPERATOR_DEPS = {
    NotInRetryPeriodDep(),
    PrevDagrunDep(),
    TriggerRuleDep(),
    NotPreviouslySkippedDep(),
    MappedTaskUpstreamDep(),
}
"""
Dependencies for the operator.

These differ from execution context dependencies in that they are specific to
tasks and can be extended/overridden by subclasses.
"""

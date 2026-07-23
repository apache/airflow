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

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import SkipMixin
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS, BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context
    from airflow.sdk.definitions._internal.node import DAGNode


class ManualGateOperator(BaseOperator, SkipMixin):
    """
    Skip an optional subsection by default so it can be run manually later.

    A manual gate marks downstream work as optional for normal Dag runs. When the gate runs, it succeeds
    and skips downstream tasks according to ``ignore_downstream_trigger_rules``. This keeps the Dag run from
    waiting on optional work while preserving the graph structure for future manual execution tooling.

    :param label: Human-readable label for UI/API surfaces. Defaults to ``task_display_name`` or
        ``task_id`` when not provided.
    :param ignore_downstream_trigger_rules: If set to True, all downstream tasks from this operator task
        will be skipped. This is the default behavior. If set to False, only direct downstream tasks are
        skipped and trigger rules on later descendants are respected.
    """

    inherits_from_skipmixin = True
    ui_color = "#e2e8f0"
    ui_fgcolor = "#1f2937"

    def __init__(
        self,
        *,
        label: str | None = None,
        ignore_downstream_trigger_rules: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        object.__setattr__(self, "_manual_gate_text", label)
        self.ignore_downstream_trigger_rules = ignore_downstream_trigger_rules

    @property
    def manual_gate_label(self) -> str:
        """Return the label future UI/API manual-run surfaces should display."""
        return self._manual_gate_text or self.task_display_name or self.task_id

    @classmethod
    def get_serialized_fields(cls) -> frozenset[str]:
        return super().get_serialized_fields() | {"ignore_downstream_trigger_rules", "_manual_gate_text"}

    def _get_tasks_to_skip(self, context: Context) -> Iterable[DAGNode]:
        if self.ignore_downstream_trigger_rules:
            tasks = context["task"].get_flat_relatives(upstream=False)
        else:
            tasks = context["task"].get_direct_relatives(upstream=False)

        yield from (task for task in tasks if not task.is_teardown)

    def execute(self, context: Context) -> dict[str, str | list[str]]:
        if not self.downstream_task_ids:
            self.log.info("No downstream tasks; nothing to skip.")
            return {"label": self.manual_gate_label, "skipped_task_ids": []}

        tasks_to_skip = list(self._get_tasks_to_skip(context))

        if self.log.getEffectiveLevel() <= logging.DEBUG:
            self.log.debug("Manual gate downstream task IDs %s", tasks_to_skip)

        self.log.info("Skipping manual gate downstream tasks.")
        if AIRFLOW_V_3_0_PLUS:
            self.skip(ti=context["ti"], tasks=tasks_to_skip)
        else:
            dag_run = context["dag_run"]
            self.skip(
                dag_run=dag_run,
                tasks=tasks_to_skip,
                execution_date=dag_run.logical_date,
                map_index=context["ti"].map_index,
            )

        return {
            "label": self.manual_gate_label,
            "skipped_task_ids": sorted(task.task_id for task in tasks_to_skip),
        }

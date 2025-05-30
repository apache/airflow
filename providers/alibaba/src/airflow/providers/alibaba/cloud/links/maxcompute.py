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

from typing import TYPE_CHECKING

from airflow.providers.alibaba.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import BaseOperatorLink
    from airflow.sdk.execution_time.xcom import XCom
else:
    from airflow.models.baseoperatorlink import BaseOperatorLink  # type: ignore[no-redef]
    from airflow.models.xcom import XCom  # type: ignore[no-redef]


class MaxComputeLogViewLink(BaseOperatorLink):
    """Helper class for constructing MaxCompute Log View Link."""

    name = "MaxCompute Log View"
    key = "maxcompute_log_view"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        url = XCom.get_value(key=self.key, ti_key=ti_key)
        if not url:
            return ""

        return url

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        log_view_url: str,
    ):
        """
        Persist the log view URL to XCom for later retrieval.

        :param context: The context of the task instance.
        :param task_instance: The task instance.
        :param log_view_url: The log view URL to persist.
        """
        task_instance.xcom_push(
            context,
            key=MaxComputeLogViewLink.key,
            value=log_view_url,
        )

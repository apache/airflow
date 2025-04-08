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

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context

from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import BaseOperatorLink
    from airflow.sdk.execution_time.xcom import XCom
else:
    from airflow.models import XCom  # type: ignore[no-redef]
    from airflow.models.baseoperatorlink import BaseOperatorLink  # type: ignore[no-redef]

XCOM_WEBLINK_KEY = "web_link"


class YQLink(BaseOperatorLink):
    """Web link to query in Yandex Query UI."""

    name = "Yandex Query"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        return XCom.get_value(key=XCOM_WEBLINK_KEY, ti_key=ti_key) or "https://yq.cloud.yandex.ru"

    @staticmethod
    def persist(context: Context, task_instance: BaseOperator, web_link: str) -> None:
        task_instance.xcom_push(context, key=XCOM_WEBLINK_KEY, value=web_link)

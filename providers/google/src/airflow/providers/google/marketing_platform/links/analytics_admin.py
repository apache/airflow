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

from typing import TYPE_CHECKING, ClassVar

from airflow.providers.common.compat.sdk import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.google.version_compat import BaseOperator


BASE_LINK = "https://analytics.google.com/analytics/web/"


class GoogleAnalyticsBaseLink(BaseOperatorLink):
    """
    Base class for Google Analytics links.

    :meta private:
    """

    name: ClassVar[str]
    key: ClassVar[str]
    format_str: ClassVar[str]

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        if conf := XCom.get_value(key=self.key, ti_key=ti_key):
            res = BASE_LINK + "#/" + self.format_str.format(**conf)
            return res
        return ""


class GoogleAnalyticsPropertyLink(GoogleAnalyticsBaseLink):
    """Helper class for constructing Google Analytics Property Link."""

    name = "Data Analytics Property"
    key = "data_analytics_property"
    format_str = "p{property_id}/"

    @staticmethod
    def persist(
        context: Context,
        property_id: str,
    ):
        context["task_instance"].xcom_push(
            key=GoogleAnalyticsPropertyLink.key,
            value={"property_id": property_id},
        )

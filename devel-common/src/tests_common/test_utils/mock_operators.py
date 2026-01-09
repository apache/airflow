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

from collections.abc import Sequence
from typing import TYPE_CHECKING

import attr

from tests_common.test_utils.compat import BaseOperatorLink
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

try:
    from airflow.models.xcom import XComModel as XCom
    from airflow.sdk import BaseOperator
except ImportError:
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]
    from airflow.models.xcom import XCom  # type: ignore[no-redef]


class MockOperator(BaseOperator):
    """Operator for testing purposes."""

    template_fields: Sequence[str] = ("arg1", "arg2")

    def __init__(self, arg1: str = "", arg2: str = "", **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2

    def execute(self, context: Context):
        pass


class AirflowLink(BaseOperatorLink):
    """Operator Link for Apache Airflow Website."""

    name = "airflow"

    def get_link(self, operator, *, ti_key):
        return "https://airflow.apache.org"


class EmptyExtraLinkTestOperator(BaseOperator):
    """
    Empty test operator with extra link.

    Example of an Operator that has an extra operator link
    and will be overridden by the one defined in tests/plugins/test_plugin.py.
    """

    operator_extra_links = (AirflowLink(),)


class EmptyNoExtraLinkTestOperator(BaseOperator):
    """
    Empty test operator without extra operator link.

    Example of an operator that has no extra Operator link.
    An operator link would be added to this operator via Airflow plugin.
    """

    operator_extra_links = ()


@attr.s(auto_attribs=True)
class CustomBaseIndexOpLink(BaseOperatorLink):
    """Custom Operator Link for Google BigQuery Console."""

    index: int = attr.ib()

    @property
    def name(self) -> str:
        return f"BigQuery Console #{self.index + 1}"

    @property
    def xcom_key(self) -> str:
        return f"bigquery_{self.index + 1}"

    def get_link(self, operator, *, ti_key):
        if AIRFLOW_V_3_0_PLUS:
            search_queries = XCom.get_many(
                task_id=ti_key.task_id, dag_id=ti_key.dag_id, run_id=ti_key.run_id, key="search_query"
            ).first()

            search_queries = XCom.deserialize_value(search_queries)
        else:
            search_queries = XCom.get_one(
                task_id=ti_key.task_id, dag_id=ti_key.dag_id, run_id=ti_key.run_id, key="search_query"
            )

        if not search_queries:
            return None
        if len(search_queries) < self.index:
            return None
        search_query = search_queries[self.index]
        return f"https://console.cloud.google.com/bigquery?j={search_query}"


class CustomOpLink(BaseOperatorLink):
    """Custom Operator with Link for Google Custom Search."""

    name = "Google Custom"

    def get_link(self, operator, *, ti_key):
        if AIRFLOW_V_3_0_PLUS:
            search_query = XCom.get_many(
                task_ids=ti_key.task_id,
                dag_ids=ti_key.dag_id,
                run_id=ti_key.run_id,
                map_indexes=ti_key.map_index,
                key="search_query",
            ).first()
            search_query = XCom.deserialize_value(search_query)
        else:
            search_query = XCom.get_one(
                task_id=ti_key.task_id,
                dag_id=ti_key.dag_id,
                run_id=ti_key.run_id,
                map_index=ti_key.map_index,
                key="search_query",
            )
        if not search_query:
            return None
        return f"http://google.com/custom_base_link?search={search_query}"


class CustomOperator(BaseOperator):
    """Custom Operator for testing purposes."""

    template_fields = ["bash_command"]
    custom_operator_name = "@custom"

    @property
    def operator_extra_links(self):
        """Return operator extra links."""
        # For mapped operators
        if not hasattr(self, "bash_command"):
            # For mapped operators, we return CustomOpLink since each mapped instance
            # will get its own link during runtime
            return (CustomOpLink(),)
        # For non-mapped operators
        if isinstance(self.bash_command, str) or self.bash_command is None:
            return (CustomOpLink(),)
        # For operators with multiple commands
        return (CustomBaseIndexOpLink(i) for i, _ in enumerate(self.bash_command))

    def __init__(self, bash_command=None, **kwargs):
        super().__init__(**kwargs)
        self.bash_command = bash_command

    def execute(self, context: Context):
        self.log.info("Hello World!")
        context["task_instance"].xcom_push(key="search_query", value="dummy_value")


class GoogleLink(BaseOperatorLink):
    """Operator Link for Apache Airflow Website for Google."""

    name = "google"
    operators = [EmptyNoExtraLinkTestOperator, CustomOperator]

    def get_link(self, operator, *, ti_key):
        return "https://www.google.com"


class AirflowLink2(BaseOperatorLink):
    """Operator Link for Apache Airflow Website for 1.10.5."""

    name = "airflow"
    operators = [EmptyExtraLinkTestOperator, EmptyNoExtraLinkTestOperator]

    def get_link(self, operator, *, ti_key):
        return "https://airflow.apache.org/1.10.5/"


class GithubLink(BaseOperatorLink):
    """Operator Link for Apache Airflow GitHub."""

    name = "github"

    def get_link(self, operator, *, ti_key):
        return "https://github.com/apache/airflow"

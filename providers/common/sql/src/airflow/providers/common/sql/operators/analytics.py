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

import json
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
    Context,
)

if TYPE_CHECKING:
    from airflow.providers.common.sql.config import DataSourceConfig
    from airflow.providers.common.sql.datafusion.engine import DataFusionEngine


class AnalyticsOperator(BaseOperator):
    """
    Operator to run queries on various datasource's stored in object stores like S3, GCS, Azure, etc.

    :param datasource_configs: List of datasource configurations to register.
    :param queries: List of SQL queries to execute.
    :param max_rows_check: Maximum number of rows returned per query. Queries exceeding this return
        only the first N rows with a warning.
    :param engine: Optional DataFusion engine instance.
    :param result_output_format: List of output formats for results. Supported: 'tabulate', 'json'. Default is 'tabulate'.
    """

    template_fields: Sequence[str] = (
        "datasource_configs",
        "queries",
        "max_rows_check",
        "result_output_format",
    )

    def __init__(
        self,
        datasource_configs: list[DataSourceConfig],
        queries: list[str],
        max_rows_check: int = 100,
        engine: DataFusionEngine | None = None,
        result_output_format: Literal["tabulate", "json"] = "tabulate",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.datasource_configs = datasource_configs
        self.queries = queries
        self.engine = engine
        self.max_rows_check = max_rows_check
        self.result_output_format = result_output_format

    @cached_property
    def _df_engine(self) -> DataFusionEngine:
        if self.engine is None:
            try:
                from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
            except ModuleNotFoundError as e:
                if e.name == "datafusion":
                    raise AirflowOptionalProviderFeatureException(
                        "Failed to import DataFusion. To use the AnalyticsOperator, please install the "
                        "`apache-airflow-providers-common-sql[datafusion]` extra."
                    ) from e
                raise
            return DataFusionEngine()
        return self.engine

    def execute(self, context: Context) -> str:

        results = []
        for datasource_config in self.datasource_configs:
            self._df_engine.register_datasource(datasource_config)

        # TODO make it parallel as there is no dependency between queries
        for query in self.queries:
            result_dict = self._df_engine.execute_query(query, max_rows=self.max_rows_check)
            results.append({"query": query, "data": result_dict})

        match self.result_output_format:
            case "tabulate":
                return self._build_tabulate_output(results)
            case "json":
                return self._build_json_output(results)
            case _:
                raise ValueError(f"Unsupported output format: {self.result_output_format}")

    def _build_tabulate_output(self, query_results: list[dict[str, Any]]) -> str:
        from tabulate import tabulate

        output_parts = []
        for item in query_results:
            query = item["query"]
            result_dict = item["data"]
            row_count = len(next(iter(result_dict.values()))) if result_dict else 0

            table_str = tabulate(
                self._get_rows(result_dict, row_count),
                headers="keys",
                tablefmt="github",
                showindex=True,
            )
            output_parts.append(f"\n### Results: {query}\n\n{table_str}\n\n{'-' * 40}\n")

        return "".join(output_parts)

    @staticmethod
    def _get_rows(result_dict: dict[str, Any], row_count: int) -> list[dict[str, Any]]:
        return [{key: result_dict[key][i] for key in result_dict} for i in range(row_count)]

    def _build_json_output(self, query_results: list[dict[str, Any]]) -> str:
        json_results = []

        for item in query_results:
            query = item["query"]
            result_dict = item["data"]
            row_count = len(next(iter(result_dict.values()))) if result_dict else 0

            json_results.append(
                {
                    "query": query,
                    "data": self._get_rows(result_dict, row_count),
                }
            )

        return json.dumps(json_results, default=str)

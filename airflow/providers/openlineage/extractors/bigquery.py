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

import traceback
from contextlib import suppress

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.utils.module_loading import import_string
from openlineage.client.facet import SqlJobFacet
from openlineage.common.provider.bigquery import BigQueryDatasetsProvider, BigQueryErrorRunFacet

_BIGQUERY_CONN_URL = "bigquery"


class BigQueryExtractor(BaseExtractor):
    """BigQuery extractor"""

    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["BigQueryOperator", "BigQueryExecuteQueryOperator"]

    def extract(self) -> OperatorLineage | None:
        return None

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        self.log.debug("extract_on_complete(%s)", str(task_instance))

        try:
            bigquery_job_id = self._get_xcom_bigquery_job_id(task_instance)
            if bigquery_job_id is None:
                raise Exception("Xcom could not resolve BigQuery job id. Job may have failed.")
        except Exception as e:
            self.log.exception("Cannot retrieve job details from BigQuery.Client.", exc_info=True)
            return OperatorLineage(
                run_facets={
                    "bigQuery_error": BigQueryErrorRunFacet(
                        clientError=f"{e}: {traceback.format_exc()}",
                    )
                }
            )

        client = self._get_client()

        stats = BigQueryDatasetsProvider(client=client).get_facets(bigquery_job_id)
        inputs = stats.inputs
        output = stats.output

        for ds in inputs:
            ds.input_facets = self._get_input_facets()

        run_facets = stats.run_facets
        job_facets = {"sql": SqlJobFacet(self.operator.sql)}

        return OperatorLineage(
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[output.to_openlineage_dataset()] if output else [],
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def _get_client(self):
        # lazy-load the bigquery Client due to its slow import
        from google.cloud.bigquery import Client

        # Get client using Airflow hook - this way we use the same credentials as Airflow
        if hasattr(self.operator, "hook") and self.operator.hook:
            hook = self.operator.hook
            return hook.get_client(project_id=hook.project_id, location=hook.location)
        with suppress(ImportError):
            BigQueryHook = import_string("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")

        if BigQueryHook is not None:
            hook = BigQueryHook(
                gcp_conn_id=self.operator.gcp_conn_id,
                use_legacy_sql=self.operator.use_legacy_sql,
                delegate_to=self.operator.delegate_to,
                location=self.operator.location,
                impersonation_chain=self.operator.impersonation_chain,
            )
            return hook.get_client(project_id=hook.project_id, location=hook.location)
        return Client()

    def _get_xcom_bigquery_job_id(self, task_instance):
        bigquery_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key="job_id")

        self.log.debug("bigquery_job_id: %s", str(bigquery_job_id))
        return bigquery_job_id

    def _get_input_facets(self):
        return {}

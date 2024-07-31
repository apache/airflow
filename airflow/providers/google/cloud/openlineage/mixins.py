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

import copy
import json
import traceback
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from airflow.providers.common.compat.openlineage.facet import (
        Dataset,
        InputDataset,
        OutputDataset,
        OutputStatisticsOutputDatasetFacet,
        RunFacet,
        SchemaDatasetFacet,
    )
    from airflow.providers.google.cloud.openlineage.utils import BigQueryJobRunFacet


BIGQUERY_NAMESPACE = "bigquery"


class _BigQueryOpenLineageMixin:
    def get_openlineage_facets_on_complete(self, _):
        """
        Retrieve OpenLineage data for a COMPLETE BigQuery job.

        This method retrieves statistics for the specified job_ids using the BigQueryDatasetsProvider.
        It calls BigQuery API, retrieving input and output dataset info from it, as well as run-level
        usage statistics.

        Run facets should contain:
            - ExternalQueryRunFacet
            - BigQueryJobRunFacet

        Run facets may contain:
            - ErrorMessageRunFacet

        Job facets should contain:
            - SqlJobFacet if operator has self.sql

        Input datasets should contain facets:
            - DataSourceDatasetFacet
            - SchemaDatasetFacet

        Output datasets should contain facets:
            - DataSourceDatasetFacet
            - SchemaDatasetFacet
            - OutputStatisticsOutputDatasetFacet
        """
        from airflow.providers.common.compat.openlineage.facet import ExternalQueryRunFacet, SQLJobFacet
        from airflow.providers.openlineage.extractors import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        if not self.job_id:
            if hasattr(self, "log"):
                self.log.warning("No BigQuery job_id was found by OpenLineage.")
            return OperatorLineage()

        if not self.hook:
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )

        run_facets: dict[str, RunFacet] = {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=self.job_id, source="bigquery")
        }

        job_facets = {"sql": SQLJobFacet(query=SQLParser.normalize_sql(self.sql))}

        self.client = self.hook.get_client(project_id=self.hook.project_id)
        job_ids = self.job_id
        if isinstance(self.job_id, str):
            job_ids = [self.job_id]
        inputs, outputs = [], []
        for job_id in job_ids:
            inner_inputs, inner_outputs, inner_run_facets = self.get_facets(job_id=job_id)
            inputs.extend(inner_inputs)
            outputs.extend(inner_outputs)
            run_facets.update(inner_run_facets)

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def get_facets(self, job_id: str):
        from airflow.providers.common.compat.openlineage.facet import ErrorMessageRunFacet
        from airflow.providers.google.cloud.openlineage.utils import (
            BigQueryErrorRunFacet,
            get_from_nullable_chain,
        )

        inputs = []
        outputs = []
        run_facets: dict[str, RunFacet] = {}
        if hasattr(self, "log"):
            self.log.debug("Extracting data from bigquery job: `%s`", job_id)
        try:
            job = self.client.get_job(job_id=job_id)  # type: ignore
            props = job._properties

            if get_from_nullable_chain(props, ["status", "state"]) != "DONE":
                raise ValueError(f"Trying to extract data from running bigquery job: `{job_id}`")

            # TODO: remove bigQuery_job in next release
            run_facets["bigQuery_job"] = run_facets["bigQueryJob"] = self._get_bigquery_job_run_facet(props)

            if get_from_nullable_chain(props, ["statistics", "numChildJobs"]):
                if hasattr(self, "log"):
                    self.log.debug("Found SCRIPT job. Extracting lineage from child jobs instead.")
                # SCRIPT job type has no input / output information but spawns child jobs that have one
                # https://cloud.google.com/bigquery/docs/information-schema-jobs#multi-statement_query_job
                for child_job_id in self.client.list_jobs(parent_job=job_id):
                    child_job = self.client.get_job(job_id=child_job_id)  # type: ignore
                    child_inputs, child_output = self._get_inputs_outputs_from_job(child_job._properties)
                    inputs.extend(child_inputs)
                    outputs.append(child_output)
            else:
                inputs, _output = self._get_inputs_outputs_from_job(props)
                outputs.append(_output)
        except Exception as e:
            if hasattr(self, "log"):
                self.log.warning("Cannot retrieve job details from BigQuery.Client. %s", e, exc_info=True)
            exception_msg = traceback.format_exc()
            # TODO: remove BigQueryErrorRunFacet in next release
            run_facets.update(
                {
                    "errorMessage": ErrorMessageRunFacet(
                        message=f"{e}: {exception_msg}",
                        programmingLanguage="python",
                    ),
                    "bigQuery_error": BigQueryErrorRunFacet(
                        clientError=f"{e}: {exception_msg}",
                    ),
                }
            )
        deduplicated_outputs = self._deduplicate_outputs(outputs)
        return inputs, deduplicated_outputs, run_facets

    def _deduplicate_outputs(self, outputs: list[OutputDataset | None]) -> list[OutputDataset]:
        # Sources are the same so we can compare only names
        final_outputs = {}
        for single_output in outputs:
            if not single_output:
                continue
            key = single_output.name
            if key not in final_outputs:
                final_outputs[key] = single_output
                continue

            # No OutputStatisticsOutputDatasetFacet is added to duplicated outputs as we can not determine
            # if the rowCount or size can be summed together.
            if single_output.outputFacets:
                single_output.outputFacets.pop("outputStatistics", None)
            final_outputs[key] = single_output

        return list(final_outputs.values())

    def _get_inputs_outputs_from_job(
        self, properties: dict
    ) -> tuple[list[InputDataset], OutputDataset | None]:
        from airflow.providers.google.cloud.openlineage.utils import get_from_nullable_chain

        input_tables = get_from_nullable_chain(properties, ["statistics", "query", "referencedTables"]) or []
        output_table = get_from_nullable_chain(properties, ["configuration", "query", "destinationTable"])
        inputs = [(self._get_input_dataset(input_table)) for input_table in input_tables]
        if output_table:
            output = self._get_output_dataset(output_table)
            dataset_stat_facet = self._get_statistics_dataset_facet(properties)
            output.outputFacets = output.outputFacets or {}
            if dataset_stat_facet:
                output.outputFacets["outputStatistics"] = dataset_stat_facet

        return inputs, output

    @staticmethod
    def _get_bigquery_job_run_facet(properties: dict) -> BigQueryJobRunFacet:
        from airflow.providers.google.cloud.openlineage.utils import (
            BigQueryJobRunFacet,
            get_from_nullable_chain,
        )

        if get_from_nullable_chain(properties, ["configuration", "query", "query"]):
            # Exclude the query to avoid event size issues and duplicating SqlJobFacet information.
            properties = copy.deepcopy(properties)
            properties["configuration"]["query"].pop("query")
        cache_hit = get_from_nullable_chain(properties, ["statistics", "query", "cacheHit"])
        billed_bytes = get_from_nullable_chain(properties, ["statistics", "query", "totalBytesBilled"])
        return BigQueryJobRunFacet(
            cached=str(cache_hit).lower() == "true",
            billedBytes=int(billed_bytes) if billed_bytes else None,
            properties=json.dumps(properties),
        )

    @staticmethod
    def _get_statistics_dataset_facet(
        properties,
    ) -> OutputStatisticsOutputDatasetFacet | None:
        from airflow.providers.common.compat.openlineage.facet import OutputStatisticsOutputDatasetFacet
        from airflow.providers.google.cloud.openlineage.utils import get_from_nullable_chain

        query_plan = get_from_nullable_chain(properties, chain=["statistics", "query", "queryPlan"])
        if not query_plan:
            return None

        out_stage = query_plan[-1]
        out_rows = out_stage.get("recordsWritten", None)
        out_bytes = out_stage.get("shuffleOutputBytes", None)
        if out_bytes and out_rows:
            return OutputStatisticsOutputDatasetFacet(rowCount=int(out_rows), size=int(out_bytes))
        return None

    def _get_input_dataset(self, table: dict) -> InputDataset:
        from airflow.providers.common.compat.openlineage.facet import InputDataset

        return cast(InputDataset, self._get_dataset(table, "input"))

    def _get_output_dataset(self, table: dict) -> OutputDataset:
        from airflow.providers.common.compat.openlineage.facet import OutputDataset

        return cast(OutputDataset, self._get_dataset(table, "output"))

    def _get_dataset(self, table: dict, dataset_type: str) -> Dataset:
        from airflow.providers.common.compat.openlineage.facet import InputDataset, OutputDataset

        project = table.get("projectId")
        dataset = table.get("datasetId")
        table_name = table.get("tableId")
        dataset_name = f"{project}.{dataset}.{table_name}"

        dataset_schema = self._get_table_schema_safely(dataset_name)
        if dataset_type == "input":
            # Logic specific to creating InputDataset (if needed)
            return InputDataset(
                namespace=BIGQUERY_NAMESPACE,
                name=dataset_name,
                facets={
                    "schema": dataset_schema,
                }
                if dataset_schema
                else {},
            )
        elif dataset_type == "output":
            # Logic specific to creating OutputDataset (if needed)
            return OutputDataset(
                namespace=BIGQUERY_NAMESPACE,
                name=dataset_name,
                facets={
                    "schema": dataset_schema,
                }
                if dataset_schema
                else {},
            )
        else:
            raise ValueError("Invalid dataset_type. Must be 'input' or 'output'")

    def _get_table_schema_safely(self, table_name: str) -> SchemaDatasetFacet | None:
        try:
            return self._get_table_schema(table_name)
        except Exception as e:
            if hasattr(self, "log"):
                self.log.warning("Could not extract output schema from bigquery. %s", e)
        return None

    def _get_table_schema(self, table: str) -> SchemaDatasetFacet | None:
        from airflow.providers.common.compat.openlineage.facet import (
            SchemaDatasetFacet,
            SchemaDatasetFacetFields,
        )
        from airflow.providers.google.cloud.openlineage.utils import get_from_nullable_chain

        bq_table = self.client.get_table(table)

        if not bq_table._properties:
            return None

        fields = get_from_nullable_chain(bq_table._properties, ["schema", "fields"])
        if not fields:
            return None

        return SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(
                    name=field.get("name"),
                    type=field.get("type"),
                    description=field.get("description"),
                )
                for field in fields
            ]
        )

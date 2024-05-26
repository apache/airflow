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
from typing import TYPE_CHECKING, Any

from attr import define, field
from openlineage.client.facet import (
    BaseFacet,
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    DocumentationDatasetFacet,
    ErrorMessageRunFacet,
    OutputStatisticsOutputDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.run import Dataset

from airflow.providers.google import __version__ as provider_version

if TYPE_CHECKING:
    from google.cloud.bigquery.table import Table


BIGQUERY_NAMESPACE = "bigquery"
BIGQUERY_URI = "bigquery"


def get_facets_from_bq_table(table: Table) -> dict[Any, Any]:
    """Get facets from BigQuery table object."""
    facets = {
        "schema": SchemaDatasetFacet(
            fields=[
                SchemaField(name=field.name, type=field.field_type, description=field.description)
                for field in table.schema
            ]
        ),
        "documentation": DocumentationDatasetFacet(description=table.description or ""),
    }

    return facets


def get_identity_column_lineage_facet(
    field_names: list[str],
    input_datasets: list[Dataset],
) -> ColumnLineageDatasetFacet:
    """
    Get column lineage facet.

    Simple lineage will be created, where each source column corresponds to single destination column
    in each input dataset and there are no transformations made.
    """
    if field_names and not input_datasets:
        raise ValueError("When providing `field_names` You must provide at least one `input_dataset`.")

    column_lineage_facet = ColumnLineageDatasetFacet(
        fields={
            field: ColumnLineageDatasetFacetFieldsAdditional(
                inputFields=[
                    ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                        namespace=dataset.namespace, name=dataset.name, field=field
                    )
                    for dataset in input_datasets
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            )
            for field in field_names
        }
    )
    return column_lineage_facet


@define
class BigQueryJobRunFacet(BaseFacet):
    """Facet that represents relevant statistics of bigquery run.

    This facet is used to provide statistics about bigquery run.

    :param cached: BigQuery caches query results. Rest of the statistics will not be provided for cached queries.
    :param billedBytes: How many bytes BigQuery bills for.
    :param properties: Full property tree of BigQUery run.
    """

    cached: bool
    billedBytes: int | None = field(default=None)
    properties: str | None = field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://raw.githubusercontent.com/apache/airflow/"
            f"providers-google/{provider_version}/airflow/providers/google/"
            "openlineage/BigQueryJobRunFacet.json"
        )


# TODO: remove BigQueryErrorRunFacet in next release
@define
class BigQueryErrorRunFacet(BaseFacet):
    """
    Represents errors that can happen during execution of BigqueryExtractor.

    :param clientError: represents errors originating in bigquery client
    :param parserError: represents errors that happened during parsing SQL provided to bigquery
    """

    clientError: str | None = field(default=None)
    parserError: str | None = field(default=None)

    @staticmethod
    def _get_schema() -> str:
        return (
            "https://raw.githubusercontent.com/apache/airflow/"
            f"providers-google/{provider_version}/airflow/providers/google/"
            "openlineage/BigQueryErrorRunFacet.json"
        )


def get_from_nullable_chain(source: Any, chain: list[str]) -> Any | None:
    """Get object from nested structure of objects, where it's not guaranteed that all keys in the nested structure exist.

    Intended to replace chain of `dict.get()` statements.

    Example usage:

    .. code-block:: python

        if (
            not job._properties.get("statistics")
            or not job._properties.get("statistics").get("query")
            or not job._properties.get("statistics").get("query").get("referencedTables")
        ):
            return None
        result = job._properties.get("statistics").get("query").get("referencedTables")

    becomes:

    .. code-block:: python

        result = get_from_nullable_chain(properties, ["statistics", "query", "queryPlan"])
        if not result:
            return None
    """
    chain.reverse()
    try:
        while chain:
            next_key = chain.pop()
            if isinstance(source, dict):
                source = source.get(next_key)
            else:
                source = getattr(source, next_key)
        return source
    except AttributeError:
        return None


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
        from openlineage.client.facet import ExternalQueryRunFacet, SqlJobFacet

        from airflow.providers.openlineage.extractors import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        if not self.job_id:
            return OperatorLineage()

        run_facets: dict[str, BaseFacet] = {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=self.job_id, source="bigquery")
        }

        job_facets = {"sql": SqlJobFacet(query=SQLParser.normalize_sql(self.sql))}

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
        inputs = []
        outputs = []
        run_facets: dict[str, BaseFacet] = {}
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

    def _deduplicate_outputs(self, outputs: list[Dataset | None]) -> list[Dataset]:
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
            single_output.facets.pop("outputStatistics", None)
            final_outputs[key] = single_output

        return list(final_outputs.values())

    def _get_inputs_outputs_from_job(self, properties: dict) -> tuple[list[Dataset], Dataset | None]:
        input_tables = get_from_nullable_chain(properties, ["statistics", "query", "referencedTables"]) or []
        output_table = get_from_nullable_chain(properties, ["configuration", "query", "destinationTable"])
        inputs = [self._get_dataset(input_table) for input_table in input_tables]
        if output_table:
            output = self._get_dataset(output_table)
            dataset_stat_facet = self._get_statistics_dataset_facet(properties)
            if dataset_stat_facet:
                output.facets.update({"outputStatistics": dataset_stat_facet})

        return inputs, output

    @staticmethod
    def _get_bigquery_job_run_facet(properties: dict) -> BigQueryJobRunFacet:
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
    def _get_statistics_dataset_facet(properties) -> OutputStatisticsOutputDatasetFacet | None:
        query_plan = get_from_nullable_chain(properties, chain=["statistics", "query", "queryPlan"])
        if not query_plan:
            return None

        out_stage = query_plan[-1]
        out_rows = out_stage.get("recordsWritten", None)
        out_bytes = out_stage.get("shuffleOutputBytes", None)
        if out_bytes and out_rows:
            return OutputStatisticsOutputDatasetFacet(rowCount=int(out_rows), size=int(out_bytes))
        return None

    def _get_dataset(self, table: dict) -> Dataset:
        project = table.get("projectId")
        dataset = table.get("datasetId")
        table_name = table.get("tableId")
        dataset_name = f"{project}.{dataset}.{table_name}"

        dataset_schema = self._get_table_schema_safely(dataset_name)
        return Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=dataset_name,
            facets={
                "schema": dataset_schema,
            }
            if dataset_schema
            else {},
        )

    def _get_table_schema_safely(self, table_name: str) -> SchemaDatasetFacet | None:
        try:
            return self._get_table_schema(table_name)
        except Exception as e:
            if hasattr(self, "log"):
                self.log.warning("Could not extract output schema from bigquery. %s", e)
        return None

    def _get_table_schema(self, table: str) -> SchemaDatasetFacet | None:
        bq_table = self.client.get_table(table)

        if not bq_table._properties:
            return None

        fields = get_from_nullable_chain(bq_table._properties, ["schema", "fields"])
        if not fields:
            return None

        return SchemaDatasetFacet(
            fields=[
                SchemaField(
                    name=field.get("name"),
                    type=field.get("type"),
                    description=field.get("description"),
                )
                for field in fields
            ]
        )

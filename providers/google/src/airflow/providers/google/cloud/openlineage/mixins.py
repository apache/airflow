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
from collections.abc import Iterable
from typing import TYPE_CHECKING, cast

from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    DatasetFacet,
    ErrorMessageRunFacet,
    ExternalQueryRunFacet,
    Fields,
    InputDataset,
    InputField,
    OutputDataset,
    OutputStatisticsOutputDatasetFacet,
    SchemaDatasetFacet,
    SQLJobFacet,
)
from airflow.providers.google.cloud.openlineage.utils import (
    BIGQUERY_NAMESPACE,
    get_facets_from_bq_table,
    get_from_nullable_chain,
    get_identity_column_lineage_facet,
    get_namespace_name_from_source_uris,
    merge_column_lineage_facets,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.openlineage.facet import Dataset, RunFacet
    from airflow.providers.google.cloud.openlineage.facets import BigQueryJobRunFacet


class _BigQueryInsertJobOperatorOpenLineageMixin:
    """Mixin for BigQueryInsertJobOperator to extract OpenLineage metadata."""

    def get_openlineage_facets_on_complete(self, _):
        """
        Retrieve OpenLineage data for a completed BigQuery job.

        This method calls BigQuery API, retrieving input and output dataset info from it,
         as well as run-level statistics.

        Run facets may contain:
            - ExternalQueryRunFacet (for QUERY job type)
            - BigQueryJobRunFacet
            - ErrorMessageRunFacet (if an error occurred)

        Job facets should contain:
            - SqlJobFacet (for QUERY job type)

        Input datasets should contain:
            - SchemaDatasetFacet

        Output datasets should contain:
            - SchemaDatasetFacet
            - OutputStatisticsOutputDatasetFacet (for QUERY job type)
            - ColumnLineageDatasetFacet (for QUERY job type)
        """
        from airflow.providers.openlineage.extractors import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        if not self.job_id:
            self.log.warning("No BigQuery job_id was found by OpenLineage.")
            return OperatorLineage()

        if not self.hook:
            # This can occur when in deferrable mode
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )

        self.log.debug("Extracting data from bigquery job: `%s`", self.job_id)
        inputs, outputs = [], []
        run_facets: dict[str, RunFacet] = {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=self.job_id, source="bigquery")
        }
        self._client = self.hook.get_client(
            project_id=self.project_id or self.hook.project_id, location=self.location
        )
        try:
            job_properties = self._client.get_job(job_id=self.job_id)._properties

            if get_from_nullable_chain(job_properties, ["status", "state"]) != "DONE":
                raise ValueError(f"Trying to extract data from running bigquery job: `{self.job_id}`")

            run_facets["bigQueryJob"] = self._get_bigquery_job_run_facet(job_properties)

            if get_from_nullable_chain(job_properties, ["statistics", "numChildJobs"]):
                self.log.debug("Found SCRIPT job. Extracting lineage from child jobs instead.")
                # SCRIPT job type has no input / output information but spawns child jobs that have one
                # https://cloud.google.com/bigquery/docs/information-schema-jobs#multi-statement_query_job
                for child_job_id in self._client.list_jobs(parent_job=self.job_id):
                    child_job_properties = self._client.get_job(job_id=child_job_id)._properties
                    child_inputs, child_outputs = self._get_inputs_and_outputs(child_job_properties)
                    inputs.extend(child_inputs)
                    outputs.extend(child_outputs)
            else:
                inputs, outputs = self._get_inputs_and_outputs(job_properties)

        except Exception as e:
            self.log.warning("Cannot retrieve job details from BigQuery.Client. %s", e, exc_info=True)
            exception_msg = traceback.format_exc()
            run_facets.update(
                {
                    "errorMessage": ErrorMessageRunFacet(
                        message=f"{e}: {exception_msg}",
                        programmingLanguage="python",
                    )
                }
            )

        return OperatorLineage(
            inputs=list(inputs),
            outputs=self._deduplicate_outputs(outputs),
            run_facets=run_facets,
            job_facets={"sql": SQLJobFacet(query=SQLParser.normalize_sql(self.sql))} if self.sql else {},
        )

    def _get_inputs_and_outputs(self, properties: dict) -> tuple[list[InputDataset], list[OutputDataset]]:
        job_type = get_from_nullable_chain(properties, ["configuration", "jobType"])

        if job_type == "QUERY":
            inputs, outputs = self._get_inputs_and_outputs_for_query_job(properties)
        elif job_type == "LOAD":
            inputs, outputs = self._get_inputs_and_outputs_for_load_job(properties)
        elif job_type == "COPY":
            inputs, outputs = self._get_inputs_and_outputs_for_copy_job(properties)
        elif job_type == "EXTRACT":
            inputs, outputs = self._get_inputs_and_outputs_for_extract_job(properties)
        else:
            self.log.debug("Unsupported job type for input/output extraction: `%s`.", job_type)  # type: ignore[attr-defined]
            inputs, outputs = [], []

        return inputs, outputs

    def _deduplicate_outputs(self, outputs: Iterable[OutputDataset | None]) -> list[OutputDataset]:
        final_outputs = {}
        for single_output in outputs:
            if not single_output:
                continue
            key = f"{single_output.namespace}.{single_output.name}"
            if key not in final_outputs:
                final_outputs[key] = single_output
                continue

            # No OutputStatisticsOutputDatasetFacet is added to duplicated outputs as we can not determine
            # if the rowCount or size can be summed together.
            if single_output.outputFacets:
                single_output.outputFacets.pop("outputStatistics", None)

            # If multiple outputs contain Column Level Lineage Facet - merge the facets
            if (
                single_output.facets
                and final_outputs[key].facets
                and "columnLineage" in single_output.facets
                and "columnLineage" in final_outputs[key].facets  # type: ignore
            ):
                single_output.facets["columnLineage"] = merge_column_lineage_facets(
                    [
                        single_output.facets["columnLineage"],  # type: ignore
                        final_outputs[key].facets["columnLineage"],  # type: ignore
                    ]
                )

            final_outputs[key] = single_output

        return list(final_outputs.values())

    def _get_input_dataset(self, table: dict) -> InputDataset:
        return cast("InputDataset", self._get_dataset(table, "input"))

    def _get_output_dataset(self, table: dict) -> OutputDataset:
        return cast("OutputDataset", self._get_dataset(table, "output"))

    def _get_dataset(self, table: dict, dataset_type: str) -> Dataset:
        project = table.get("projectId")
        dataset = table.get("datasetId")
        table_name = table.get("tableId")
        dataset_name = f"{project}.{dataset}.{table_name}"

        dataset_facets = self._get_table_facets_safely(dataset_name)
        if dataset_type == "input":
            # Logic specific to creating InputDataset (if needed)
            return InputDataset(
                namespace=BIGQUERY_NAMESPACE,
                name=dataset_name,
                facets=dataset_facets,
            )
        if dataset_type == "output":
            # Logic specific to creating OutputDataset (if needed)
            return OutputDataset(
                namespace=BIGQUERY_NAMESPACE,
                name=dataset_name,
                facets=dataset_facets,
            )
        raise ValueError("Invalid dataset_type. Must be 'input' or 'output'")

    def _get_table_facets_safely(self, table_name: str) -> dict[str, DatasetFacet]:
        try:
            bq_table = self._client.get_table(table_name)
            return get_facets_from_bq_table(bq_table)
        except Exception as e:
            self.log.warning("Could not extract facets from bigquery table: `%s`. %s", table_name, e)  # type: ignore[attr-defined]
        return {}

    def _get_inputs_and_outputs_for_query_job(
        self, properties: dict
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        input_tables = get_from_nullable_chain(properties, ["statistics", "query", "referencedTables"]) or []
        output_table = get_from_nullable_chain(properties, ["configuration", "query", "destinationTable"])

        inputs = [
            self._get_input_dataset(input_table)
            for input_table in input_tables
            if input_table != output_table  # Output table is in `referencedTables` and needs to be removed
        ]

        if not output_table:
            return inputs, []

        output = self._get_output_dataset(output_table)
        if dataset_stat_facet := self._get_output_statistics_dataset_facet(properties):
            output.outputFacets = output.outputFacets or {}
            output.outputFacets["outputStatistics"] = dataset_stat_facet
        if cll_facet := self._get_column_level_lineage_facet_for_query_job(properties, output, inputs):
            output.facets = output.facets or {}
            output.facets["columnLineage"] = cll_facet
        return inputs, [output]

    def _get_inputs_and_outputs_for_load_job(
        self, properties: dict
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        output = self._get_output_dataset(properties["configuration"]["load"]["destinationTable"])
        output_table_schema_facet = output.facets.get("schema") if output.facets else None

        source_uris = properties["configuration"]["load"]["sourceUris"]
        inputs = [
            InputDataset(
                namespace=namespace,
                name=name,
                facets={"schema": output_table_schema_facet} if output_table_schema_facet else {},
            )
            for namespace, name in get_namespace_name_from_source_uris(source_uris)
        ]

        if dataset_stat_facet := self._get_output_statistics_dataset_facet(properties):
            output.outputFacets = output.outputFacets or {}
            output.outputFacets["outputStatistics"] = dataset_stat_facet
        if cll_facet := get_identity_column_lineage_facet(self._extract_column_names(output), inputs):
            output.facets = {**output.facets, **cll_facet} if output.facets else cll_facet
        return inputs, [output]

    def _get_inputs_and_outputs_for_copy_job(
        self, properties: dict
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        input_tables = get_from_nullable_chain(properties, ["configuration", "copy", "sourceTables"]) or [
            get_from_nullable_chain(properties, ["configuration", "copy", "sourceTable"])
        ]
        inputs = [self._get_input_dataset(input_table) for input_table in input_tables]

        output = self._get_output_dataset(properties["configuration"]["copy"]["destinationTable"])
        if dataset_stat_facet := self._get_output_statistics_dataset_facet(properties):
            output.outputFacets = output.outputFacets or {}
            output.outputFacets["outputStatistics"] = dataset_stat_facet
        if cll_facet := get_identity_column_lineage_facet(self._extract_column_names(output), inputs):
            output.facets = {**output.facets, **cll_facet} if output.facets else cll_facet
        return inputs, [output]

    def _get_inputs_and_outputs_for_extract_job(
        self, properties: dict
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        source_table = get_from_nullable_chain(properties, ["configuration", "extract", "sourceTable"])
        input_dataset = self._get_input_dataset(source_table) if source_table else None

        destination_uris = get_from_nullable_chain(
            properties, ["configuration", "extract", "destinationUris"]
        ) or [get_from_nullable_chain(properties, ["configuration", "extract", "destinationUri"])]

        outputs = []
        for namespace, name in get_namespace_name_from_source_uris(destination_uris):
            output_facets = {}
            if input_dataset:
                input_schema = input_dataset.facets.get("schema") if input_dataset.facets else None
                if input_schema:
                    output_facets["schema"] = input_schema
                if cll_facet := get_identity_column_lineage_facet(
                    self._extract_column_names(input_dataset), [input_dataset]
                ):
                    output_facets = {**output_facets, **cll_facet}
            outputs.append(OutputDataset(namespace=namespace, name=name, facets=output_facets))

        inputs = [input_dataset] if input_dataset else []
        return inputs, outputs

    @staticmethod
    def _get_bigquery_job_run_facet(properties: dict) -> BigQueryJobRunFacet:
        from airflow.providers.google.cloud.openlineage.facets import BigQueryJobRunFacet

        job_type = get_from_nullable_chain(properties, ["configuration", "jobType"])
        cache_hit, billed_bytes = None, None
        if job_type == "QUERY":
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
    def _get_output_statistics_dataset_facet(
        properties,
    ) -> OutputStatisticsOutputDatasetFacet | None:
        job_type = get_from_nullable_chain(properties, ["configuration", "jobType"])
        out_rows, out_bytes = None, None
        if job_type == "QUERY":
            query_plan = get_from_nullable_chain(properties, chain=["statistics", "query", "queryPlan"])
            if not query_plan:  # Without query plan there is no statistics
                return None
            out_stage = query_plan[-1]  # Last stage of query plan writes the data and has all the statistics
            out_rows = out_stage.get("recordsWritten", None)
            out_bytes = out_stage.get("shuffleOutputBytes", None)
        elif job_type == "LOAD":
            out_rows = get_from_nullable_chain(properties, ["statistics", "load", "outputRows"])
            out_bytes = get_from_nullable_chain(properties, ["statistics", "load", "outputBytes"])
        elif job_type == "COPY":
            out_rows = get_from_nullable_chain(properties, ["statistics", "copy", "copiedRows"])
            out_bytes = get_from_nullable_chain(properties, ["statistics", "copy", "copiedLogicalBytes"])
        # No statistics available for EXTRACT job type

        if out_bytes and out_rows:
            return OutputStatisticsOutputDatasetFacet(rowCount=int(out_rows), size=int(out_bytes))
        return None

    def _get_column_level_lineage_facet_for_query_job(
        self, properties: dict, output: OutputDataset, inputs: Iterable[InputDataset]
    ) -> ColumnLineageDatasetFacet | None:
        """
        Extract column-level lineage information from a BigQuery job and return it as a facet.

        The Column Level Lineage Facet will NOT be returned if any of the following condition is met:
        - The parsed result does not contain column lineage information.
        - The parsed result does not contain exactly one output table.
        - The parsed result has a different output table than the output table from the BQ job.
        - The parsed result has at least one input table not present in the input tables from the BQ job.
        - The parsed result has a column not present in the schema of given dataset from the BQ job.

        Args:
            properties: The properties of the BigQuery job.
            output: The output dataset for which the column lineage is being extracted.

        Returns:
            The extracted Column Lineage Dataset Facet, or None if conditions are not met.
        """
        from airflow.providers.openlineage.sqlparser import SQLParser

        # Extract SQL query and parse it
        self.log.debug("Extracting column-level lineage facet from BigQuery query.")  # type: ignore[attr-defined]

        query = get_from_nullable_chain(properties, ["configuration", "query", "query"])
        if query is None:
            self.log.debug("No query found in BQ job configuration. Facet generation skipped.")  # type: ignore[attr-defined]
            return None

        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string(SQLParser.normalize_sql(query)))
        if parse_result is None or parse_result.column_lineage == []:
            self.log.debug("No column-level lineage found in the SQL query. Facet generation skipped.")  # type: ignore[attr-defined]
            return None

        default_dataset, default_project = self._extract_default_dataset_and_project(
            properties,
            self.project_id,  # type: ignore[attr-defined]
        )

        # Verify if the output table id from the parse result matches the BQ job output table
        if not self._validate_output_table_id(
            parse_result,
            output,
            default_project,
            default_dataset,
        ):
            return None

        # Verify if all columns from parse results are present in the output dataset schema
        if not self._validate_output_columns(parse_result, output):
            return None

        input_tables_from_parse_result = self._extract_parsed_input_tables(
            parse_result, default_project, default_dataset
        )
        input_tables_from_bq = {input_ds.name: self._extract_column_names(input_ds) for input_ds in inputs}

        # Verify if all datasets from parse results are present in bq job input datasets
        if not self._validate_input_tables(input_tables_from_parse_result, input_tables_from_bq):
            return None

        # Verify if all columns from parse results are present in their respective bq job input datasets
        if not self._validate_input_columns(input_tables_from_parse_result, input_tables_from_bq):
            return None

        return self._generate_column_lineage_facet(parse_result, default_project, default_dataset)

    @staticmethod
    def _get_qualified_name_from_parse_result(table, default_project: str, default_dataset: str) -> str:
        """Get the qualified name of a table from the parse result."""
        return ".".join(
            (
                table.database or default_project,
                table.schema or default_dataset,
                table.name,
            )
        )

    @staticmethod
    def _extract_default_dataset_and_project(properties: dict, default_project: str) -> tuple[str, str]:
        """Extract the default dataset and project from the BigQuery job properties."""
        default_dataset_obj = get_from_nullable_chain(
            properties, ["configuration", "query", "defaultDataset"]
        )
        default_dataset = default_dataset_obj.get("datasetId", "") if default_dataset_obj else ""
        default_project = (
            default_dataset_obj.get("projectId", default_project) if default_dataset_obj else default_project
        )
        return default_dataset, default_project

    def _validate_output_table_id(
        self, parse_result, output: OutputDataset, default_project: str, default_dataset: str
    ) -> bool:
        """Check if the output table id from the parse result matches the BQ job output table."""
        if len(parse_result.out_tables) != 1:
            self.log.debug(  # type: ignore[attr-defined]
                "Invalid output tables in the parse result: `%s`. Expected exactly one output table.",
                parse_result.out_tables,
            )
            return False

        parsed_output_table = self._get_qualified_name_from_parse_result(
            parse_result.out_tables[0], default_project, default_dataset
        )
        if parsed_output_table != output.name:
            self.log.debug(  # type: ignore[attr-defined]
                "Mismatch between parsed output table `%s` and BQ job output table `%s`.",
                parsed_output_table,
                output.name,
            )
            return False
        return True

    @staticmethod
    def _extract_column_names(dataset: Dataset) -> list[str]:
        """Extract column names from a dataset's schema."""
        return [
            f.name
            for f in dataset.facets.get("schema", SchemaDatasetFacet(fields=[])).fields  # type: ignore[union-attr]
            if dataset.facets
        ]

    def _validate_output_columns(self, parse_result, output: OutputDataset) -> bool:
        """Validate if all descendant columns in parse result exist in output dataset schema."""
        output_column_names = self._extract_column_names(output)
        missing_columns = [
            lineage.descendant.name
            for lineage in parse_result.column_lineage
            if lineage.descendant.name not in output_column_names
        ]
        if missing_columns:
            self.log.debug(  # type: ignore[attr-defined]
                "Output dataset schema is missing columns from the parse result: `%s`.", missing_columns
            )
            return False
        return True

    def _extract_parsed_input_tables(
        self, parse_result, default_project: str, default_dataset: str
    ) -> dict[str, list[str]]:
        """Extract input tables and their columns from the parse result."""
        input_tables: dict[str, list[str]] = {}
        for lineage in parse_result.column_lineage:
            for column_meta in lineage.lineage:
                if not column_meta.origin:
                    self.log.debug(  # type: ignore[attr-defined]
                        "Column `%s` lacks origin information. Skipping facet generation.", column_meta.name
                    )
                    return {}

                input_table_id = self._get_qualified_name_from_parse_result(
                    column_meta.origin, default_project, default_dataset
                )
                input_tables.setdefault(input_table_id, []).append(column_meta.name)
        return input_tables

    def _validate_input_tables(
        self, parsed_input_tables: dict[str, list[str]], input_tables_from_bq: dict[str, list[str]]
    ) -> bool:
        """Validate if all parsed input tables exist in the BQ job's input datasets."""
        if not parsed_input_tables:
            self.log.debug("No input tables found in the parse result. Facet generation skipped.")  # type: ignore[attr-defined]
            return False
        if missing_tables := set(parsed_input_tables) - set(input_tables_from_bq):
            self.log.debug(  # type: ignore[attr-defined]
                "Parsed input tables not found in the BQ job's input datasets: `%s`.", missing_tables
            )
            return False
        return True

    def _validate_input_columns(
        self, parsed_input_tables: dict[str, list[str]], input_tables_from_bq: dict[str, list[str]]
    ) -> bool:
        """Validate if all parsed input columns exist in their respective BQ job input table schemas."""
        if not parsed_input_tables:
            self.log.debug("No input tables found in the parse result. Facet generation skipped.")  # type: ignore[attr-defined]
            return False
        for table, columns in parsed_input_tables.items():
            if missing_columns := set(columns) - set(input_tables_from_bq.get(table, [])):
                self.log.debug(  # type: ignore[attr-defined]
                    "Input table `%s` is missing columns from the parse result: `%s`.", table, missing_columns
                )
                return False
        return True

    def _generate_column_lineage_facet(
        self, parse_result, default_project: str, default_dataset: str
    ) -> ColumnLineageDatasetFacet:
        """Generate the ColumnLineageDatasetFacet based on the parsed result."""
        return ColumnLineageDatasetFacet(
            fields={
                lineage.descendant.name: Fields(
                    inputFields=[
                        InputField(
                            namespace=BIGQUERY_NAMESPACE,
                            name=self._get_qualified_name_from_parse_result(
                                column_meta.origin, default_project, default_dataset
                            ),
                            field=column_meta.name,
                        )
                        for column_meta in lineage.lineage
                    ],
                    transformationType="",
                    transformationDescription="",
                )
                for lineage in parse_result.column_lineage
            }
        )

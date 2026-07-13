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
"""
DAG with a custom operator that emits all importable openlineage.client.facet_v2 facets that OL do not overwrite.

It checks:
    - all run facets
    - all job facets
    - all input dataset facets
    - all output dataset facets
    - one custom facet per entity
    - all optional parameters are populated for comprehensive serialization coverage
"""

from __future__ import annotations

from datetime import datetime

from openlineage.client.event_v2 import InputDataset, OutputDataset
from openlineage.client.facet_v2 import (
    base_subset_dataset,
    catalog_dataset,
    column_lineage_dataset,
    data_quality_assertions_dataset,
    data_quality_metrics_dataset,
    data_quality_metrics_input_dataset,
    dataset_type_dataset,
    dataset_version_dataset,
    datasource_dataset,
    documentation_dataset,
    documentation_job,
    environment_variables_run,
    error_message_run,
    execution_parameters_run,
    external_query_run,
    extraction_error_run,
    hierarchy_dataset,
    input_statistics_input_dataset,
    job_dependencies_run,
    lifecycle_state_change_dataset,
    output_statistics_output_dataset,
    ownership_dataset,
    ownership_job,
    schema_dataset,
    source_code_job,
    source_code_location_job,
    sql_job,
    storage_dataset,
    symlinks_dataset,
    tags_dataset,
    tags_job,
    tags_run,
    test_run as test_run_facet_module,
)

from airflow import DAG
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


class AllFacetsOperator(BaseOperator):
    def execute(self, context):
        pass

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        input_ds = InputDataset(
            namespace="s3://all-facets-bucket",
            name="input/data.csv",
            facets={
                "custom_input_ds_facet": {"key": "value"},  # type: ignore[dict-item]
                "schema": schema_dataset.SchemaDatasetFacet(
                    fields=[
                        schema_dataset.SchemaDatasetFacetFields(
                            name="id",
                            type="INTEGER",
                            description="Unique row identifier",
                            ordinal_position=1,
                        ),
                        schema_dataset.SchemaDatasetFacetFields(
                            name="name",
                            type="VARCHAR",
                            description="Full name of the entity",
                            ordinal_position=2,
                        ),
                        schema_dataset.SchemaDatasetFacetFields(
                            name="address",
                            type="STRUCT",
                            description="Nested address record",
                            ordinal_position=3,
                            fields=[
                                schema_dataset.SchemaDatasetFacetFields(
                                    name="street",
                                    type="VARCHAR",
                                    description="Street address line",
                                    ordinal_position=1,
                                ),
                                schema_dataset.SchemaDatasetFacetFields(
                                    name="city",
                                    type="VARCHAR",
                                    description="City name",
                                    ordinal_position=2,
                                ),
                            ],
                        ),
                    ]
                ),
                "dataSource": datasource_dataset.DatasourceDatasetFacet(
                    name="all-facets-source",
                    uri="s3://all-facets-bucket",
                ),
                "columnLineage": column_lineage_dataset.ColumnLineageDatasetFacet(
                    fields={
                        "name": column_lineage_dataset.Fields(
                            inputFields=[
                                column_lineage_dataset.InputField(
                                    namespace="s3://upstream",
                                    name="upstream/data.csv",
                                    field="full_name",
                                    transformations=[
                                        column_lineage_dataset.Transformation(
                                            type="DIRECT",
                                            subtype="IDENTITY",
                                            description="Direct copy of the full_name field",
                                            masking=False,
                                        )
                                    ],
                                )
                            ],
                            transformationDescription="Column passed through without modification",
                            transformationType="IDENTITY",
                        ),
                    },
                ),
                "documentation": documentation_dataset.DocumentationDatasetFacet(
                    description="Input dataset for all-facets comprehensive serialization test",
                    contentType="text/plain",
                ),
                "inputStatistics": input_statistics_input_dataset.InputStatisticsInputDatasetFacet(  # type: ignore[dict-item]
                    rowCount=1000,
                    size=8192,
                    fileCount=1,
                ),
                "dataQualityAssertions": data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(  # type: ignore[dict-item]
                    assertions=[
                        data_quality_assertions_dataset.Assertion(
                            assertion="not_null",
                            success=True,
                            column="id",
                            severity="ERROR",
                            name="id_not_null_check",
                            description="Checks that id column has no null values",
                            expected="0 nulls",
                            actual="0 nulls",
                            content='{"nullCount": 0}',
                            contentType="application/json",
                            params={"sample_size": "1000"},
                        ),
                        data_quality_assertions_dataset.Assertion(
                            assertion="row_count_above_threshold",
                            success=True,
                            severity="WARNING",
                            name="row_count_check",
                            description="Checks that row count is above the minimum threshold",
                            expected=">= 100",
                            actual="1000",
                        ),
                    ],
                ),
                "ownership": ownership_dataset.OwnershipDatasetFacet(
                    owners=[
                        ownership_dataset.Owner(name="team:data-engineering", type="team"),
                        ownership_dataset.Owner(name="user:jane.smith@example.com", type="user"),
                    ],
                ),
                "tags": tags_dataset.TagsDatasetFacet(
                    tags=[
                        tags_dataset.TagsDatasetFacetFields(
                            key="env",
                            value="test",
                            source="airflow-system-test",
                        ),
                        tags_dataset.TagsDatasetFacetFields(
                            key="source",
                            value="s3",
                            source="airflow-system-test",
                        ),
                        tags_dataset.TagsDatasetFacetFields(
                            key="format",
                            value="csv",
                            source="airflow-system-test",
                            field="id",
                        ),
                    ],
                ),
                "catalog": catalog_dataset.CatalogDatasetFacet(
                    framework="iceberg",
                    type="TABLE",
                    name="all_facets_input",
                    metadataUri="s3://metastore/all_facets.json",
                    warehouseUri="s3://all-facets-bucket/warehouse",
                    source="s3://all-facets-bucket/catalog.json",
                    catalogProperties={"location": "s3://all-facets-bucket/warehouse/all_facets_input"},
                ),
                "dataQualityMetrics": data_quality_metrics_dataset.DataQualityMetricsDatasetFacet(
                    columnMetrics={
                        "id": data_quality_metrics_dataset.ColumnMetrics(
                            nullCount=0,
                            distinctCount=1000,
                            sum=500500.0,
                            count=1000,
                            min=1.0,
                            max=1000.0,
                            quantiles={"0.25": 250.0, "0.5": 500.0, "0.75": 750.0},
                        ),
                        "name": data_quality_metrics_dataset.ColumnMetrics(
                            nullCount=5,
                            distinctCount=980,
                            count=1000,
                        ),
                    },
                    rowCount=1000,
                    bytes=8192,
                    fileCount=1,
                ),
                "dataQualityMetricsInput": data_quality_metrics_input_dataset.DataQualityMetricsInputDatasetFacet(  # type: ignore[dict-item]
                    columnMetrics={
                        "id": data_quality_metrics_input_dataset.ColumnMetrics(
                            nullCount=0,
                            distinctCount=1000,
                            sum=500500.0,
                            count=1000,
                            min=1.0,
                            max=1000.0,
                            quantiles={"0.25": 250.0, "0.5": 500.0, "0.75": 750.0},
                        ),
                    },
                    rowCount=1000,
                    bytes=8192,
                    fileCount=1,
                ),
                "datasetType": dataset_type_dataset.DatasetTypeDatasetFacet(
                    datasetType="TABLE",
                    subType="ICEBERG_TABLE",
                ),
                "hierarchy": hierarchy_dataset.HierarchyDatasetFacet(
                    hierarchy=[
                        hierarchy_dataset.HierarchyDatasetFacetLevel(type="catalog", name="aws-glue"),
                        hierarchy_dataset.HierarchyDatasetFacetLevel(type="database", name="analytics"),
                        hierarchy_dataset.HierarchyDatasetFacetLevel(type="schema", name="public"),
                    ],
                ),
                "inputSubset": base_subset_dataset.InputSubsetInputDatasetFacet(  # type: ignore[dict-item]
                    inputCondition=base_subset_dataset.PartitionSubsetCondition(
                        partitions=[
                            base_subset_dataset.Partition(
                                dimensions={"business_date": "2024-10-15", "country": "PL"},
                                identifier="2024-01-01/us-east-1",
                            )
                        ],
                        type="partition",
                    ),
                ),
            },
        )

        output_ds = OutputDataset(
            namespace="snowflake://account",
            name="analytics.public.all_facets_output",
            facets={
                "custom_output_ds_facet": {"key": "value"},  # type: ignore[dict-item]
                "outputStatistics": output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(  # type: ignore[dict-item]
                    rowCount=500,
                    size=4096,
                    fileCount=1,
                ),
                "storage": storage_dataset.StorageDatasetFacet(
                    storageLayer="snowflake",
                    fileFormat="table",
                ),
                "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                    identifiers=[
                        symlinks_dataset.Identifier(
                            namespace="snowflake://account",
                            name="analytics.public.all_facets_output_alias",
                            type="TABLE",
                        ),
                    ],
                ),
                "version": dataset_version_dataset.DatasetVersionDatasetFacet(
                    datasetVersion="v1.0.0",
                ),
                "lifecycleStateChange": lifecycle_state_change_dataset.LifecycleStateChangeDatasetFacet(
                    lifecycleStateChange=lifecycle_state_change_dataset.LifecycleStateChange.CREATE,
                    previousIdentifier=lifecycle_state_change_dataset.PreviousIdentifier(
                        name="analytics.public.all_facets_output_v0",
                        namespace="snowflake://account",
                    ),
                ),
                "outputSubset": base_subset_dataset.OutputSubsetOutputDatasetFacet(  # type: ignore[dict-item]
                    outputCondition=base_subset_dataset.PartitionSubsetCondition(
                        partitions=[
                            base_subset_dataset.Partition(
                                dimensions={"business_date": "2024-10-15", "country": "PL"},
                                identifier="2024-01-01",
                            )
                        ],
                        type="partition",
                    ),
                ),
            },
        )

        return OperatorLineage(
            inputs=[input_ds],
            outputs=[output_ds],
            run_facets={
                "custom_run_facet": {"key": "value"},
                "tags": tags_run.TagsRunFacet(
                    tags=[
                        tags_run.TagsRunFacetFields(
                            key="test_type",
                            value="all_facets",
                            source="airflow-system-test",
                        ),
                    ],
                ),
                "externalQuery": external_query_run.ExternalQueryRunFacet(
                    externalQueryId="all-facets-query-id-001",
                    source="snowflake://account",
                ),
                "testRun": test_run_facet_module.TestRunFacet(
                    tests=[
                        test_run_facet_module.TestExecution(
                            name="all_facets_test",
                            status="success",
                            severity="WARNING",
                            type="integration",
                            description="Checks all facets are emitted and serialized correctly",
                            expected="all_facets_present",
                            actual="all_facets_present",
                            content='{"facetCount": 37}',
                            contentType="application/json",
                            params={"batch": "1"},
                        ),
                    ],
                ),
                "environmentVariables": environment_variables_run.EnvironmentVariablesRunFacet(
                    environmentVariables=[
                        environment_variables_run.EnvironmentVariable(
                            name="SPARK_MASTER",
                            value="yarn",
                        ),
                        environment_variables_run.EnvironmentVariable(
                            name="JAVA_HOME",
                            value="/usr/lib/jvm/java-11-openjdk",
                        ),
                    ],
                ),
                "errorMessage": error_message_run.ErrorMessageRunFacet(
                    message="Non-fatal warning detected during extraction phase",
                    programmingLanguage="python",
                    stackTrace=(
                        "Traceback (most recent call last):\n"
                        '  File "extract.py", line 42, in run\n'
                        '    raise ValueError("Partial extraction failure")\n'
                        "ValueError: Partial extraction failure"
                    ),
                ),
                "executionParameters": execution_parameters_run.ExecutionParametersRunFacet(
                    parameters=[
                        execution_parameters_run.ExecutionParameter(
                            key="executor-cores",
                            name="executor-cores",
                            description="Number of CPU cores per executor",
                            value="4",
                        ),
                        execution_parameters_run.ExecutionParameter(
                            key="executor-memory",
                            name="executor-memory",
                            description="Memory allocated per executor in gigabytes",
                            value="8g",
                        ),
                    ],
                ),
                "extractionError": extraction_error_run.ExtractionErrorRunFacet(
                    totalTasks=10,
                    failedTasks=1,
                    errors=[
                        extraction_error_run.Error(
                            errorMessage="Unable to parse column 'event_ts' as TIMESTAMP",
                            stackTrace=(
                                "Traceback (most recent call last):\n"
                                '  File "schema.py", line 18, in parse_column\n'
                                '    raise TypeError("Cannot cast to TIMESTAMP")\n'
                                "TypeError: Cannot cast to TIMESTAMP"
                            ),
                            task="parse_schema",
                            taskNumber=3,
                        ),
                    ],
                ),
                "jobDependencies": job_dependencies_run.JobDependenciesRunFacet(
                    upstream=[
                        job_dependencies_run.JobDependency(
                            job=job_dependencies_run.JobIdentifier(
                                namespace="default",
                                name="upstream_etl_job",
                            ),
                            run=job_dependencies_run.RunIdentifier(
                                runId="00000000-0000-0000-0000-000000000001",
                            ),
                            dependency_type="WAIT",
                            sequence_trigger_rule="ALL_DONE",
                            status_trigger_rule="ALL_SUCCESS",
                        ),
                    ],
                    downstream=[],
                    trigger_rule="ALL_SUCCESS",
                ),
            },
            job_facets={
                "custom_job_facet": {"key": "value"},
                "tags": tags_job.TagsJobFacet(
                    tags=[
                        tags_job.TagsJobFacetFields(
                            key="domain",
                            value="data-platform",
                            source="airflow-system-test",
                        ),
                        tags_job.TagsJobFacetFields(
                            key="team",
                            value="data-engineering",
                            source="airflow-system-test",
                        ),
                    ],
                ),
                "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
                    type="git",
                    url="https://github.com/apache/airflow/blob/main/providers/openlineage/tests/system/openlineage/example_openlineage_all_facets_dag.py",
                    repoUrl="https://github.com/apache/airflow",
                    path="providers/openlineage/tests/system/openlineage/example_openlineage_all_facets_dag.py",
                    version="main",
                    tag="v2.10.0",
                    branch="main",
                    pullRequestNumber="12345",
                ),
                "documentation": documentation_job.DocumentationJobFacet(
                    description="All-facets test operator emitting every available OL facet",
                ),
                "sql": sql_job.SQLJobFacet(
                    query="SELECT id, name FROM analytics.public.all_facets_output WHERE id IS NOT NULL",
                    dialect="snowflake_sql",
                ),
                "ownership": ownership_job.OwnershipJobFacet(
                    owners=[
                        ownership_job.Owner(name="data-engineering-team", type="team"),
                        ownership_job.Owner(name="user:john.doe@example.com", type="user"),
                    ],
                ),
                "sourceCode": source_code_job.SourceCodeJobFacet(
                    language="python",
                    sourceCode=(
                        "def transform(df):\n    return df.select('id', 'name').filter(df.id.isNotNull())\n"
                    ),
                ),
            },
        )


DAG_ID = "openlineage_all_facets_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    all_facets_task = AllFacetsOperator(task_id="all_facets_task")

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    all_facets_task >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)

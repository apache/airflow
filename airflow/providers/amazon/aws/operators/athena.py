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

from typing import TYPE_CHECKING, Any, Sequence
from urllib.parse import urlparse

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from openlineage.client.facet import BaseFacet
    from openlineage.client.run import Dataset

    from airflow.providers.openlineage.extractors.base import OperatorLineage
    from airflow.utils.context import Context


class AthenaOperator(AwsBaseOperator[AthenaHook]):
    """
    An operator that submits a Trino/Presto query to Amazon Athena.

    .. note:: if the task is killed while it runs, it'll cancel the athena query that was launched,
        EXCEPT if running in deferrable mode.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AthenaOperator`

    :param query: Trino/Presto query to be run on Amazon Athena. (templated)
    :param database: Database to select. (templated)
    :param catalog: Catalog to select. (templated)
    :param output_location: s3 path to write the query results into. (templated)
        To run the query, you must specify the query results location using one of the ways:
        either for individual queries using either this setting (client-side),
        or in the workgroup, using WorkGroupConfiguration.
        If none of them is set, Athena issues an error that no output location is provided
    :param client_request_token: Unique token created by user to avoid multiple executions of same query
    :param workgroup: Athena workgroup in which query will be run. (templated)
    :param query_execution_context: Context in which query need to be run
    :param result_configuration: Dict with path to store results in and config related to encryption
    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
    :param max_polling_attempts: Number of times to poll for query state before function exits
        To limit task execution time, use execution_timeout.
    :param log_query: Whether to log athena query and other execution params when it's executed.
        Defaults to *True*.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = AthenaHook
    ui_color = "#44b5e2"
    template_fields: Sequence[str] = aws_template_fields(
        "query", "database", "output_location", "workgroup", "catalog"
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}

    def __init__(
        self,
        *,
        query: str,
        database: str,
        output_location: str | None = None,
        client_request_token: str | None = None,
        workgroup: str = "primary",
        query_execution_context: dict[str, str] | None = None,
        result_configuration: dict[str, Any] | None = None,
        sleep_time: int = 30,
        max_polling_attempts: int | None = None,
        log_query: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        catalog: str = "AwsDataCatalog",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.database = database
        self.output_location = output_location
        self.client_request_token = client_request_token
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.max_polling_attempts = max_polling_attempts or 999999
        self.query_execution_id: str | None = None
        self.log_query: bool = log_query
        self.deferrable = deferrable
        self.catalog: str = catalog

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "log_query": self.log_query}

    def execute(self, context: Context) -> str | None:
        """Run Trino/Presto Query on Amazon Athena."""
        self.query_execution_context["Database"] = self.database
        self.query_execution_context["Catalog"] = self.catalog
        if self.output_location:
            self.result_configuration["OutputLocation"] = self.output_location
        self.query_execution_id = self.hook.run_query(
            self.query,
            self.query_execution_context,
            self.result_configuration,
            self.client_request_token,
            self.workgroup,
        )

        if self.deferrable:
            self.defer(
                trigger=AthenaTrigger(
                    query_execution_id=self.query_execution_id,
                    waiter_delay=self.sleep_time,
                    waiter_max_attempts=self.max_polling_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )
        # implicit else:
        query_status = self.hook.poll_query_status(
            self.query_execution_id,
            max_polling_attempts=self.max_polling_attempts,
            sleep_time=self.sleep_time,
        )

        if query_status in AthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(self.query_execution_id)
            raise Exception(
                f"Final state of Athena job is {query_status}, query_execution_id is "
                f"{self.query_execution_id}. Error: {error_message}"
            )
        elif not query_status or query_status in AthenaHook.INTERMEDIATE_STATES:
            raise Exception(
                f"Final state of Athena job is {query_status}. Max tries of poll status exceeded, "
                f"query_execution_id is {self.query_execution_id}."
            )

        # Save output location from API response for later use in OpenLineage.
        self.output_location = self.hook.get_output_location(self.query_execution_id)

        return self.query_execution_id

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error while waiting for operation on cluster to complete: {event}")
        return event["value"]

    def on_kill(self) -> None:
        """Cancel the submitted Amazon Athena query."""
        if self.query_execution_id:
            self.log.info("Received a kill signal.")
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            except Exception:
                self.log.exception(
                    "Exception while cancelling query. Query execution id: %s", self.query_execution_id
                )
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error("Unable to request query cancel on athena. Exiting")
                else:
                    self.log.info(
                        "Polling Athena for query with id %s to reach final state", self.query_execution_id
                    )
                    self.hook.poll_query_status(self.query_execution_id, sleep_time=self.sleep_time)

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        """Retrieve OpenLineage data by parsing SQL queries and enriching them with Athena API.

        In addition to CTAS query, query and calculation results are stored in S3 location.
        For that reason additional output is attached with this location.
        """
        from openlineage.client.facet import ExtractionError, ExtractionErrorRunFacet, SqlJobFacet
        from openlineage.client.run import Dataset

        from airflow.providers.openlineage.extractors.base import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        sql_parser = SQLParser(dialect="generic")

        job_facets: dict[str, BaseFacet] = {"sql": SqlJobFacet(query=sql_parser.normalize_sql(self.query))}
        parse_result = sql_parser.parse(sql=self.query)

        if not parse_result:
            return OperatorLineage(job_facets=job_facets)

        run_facets: dict[str, BaseFacet] = {}
        if parse_result.errors:
            run_facets["extractionError"] = ExtractionErrorRunFacet(
                totalTasks=len(self.query) if isinstance(self.query, list) else 1,
                failedTasks=len(parse_result.errors),
                errors=[
                    ExtractionError(
                        errorMessage=error.message,
                        stackTrace=None,
                        task=error.origin_statement,
                        taskNumber=error.index,
                    )
                    for error in parse_result.errors
                ],
            )

        inputs: list[Dataset] = list(
            filter(
                None,
                [
                    self.get_openlineage_dataset(table.schema or self.database, table.name)
                    for table in parse_result.in_tables
                ],
            )
        )

        outputs: list[Dataset] = list(
            filter(
                None,
                [
                    self.get_openlineage_dataset(table.schema or self.database, table.name)
                    for table in parse_result.out_tables
                ],
            )
        )

        if self.output_location:
            parsed = urlparse(self.output_location)
            outputs.append(Dataset(namespace=f"{parsed.scheme}://{parsed.netloc}", name=parsed.path))

        return OperatorLineage(job_facets=job_facets, run_facets=run_facets, inputs=inputs, outputs=outputs)

    def get_openlineage_dataset(self, database, table) -> Dataset | None:
        from openlineage.client.facet import (
            SchemaDatasetFacet,
            SchemaField,
            SymlinksDatasetFacet,
            SymlinksDatasetFacetIdentifiers,
        )
        from openlineage.client.run import Dataset

        client = self.hook.get_conn()
        try:
            table_metadata = client.get_table_metadata(
                CatalogName=self.catalog, DatabaseName=database, TableName=table
            )

            # Dataset has also its' physical location which we can add in symlink facet.
            s3_location = table_metadata["TableMetadata"]["Parameters"]["location"]
            parsed_path = urlparse(s3_location)
            facets: dict[str, BaseFacet] = {
                "symlinks": SymlinksDatasetFacet(
                    identifiers=[
                        SymlinksDatasetFacetIdentifiers(
                            namespace=f"{parsed_path.scheme}://{parsed_path.netloc}",
                            name=str(parsed_path.path),
                            type="TABLE",
                        )
                    ]
                )
            }
            fields = [
                SchemaField(name=column["Name"], type=column["Type"], description=column["Comment"])
                for column in table_metadata["TableMetadata"]["Columns"]
            ]
            if fields:
                facets["schema"] = SchemaDatasetFacet(fields=fields)
            return Dataset(
                namespace=f"awsathena://athena.{self.hook.region_name}.amazonaws.com",
                name=".".join(filter(None, (self.catalog, database, table))),
                facets=facets,
            )

        except Exception as e:
            self.log.error("Cannot retrieve table metadata from Athena.Client. %s", e)
            return None

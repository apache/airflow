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

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Final, cast
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.links.athena import AthenaQueryResultsLink
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf

_DURABLE_UNSET = object()
_NOT_FOUND_QUERY_STATE: Final[str] = "NOT_FOUND"


def _warn_and_disable_durable_pre_3_3(durable: Any) -> bool:
    if durable is not _DURABLE_UNSET:
        warnings.warn(
            "`durable` has no effect on Airflow versions below 3.3.",
            UserWarning,
            stacklevel=3,
        )
    return False


def _is_query_not_found_error(*, error: ClientError, query_execution_id: str) -> bool:
    error_details: dict[str, Any] = error.response.get("Error", {})
    message: Any = error_details.get("Message")
    return (
        error_details.get("Code") == "InvalidRequestException"
        and isinstance(message, str)
        and message == f"QueryExecution {query_execution_id} was not found"
    )


try:
    from airflow.sdk import ResumableJobMixin
except ImportError:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """Airflow <3.3 stub, task_state_store unavailable, always submits fresh."""

        external_id_key: str = "athena_query_execution_id"

        def __init__(self, *, durable: Any = _DURABLE_UNSET, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = _warn_and_disable_durable_pre_3_3(durable)

        def execute_resumable(self, context: Context) -> Any:
            operator = cast("AthenaOperator", self)
            external_id = operator.submit_job(context)
            operator.poll_until_complete(external_id, context)
            return operator.get_job_result(external_id, context)


if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.providers.common.compat.openlineage.facet import BaseFacet, Dataset, DatasetFacet
    from airflow.providers.openlineage.extractors.base import OperatorLineage
    from airflow.sdk import Context


class AthenaOperator(ResumableJobMixin, AwsBaseOperator[AthenaHook]):
    """
    An operator that submits a Trino/Presto query to Amazon Athena.

    .. note:: if the task is killed while it runs, it'll cancel the athena query that was launched,
        EXCEPT if running in deferrable mode.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AthenaOperator`

    :param query: Trino/Presto query to be run on Amazon Athena. (templated)
    :param database: Default database for query execution. (templated)
        This argument is optional when the query does not require a default database,
        such as when all referenced table names are fully qualified.
        If omitted or set to ``None``, any ``Database`` value set in
        the ``query_execution_context`` will be used instead.
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
    :param durable: When ``True`` (the default on Airflow 3.3+), the Athena query execution id
        is persisted to task state before synchronous polling begins. A worker crash on retry
        reconnects to the existing query instead of submitting a duplicate. Set to ``False`` to
        always submit a fresh query. On earlier Airflow versions, this defaults to ``False`` and
        an explicitly configured value is ignored with a warning.
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
    operator_extra_links = (AthenaQueryResultsLink(),)
    external_id_key: str = "athena_query_execution_id"

    def __init__(
        self,
        *,
        query: str,
        database: str | None = None,
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
        durable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        if durable is not None:
            kwargs["durable"] = durable
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
        self._query_results_link_id: str | None = None
        self._query_status: str | None = None
        self.log_query: bool = log_query
        self.deferrable = deferrable
        self.catalog: str = catalog

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "log_query": self.log_query}

    def execute(self, context: Context) -> str | None:
        """Run Trino/Presto Query on Amazon Athena."""
        if self.database:
            self.query_execution_context["Database"] = self.database
        self.query_execution_context["Catalog"] = self.catalog
        if self.output_location:
            self.result_configuration["OutputLocation"] = self.output_location

        if self.deferrable:
            query_execution_id = self.submit_job(context)
            self._set_query_execution_id(context=context, query_execution_id=query_execution_id)
            self.defer(
                trigger=AthenaTrigger(
                    query_execution_id=query_execution_id,
                    waiter_delay=self.sleep_time,
                    waiter_max_attempts=self.max_polling_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )
            return query_execution_id

        self.execute_resumable(context)
        query_status = self._query_status
        query_execution_id = cast("str", self.query_execution_id)

        if query_status in AthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(query_execution_id=query_execution_id)
            raise AirflowException(
                f"Final state of Athena job is {query_status}, query_execution_id is "
                f"{query_execution_id}. Error: {error_message}"
            )
        if not query_status or query_status in AthenaHook.INTERMEDIATE_STATES:
            raise AirflowException(
                f"Final state of Athena job is {query_status}. Max tries of poll status exceeded, "
                f"query_execution_id is {query_execution_id}."
            )

        return query_execution_id

    def _set_query_execution_id(self, *, context: Context, query_execution_id: str) -> None:
        self.query_execution_id = query_execution_id
        if self._query_results_link_id == query_execution_id:
            return
        AthenaQueryResultsLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            query_execution_id=query_execution_id,
        )
        self._query_results_link_id = query_execution_id

    def submit_job(self, context: Context) -> str:
        query_execution_id: str = self.hook.run_query(
            query=self.query,
            query_context=self.query_execution_context,
            result_configuration=self.result_configuration,
            client_request_token=self.client_request_token,
            workgroup=self.workgroup,
        )
        self.query_execution_id = query_execution_id
        return query_execution_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        query_execution_id = cast("str", external_id)
        self._set_query_execution_id(context=context, query_execution_id=query_execution_id)
        try:
            query_status = self.hook.check_query_status(query_execution_id=query_execution_id)
        except ClientError as error:
            if _is_query_not_found_error(error=error, query_execution_id=query_execution_id):
                return _NOT_FOUND_QUERY_STATE
            raise
        if query_status not in (*AthenaHook.INTERMEDIATE_STATES, *AthenaHook.TERMINAL_STATES):
            raise ValueError(f"Unexpected Athena query status: {query_status!r}")
        self._query_status = query_status
        return query_status

    def is_job_active(self, status: str) -> bool:
        return status in AthenaHook.INTERMEDIATE_STATES

    def is_job_succeeded(self, status: str) -> bool:
        return status in AthenaHook.SUCCESS_STATES

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        query_execution_id = cast("str", external_id)
        self._set_query_execution_id(context=context, query_execution_id=query_execution_id)
        self._query_status = self.hook.poll_query_status(
            query_execution_id=query_execution_id,
            max_polling_attempts=self.max_polling_attempts,
            sleep_time=self.sleep_time,
        )

    def get_job_result(self, external_id: JsonValue, context: Context) -> str:
        query_execution_id = cast("str", external_id)
        self._set_query_execution_id(context=context, query_execution_id=query_execution_id)
        return query_execution_id

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(
                f"Error while waiting for operation on cluster to complete: {validated_event}"
            )

        # Save query_execution_id to be later used by listeners
        self.query_execution_id = validated_event["value"]
        return validated_event["value"]

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

    def get_openlineage_facets_on_complete(self, _) -> OperatorLineage:
        """
        Retrieve OpenLineage data by parsing SQL queries and enriching them with Athena API.

        In addition to CTAS query, query and calculation results are stored in S3 location.
        For that reason additional output is attached with this location. Instead of using the complete
        path where the results are saved (user's prefix + some UUID), we are creating a dataset with the
        user-provided path only. This should make it easier to match this dataset across different processes.
        """
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            Error,
            ExternalQueryRunFacet,
            ExtractionErrorRunFacet,
            SQLJobFacet,
        )
        from airflow.providers.openlineage.extractors.base import OperatorLineage
        from airflow.providers.openlineage.sqlparser import SQLParser

        sql_parser = SQLParser(dialect="generic")

        job_facets: dict[str, BaseFacet] = {"sql": SQLJobFacet(query=sql_parser.normalize_sql(self.query))}
        parse_result = sql_parser.parse(sql=self.query)

        if not parse_result:
            return OperatorLineage(job_facets=job_facets)

        run_facets: dict[str, BaseFacet] = {}
        if parse_result.errors:
            run_facets["extractionError"] = ExtractionErrorRunFacet(
                totalTasks=1,
                failedTasks=len(parse_result.errors),
                errors=[
                    Error(
                        errorMessage=error.message,
                        stackTrace=None,
                        task=error.origin_statement,
                        taskNumber=error.index,
                    )
                    for error in parse_result.errors
                ],
            )

        fallback_database = self.database or self.query_execution_context.get("Database")

        inputs: list[Dataset] = list(
            filter(
                None,
                [
                    self.get_openlineage_dataset(table.schema or fallback_database, table.name)
                    for table in parse_result.in_tables
                ],
            )
        )

        outputs: list[Dataset] = list(
            filter(
                None,
                [
                    self.get_openlineage_dataset(table.schema or fallback_database, table.name)
                    for table in parse_result.out_tables
                ],
            )
        )

        if self.query_execution_id:
            run_facets["externalQuery"] = ExternalQueryRunFacet(
                externalQueryId=self.query_execution_id, source="awsathena"
            )

        if self.output_location:
            parsed = urlparse(self.output_location)
            outputs.append(Dataset(namespace=f"{parsed.scheme}://{parsed.netloc}", name=parsed.path or "/"))

        return OperatorLineage(job_facets=job_facets, run_facets=run_facets, inputs=inputs, outputs=outputs)

    def get_openlineage_dataset(self, database, table) -> Dataset | None:
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            Identifier,
            SchemaDatasetFacet,
            SchemaDatasetFacetFields,
            SymlinksDatasetFacet,
        )

        client = self.hook.get_conn()
        try:
            table_metadata = client.get_table_metadata(
                CatalogName=self.catalog, DatabaseName=database, TableName=table
            )

            # Dataset has also its' physical location which we can add in symlink facet.
            s3_location = table_metadata["TableMetadata"]["Parameters"]["location"]
            parsed_path = urlparse(s3_location)
            facets: dict[str, DatasetFacet] = {
                "symlinks": SymlinksDatasetFacet(
                    identifiers=[
                        Identifier(
                            namespace=f"{parsed_path.scheme}://{parsed_path.netloc}",
                            name=str(parsed_path.path),
                            type="TABLE",
                        )
                    ]
                )
            }
            fields = [
                SchemaDatasetFacetFields(
                    name=column["Name"],
                    type=column["Type"],
                    description=column.get("Comment"),
                )
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

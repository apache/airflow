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

import time
from typing import TYPE_CHECKING, Any, Literal, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger
from airflow.providers.airbyte.utils.utils import get_field_type, get_streams, resolve_table_schema

if TYPE_CHECKING:
    from openlineage.client.generated.column_lineage_dataset import ColumnLineageDatasetFacet
    from openlineage.client.generated.schema_dataset import SchemaDatasetFacet, SchemaDatasetFacetFields

    from airflow.providers.airbyte.hooks.model import _JobStatistics
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.utils.context import Context


class AirbyteTriggerSyncOperator(BaseOperator):
    """
    Submits a job to an Airbyte server to run a integration process between your source and destination.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AirbyteTriggerSyncOperator`

    :param airbyte_conn_id: Optional. The name of the Airflow connection to get connection
        information for Airbyte. Defaults to "airbyte_default".
    :param connection_id: Required. The Airbyte ConnectionId UUID between a source and destination.
    :param asynchronous: Optional. Flag to get job_id after submitting the job to the Airbyte API.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the AirbyteJobSensor. Defaults to False.
    :param deferrable: Run operator in the deferrable mode.
    :param api_version: Optional. Airbyte API version. Defaults to "v1".
    :param api_type: Optional. The type of Airbyte API to use. Either "config" or "cloud". Defaults to "config".
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``asynchronous`` is False.
        Defaults to 3 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Only used when ``asynchronous`` is False. Defaults to 3600 seconds (or 1 hour).
    """

    template_fields: Sequence[str] = ("connection_id",)
    ui_color = "#6C51FD"

    def __init__(
        self,
        connection_id: str,
        airbyte_conn_id: str = "airbyte_default",
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        api_version: str = "v1",
        api_type: Literal["config", "cloud"] = "config",
        wait_seconds: float = 3,
        timeout: float = 3600,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.connection_id = connection_id
        self.timeout = timeout
        self.api_version = api_version
        self.api_type = api_type
        self.wait_seconds = wait_seconds
        self.asynchronous = asynchronous
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        """Create Airbyte Job and wait to finish."""
        hook = AirbyteHook(
            airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version, api_type=self.api_type
        )
        job_object = hook.submit_sync_connection(connection_id=self.connection_id)
        if self.api_type == "config":
            self.job_id = job_object.json()["job"]["id"]
            state = job_object.json()["job"]["status"]
        else:
            self.job_id = job_object.json()["jobId"]
            state = job_object.json()["status"]
        end_time = time.time() + self.timeout

        self.log.info("Job %s was submitted to Airbyte Server", self.job_id)
        if not self.asynchronous:
            self.log.info("Waiting for job %s to complete", self.job_id)
            if self.deferrable:
                if state in (hook.RUNNING, hook.PENDING, hook.INCOMPLETE):
                    self.defer(
                        timeout=self.execution_timeout,
                        trigger=AirbyteSyncTrigger(
                            conn_id=self.airbyte_conn_id,
                            api_type=self.api_type,
                            job_id=self.job_id,
                            end_time=end_time,
                            poll_interval=60,
                        ),
                        method_name="execute_complete",
                    )
                elif state == hook.SUCCEEDED:
                    self.log.info("Job %s completed successfully", self.job_id)
                    return
                elif state == hook.ERROR:
                    raise AirflowException(f"Job failed:\n{self.job_id}")
                elif state == hook.CANCELLED:
                    raise AirflowException(f"Job was cancelled:\n{self.job_id}")
                else:
                    raise AirflowException(
                        f"Encountered unexpected state `{state}` for job_id `{self.job_id}"
                    )
            else:
                hook.wait_for_job(job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
            self.log.info("Job %s completed successfully", self.job_id)

        return self.job_id

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Invoke this callback when the trigger fires; return immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        return None

    def on_kill(self):
        """Cancel the job if task is cancelled."""
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_type=self.api_type)
        if self.job_id:
            self.log.info("on_kill: cancel the airbyte Job %s", self.job_id)
            hook.cancel_job(self.job_id)

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        from airflow.providers.openlineage.extractors import OperatorLineage

        hook = AirbyteHook(
            airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version, api_type=self.api_type
        )

        # get job data
        jobs_statistics = hook.get_job_statistics(self.job_id)

        connection = hook.get_airbyte_connection_info(self.connection_id)
        if connection is None:
            return OperatorLineage()

        destination_id = connection.get("destinationId", None)
        if destination_id is None:
            self.log.warning("DestinationID is missing in the response")
            return OperatorLineage()

        source_id = connection.get("sourceId", None)
        if source_id is None:
            self.log.warning("SourceID is missing in the response")
            return OperatorLineage()

        destination_response = hook.get_airbyte_destination(destination_id)
        if destination_response is None:
            return OperatorLineage()

        source_response = hook.get_airbyte_source(source_id)
        if source_response is None:
            return OperatorLineage()

        return self.resolve_metadata(
            connection=connection,
            namespace="airbyte",
            destination=destination_response,
            source_response=source_response,
            job_statistics=jobs_statistics,
        )

    def resolve_metadata(
        self,
        connection: dict[str, Any],
        destination: dict[str, Any],
        source_response: dict[str, Any],
        namespace: str,
        job_statistics: _JobStatistics,
    ) -> OperatorLineage:
        """
        Resolve metadata for the OpenLineage lineage.

        It takes information about the connection from airbyte API, currently config API is fully supported,
        due to limited capabilities of Airbyte API. Destination is used only to get the information about the
        destination schema to properly resolve the schema for the output dataset if destination schema has been
        selected.

        :param connection: it's the response from config API about the connection
        :param destination: it's the response from config API about the destination
        :param source_response: it's the response from config API about the source
        :param namespace: it's namespace for the OpenLineage metadata
        :param job_statistics: JobStatistics object
        :return:
        """
        from openlineage.client.generated.base import InputDataset, OutputDataset
        from openlineage.client.generated.external_query_run import ExternalQueryRunFacet
        from openlineage.client.generated.output_statistics_output_dataset import (
            OutputStatisticsOutputDatasetFacet,
        )

        from airflow.providers.openlineage.extractors import OperatorLineage

        ol_schema_resolver = _AirbyteOlSchemaResolver()

        streams = get_streams(connection)

        prefix = connection.get("prefix", "")

        inputs = []
        outputs = []

        for stream_data in streams:
            stream: dict[str, Any] = stream_data.get("stream", {})

            stream_name = stream.get("name", "")

            input_schema = stream.get("namespace", "")

            resolved_schema = resolve_table_schema(
                name_source=connection.get("namespaceDefinition", "source"),
                source=stream,
                destination=destination,
                connection=connection,
            )

            properties = self.get_properties(stream)

            target_table_name = prefix + stream_name

            column_lineage = _ColumnLineageResolver.resolve_column_lineage(
                namespace=namespace,
                schema=resolved_schema,
                stream=properties,
            )

            schema_facet = self.get_schema(ol_schema_resolver, properties)

            outputs.append(
                OutputDataset(
                    namespace=self.resolve_output_namespace(destination),
                    name=f"{resolved_schema}.{target_table_name}"
                    if resolved_schema != ""
                    else target_table_name,
                    facets={
                        "schema": schema_facet,
                        "columnLineage": column_lineage,
                    },
                    outputFacets={
                        "outputStatistics": OutputStatisticsOutputDatasetFacet(
                            rowCount=job_statistics.records_emitted.get(stream_name, 0),
                        ),
                    },
                )
            )

            inputs.append(
                InputDataset(
                    namespace=self.resolve_input_namespace(source_response),
                    name=f"{input_schema}.{stream_name}" if input_schema != "" else stream_name,
                    facets={"schema": schema_facet},
                )
            )

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
            run_facets={
                "externalQuery": ExternalQueryRunFacet(
                    externalQueryId=str(self.job_id),
                    source="airbyte",
                )
            },
        )

    @staticmethod
    def resolve_input_namespace(source: dict[str, Any]) -> str:
        source_name = source.get("sourceName", "").lower()

        configuration = source.get("connectionConfiguration", {})

        host = configuration.get("host", "")
        port = configuration.get("port", "")

        if port:
            return f"{source_name}://{host}:{port}"

        return f"{source_name}://{host}"

    @staticmethod
    def resolve_output_namespace(destination: dict[str, Any]) -> str:
        destination_name = destination.get("destinationName", "").lower()

        configuration = destination.get("connectionConfiguration", {})

        host = configuration.get("host", "")
        port = configuration.get("port", "")

        if port:
            return f"{destination_name}://{host}:{port}"

        return f"{destination_name}://{host}"

    @staticmethod
    def get_schema(
        schema_resolver: _AirbyteOlSchemaResolver, properties: dict[str, Any]
    ) -> SchemaDatasetFacet:
        from openlineage.client.generated.schema_dataset import SchemaDatasetFacet

        schema = schema_resolver.map_airbyte_type_to_ol_schema_field(properties)

        schema_facet = SchemaDatasetFacet(fields=schema)

        return schema_facet

    @staticmethod
    def get_properties(stream: dict[str, Any]) -> dict[str, Any]:
        json_schema = stream.get("jsonSchema", {})

        properties: dict[str, Any] = json_schema.get("properties")

        return properties


class _AirbyteOlSchemaResolver:
    def map_airbyte_type_to_ol_schema_field(self, stream: dict[str, Any]) -> list[SchemaDatasetFacetFields]:
        from openlineage.client.generated.schema_dataset import SchemaDatasetFacetFields

        schema = []

        for field_name, field_metadata in stream.items():
            airbyte_target_field_type = get_field_type(field_metadata)
            if airbyte_target_field_type == "":
                continue

            if airbyte_target_field_type == "object":
                schema.append(
                    SchemaDatasetFacetFields(
                        name=field_name,
                        type="struct",
                        fields=self.map_airbyte_type_to_ol_schema_field(field_metadata.get("properties")),
                    )
                )
                continue

            if airbyte_target_field_type == "array":
                schema.append(self.map_array_type_to_ol_schema_field(field_metadata, field_name))
                continue

            if airbyte_target_field_type in ["string", "int", "float", "boolean", "date", "time"]:
                schema.append(SchemaDatasetFacetFields(name=field_name, type=airbyte_target_field_type))
                continue

        return schema

    def map_array_type_to_ol_schema_field(
        self, field_metadata: dict[str, Any], field_name: str
    ) -> SchemaDatasetFacetFields:
        from openlineage.client.generated.schema_dataset import SchemaDatasetFacetFields

        items = field_metadata.get("items", {})
        items_type = items.get("type", None)
        items_format = items.get("format", None)

        field_type = get_field_type(items)

        if field_type == "object":
            return SchemaDatasetFacetFields(
                name=field_name,
                type="array",
                fields=self.map_airbyte_type_to_ol_schema_field(
                    {
                        "_element": {
                            "name": "_element",
                            "type": [
                                get_field_type({"type": items_type, "format": items_format}),
                            ],
                            "properties": items.get("properties", []),
                        }
                    }
                ),
            )

        if field_type == "array":
            return SchemaDatasetFacetFields(
                name=field_name,
                type="array",
                fields=self.map_airbyte_type_to_ol_schema_field(
                    {
                        "_element": {
                            "name": "_element",
                            "type": ["array"],
                            "items": items.get("items", []),
                        }
                    }
                ),
            )

        return SchemaDatasetFacetFields(
            name=field_name,
            type="array",
            fields=[SchemaDatasetFacetFields(name="_element", type=field_type)],
        )


class _ColumnLineageResolver:
    @staticmethod
    def resolve_column_lineage(
        namespace: str, schema: str | None, stream: dict[str, Any]
    ) -> ColumnLineageDatasetFacet:
        from openlineage.client.generated.column_lineage_dataset import (
            ColumnLineageDatasetFacet,
            Fields,
            InputField,
        )

        fields = {}

        for field_name in stream.keys():
            fields[field_name] = Fields(
                inputFields=[
                    InputField(
                        namespace=namespace,
                        name=schema if schema is not None else "",
                        field=field_name,
                    )
                ],
                transformationType="IDENTITY",
                transformationDescription="",
            )

        return ColumnLineageDatasetFacet(fields=fields)

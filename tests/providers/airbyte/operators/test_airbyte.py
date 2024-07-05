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

from collections import defaultdict
from unittest import mock

from openlineage.client.generated.schema_dataset import SchemaDatasetFacetFields

from airflow.providers.airbyte.hooks.airbyte import JobStatistics
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from tests.providers.airbyte.operators.connection import (
    connection_flat,
    connection_nested,
    streams_are_missing,
    sync_catalog_is_missing,
)
from tests.providers.airbyte.operators.destination import destination_snowflake
from tests.providers.airbyte.operators.source import source_postgresql


class TestAirbyteTriggerSyncOp:
    """
    Test execute function from Airbyte Operator
    """

    airbyte_conn_id = "test_airbyte_conn_id"
    connection_id = "test_airbyte_connection"
    job_id = 1
    wait_seconds = 0
    timeout = 360

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.submit_sync_connection")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.wait_for_job", return_value=None)
    def test_execute(self, mock_wait_for_job, mock_submit_sync_connection):
        mock_submit_sync_connection.return_value = mock.Mock(
            **{"json.return_value": {"job": {"id": self.job_id, "status": "running"}}}
        )

        op = AirbyteTriggerSyncOperator(
            task_id="test_Airbyte_op",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )
        op.execute({})

        mock_submit_sync_connection.assert_called_once_with(connection_id=self.connection_id)
        mock_wait_for_job.assert_called_once_with(
            job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout
        )

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_source")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_statistics")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_destination")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_connection_info")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.wait_for_job", return_value=None)
    def test_send_metadata_to_open_lineage_target_nested_schema(
        self,
        mock_wait_for_job,
        mock_get_airbyte_connection_info,
        mock_get_airbyte_destination,
        mock_get_job_statistics,
        mock_get_airbyte_source,
    ):
        connection = connection_nested

        destination = destination_snowflake

        source = source_postgresql

        mock_get_airbyte_connection_info.return_value = connection

        mock_get_airbyte_destination.return_value = destination

        mock_get_airbyte_source.return_value = source

        mock_get_job_statistics.return_value = JobStatistics(
            number_of_attempts=1,
            records_emitted={"pokemon": 1},
        )

        op = AirbyteTriggerSyncOperator(
            task_id="test_airbyte_open_lineage",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

        op.job_id = 10

        result = op.get_openlineage_facets_on_complete(None)
        assert 1 == len(result.inputs)
        assert 1 == len(result.outputs)

        inputs = result.inputs[0]
        outputs = result.outputs[0]

        assert "postgres://host.docker.internal:5439" == inputs.namespace
        assert "pokemon" == inputs.name
        assert "public.pokemon" == outputs.name
        assert "snowflake://localhost" == outputs.namespace

        schema = inputs.facets["schema"]
        input_fields = schema.fields

        assert input_fields == [
            SchemaDatasetFacetFields(name="order", type="int", description=None, fields=[]),
            SchemaDatasetFacetFields(name="weight", type="int", description=None, fields=[]),
            SchemaDatasetFacetFields(
                name="species",
                type="struct",
                description=None,
                fields=[
                    SchemaDatasetFacetFields(name="url", type="string", description=None, fields=[]),
                    SchemaDatasetFacetFields(name="name", type="string", description=None, fields=[]),
                ],
            ),
            SchemaDatasetFacetFields(name="is_default", type="boolean", description=None, fields=[]),
            SchemaDatasetFacetFields(
                name="past_types",
                type="array",
                description=None,
                fields=[
                    SchemaDatasetFacetFields(
                        name="_element",
                        type="struct",
                        description=None,
                        fields=[
                            SchemaDatasetFacetFields(
                                name="types",
                                type="array",
                                description=None,
                                fields=[
                                    SchemaDatasetFacetFields(
                                        name="_element",
                                        type="struct",
                                        description=None,
                                        fields=[
                                            SchemaDatasetFacetFields(
                                                name="slot", type="int", description=None, fields=[]
                                            ),
                                            SchemaDatasetFacetFields(
                                                name="type",
                                                type="struct",
                                                description=None,
                                                fields=[
                                                    SchemaDatasetFacetFields(
                                                        name="url", type="string", description=None, fields=[]
                                                    ),
                                                    SchemaDatasetFacetFields(
                                                        name="name",
                                                        type="string",
                                                        description=None,
                                                        fields=[],
                                                    ),
                                                ],
                                            ),
                                        ],
                                    )
                                ],
                            ),
                            SchemaDatasetFacetFields(
                                name="generation",
                                type="struct",
                                description=None,
                                fields=[
                                    SchemaDatasetFacetFields(
                                        name="url", type="string", description=None, fields=[]
                                    ),
                                    SchemaDatasetFacetFields(
                                        name="name", type="string", description=None, fields=[]
                                    ),
                                ],
                            ),
                        ],
                    )
                ],
            ),
            SchemaDatasetFacetFields(name="base_experience", type="int", description=None, fields=[]),
            SchemaDatasetFacetFields(
                name="location_area_encounters", type="string", description=None, fields=[]
            ),
        ]

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_source")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_statistics")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_destination")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_connection_info")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.wait_for_job", return_value=None)
    def test_send_metadata_to_open_lineage_target_flat_schema_multiple_sources(
        self,
        mock_wait_for_job,
        mock_get_airbyte_connection_info,
        mock_get_airbyte_destination,
        mock_get_job_statistics,
        mock_get_airbyte_source,
    ):
        destination = destination_snowflake

        mock_get_airbyte_connection_info.status_code = mock.Mock(status_code=200)

        mock_get_airbyte_connection_info.return_value = connection_flat

        source = source_postgresql

        mock_get_airbyte_source.return_value = source

        mock_get_airbyte_destination.return_value = mock.Mock(
            **{"json.return_value": destination, "status_code": 200}
        )

        mock_get_job_statistics.return_value = JobStatistics(
            number_of_attempts=1,
            records_emitted={"vehicle": 100},
        )

        op = AirbyteTriggerSyncOperator(
            task_id="test_airbyte_open_lineage",
            airbyte_conn_id=self.airbyte_conn_id,
            connection_id=self.connection_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

        op.job_id = 10

        result = op.get_openlineage_facets_on_complete(None)
        assert 2 == len(result.inputs)
        assert 2 == len(result.outputs)

        input_fields = defaultdict(dict)
        output_fields = defaultdict(dict)

        for index, input in enumerate(result.inputs):
            input_fields[input.name]["schema"] = input.facets["schema"].fields
            input_fields[input.name]["namespace"] = input.namespace
            input_fields[input.name]["name"] = input.name

            output = result.outputs[index]

            output_fields[output.name]["schema"] = output.facets["schema"].fields
            output_fields[output.name]["namespace"] = output.namespace
            output_fields[output.name]["name"] = output.name
            output_fields[output.name]["outputStatistics"] = output.outputFacets["outputStatistics"]

        assert input_fields["public.vehicle"]["schema"] == [
            SchemaDatasetFacetFields(name="id", type="string", description=None, fields=[]),
            SchemaDatasetFacetFields(name="number", type="float", description=None, fields=[]),
        ]

        assert output_fields["new_schema.sync_vehicle"]["schema"] == [
            SchemaDatasetFacetFields(name="id", type="string", description=None, fields=[]),
            SchemaDatasetFacetFields(name="number", type="float", description=None, fields=[]),
        ]

        assert output_fields["new_schema.sync_vehicle"]["outputStatistics"].rowCount == 100
        assert input_fields["public.location"]["schema"] == [
            SchemaDatasetFacetFields(name="id", type="string", description=None, fields=[]),
            SchemaDatasetFacetFields(name="lat", type="float", description=None, fields=[]),
            SchemaDatasetFacetFields(name="lon", type="float", description=None, fields=[]),
            SchemaDatasetFacetFields(name="name", type="string", description=None, fields=[]),
        ]

        assert output_fields["new_schema.sync_location"]["schema"] == [
            SchemaDatasetFacetFields(name="id", type="string", description=None, fields=[]),
            SchemaDatasetFacetFields(name="lat", type="float", description=None, fields=[]),
            SchemaDatasetFacetFields(name="lon", type="float", description=None, fields=[]),
            SchemaDatasetFacetFields(name="name", type="string", description=None, fields=[]),
        ]

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_source")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_statistics")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_destination")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_airbyte_connection_info")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.wait_for_job", return_value=None)
    def test_invalid_data_input(
        self,
        mock_wait_for_job,
        mock_get_airbyte_connection_info,
        mock_get_airbyte_destination,
        mock_get_job_statistics,
        mock_get_airbyte_source,
    ):
        test_cases = {
            "sync catalog is missing": {
                "connection": sync_catalog_is_missing,
                "expected_inputs": [],
                "expected_outputs": [],
            },
            "streams_is_missing": {
                "connection": streams_are_missing,
                "expected_inputs": [],
                "expected_outputs": [],
            },
        }

        for test_case in test_cases.values():
            destination = destination_snowflake
            source = source_postgresql

            mock_get_airbyte_source.return_value = source

            mock_get_airbyte_connection_info.status_code = mock.Mock(status_code=200)

            mock_get_job_statistics.return_value = JobStatistics(
                number_of_attempts=1,
                records_emitted={"pokemon": 1},
            )

            mock_get_airbyte_connection_info.return_value = test_case["connection"]

            mock_get_airbyte_destination.return_value = mock.Mock(
                **{"json.return_value": destination, "status_code": 200}
            )

            op = AirbyteTriggerSyncOperator(
                task_id="test_airbyte_open_lineage",
                airbyte_conn_id=self.airbyte_conn_id,
                connection_id=self.connection_id,
                wait_seconds=self.wait_seconds,
                timeout=self.timeout,
            )

            op.job_id = 10

            result = op.get_openlineage_facets_on_complete(None)

            assert result.inputs == test_case["expected_inputs"]
            assert result.outputs == test_case["expected_outputs"]

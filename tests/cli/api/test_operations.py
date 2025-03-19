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

import datetime
import json
from contextlib import redirect_stdout
from io import StringIO

import httpx
import pytest

from airflow.cli.api.client import Client
from airflow.cli.api.datamodels._generated import (
    AssetAliasCollectionResponse,
    AssetAliasResponse,
    AssetCollectionResponse,
    AssetResponse,
    BackfillPostBody,
    BackfillResponse,
    BulkAction,
    BulkActionOnExistence,
    Config,
    ConfigOption,
    ConfigSection,
    ConnectionBody,
    ConnectionBulkActionResponse,
    ConnectionBulkBody,
    ConnectionBulkCreateAction,
    ConnectionCollectionResponse,
    ConnectionResponse,
    ConnectionTestResponse,
    DAGDetailsResponse,
    DAGResponse,
    DAGRunCollectionResponse,
    DAGRunResponse,
    DagRunState,
    DagRunTriggeredByType,
    DagRunType,
    JobCollectionResponse,
    JobResponse,
    PoolBulkActionResponse,
    PoolBulkBody,
    PoolBulkCreateAction,
    PoolCollectionResponse,
    PoolPostBody,
    PoolResponse,
    ProviderCollectionResponse,
    ProviderResponse,
    ReprocessBehavior,
    TriggerDAGRunPostBody,
    VariableBody,
    VariableBulkActionResponse,
    VariableBulkBody,
    VariableBulkCreateAction,
    VariableCollectionResponse,
    VariableResponse,
    VersionInfo,
)
from airflow.cli.api.operations import SERVER_CONNECTION_REFUSED_ERROR


def make_cli_api_client(
    transport: httpx.MockTransport | None = None, base_url: str = "test://server"
) -> Client:
    """Get a client with a custom transport"""
    return Client(base_url=base_url, transport=transport)


class TestBaseOperations:
    def test_server_connection_refused(self):
        client = make_cli_api_client(base_url="http://localhost")
        with (
            pytest.raises(SystemExit),
            pytest.raises(httpx.ConnectError),
            redirect_stdout(StringIO()) as stdout,
        ):
            client.connections.get(1)
        stdout = stdout.getvalue()
        assert SERVER_CONNECTION_REFUSED_ERROR in stdout


class TestAssetsOperations:
    asset_id: int = 1
    asset_response = AssetResponse(
        id=asset_id,
        name="asset",
        uri="asset_uri",
        extra={"extra": "extra"},
        created_at=datetime.datetime(2024, 12, 31, 23, 59, 59),
        updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        consuming_dags=[],
        producing_tasks=[],
        aliases=[],
        group="group",
    )
    asset_alias_response = AssetAliasResponse(
        id=asset_id,
        name="asset",
        group="group",
    )

    def test_get_asset(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/assets/{self.asset_id}"
            return httpx.Response(200, json=json.loads(self.asset_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.get(self.asset_id)
        assert response == self.asset_response

    def test_get_by_alias(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/assets/aliases/{self.asset_id}"
            return httpx.Response(200, json=json.loads(self.asset_alias_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.get_by_alias(self.asset_id)
        assert response == self.asset_alias_response

    def test_list(self):
        assets_collection_response = AssetCollectionResponse(
            assets=[self.asset_response],
            total_entries=1,
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/assets"
            return httpx.Response(200, json=json.loads(assets_collection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.list()
        assert response == assets_collection_response

    def test_list_by_alias(self):
        assets_collection_response = AssetAliasCollectionResponse(
            asset_aliases=[self.asset_alias_response],
            total_entries=1,
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/assets/aliases"
            return httpx.Response(200, json=json.loads(assets_collection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.list_by_alias()
        assert response == assets_collection_response


class TestBackfillOperations:
    backfill_id: int = 1

    def test_create(self):
        backfill_body = BackfillPostBody(
            dag_id="dag_id",
            from_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
            to_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
            run_backwards=False,
            dag_run_conf={},
            reprocess_behavior=ReprocessBehavior.COMPLETED,
            max_active_runs=1,
        )
        backfill_response = BackfillResponse(
            id=self.backfill_id,
            dag_id="dag_id",
            from_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
            to_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
            dag_run_conf={},
            is_paused=False,
            reprocess_behavior=ReprocessBehavior.COMPLETED,
            max_active_runs=1,
            created_at=datetime.datetime(2024, 12, 31, 23, 59, 59),
            completed_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
            updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/backfills"
            return httpx.Response(200, json=json.loads(backfill_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.create(backfill=backfill_body)
        assert response == backfill_response


class TestConfigOperations:
    section: str = "core"
    option: str = "config"

    def test_get(self):
        response_config = Config(
            sections=[
                ConfigSection(
                    name=self.section,
                    options=[
                        ConfigOption(
                            key=self.option,
                            value="config",
                        )
                    ],
                )
            ]
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/section/{self.section}/option/{self.option}"
            return httpx.Response(200, json=response_config.model_dump())

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.configs.get(section=self.section, option=self.option)
        assert response == response_config


class TestConnectionsOperations:
    connection_id: str = "test_connection"
    conn_type: str = "conn_type"
    host: str = "host"
    schema_: str = "schema"
    login: str = "login"
    password: str = "password"
    port: int = 1
    extra: str = json.dumps({"extra": "extra"})
    connection = ConnectionBody(
        connection_id=connection_id,
        conn_type=conn_type,
        host=host,
        schema_=schema_,
        login=login,
        password=password,
        port=port,
        extra=extra,
    )

    connection_response = ConnectionResponse(
        connection_id=connection_id,
        conn_type=conn_type,
        host=host,
        schema_=schema_,
        login=login,
        password=password,
        port=port,
        extra=extra,
    )

    connections_response = ConnectionCollectionResponse(
        connections=[connection_response],
        total_entries=1,
    )

    connection_bulk_body = ConnectionBulkBody(
        actions=[
            ConnectionBulkCreateAction(
                action=BulkAction.CREATE,
                connections=[connection],
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )

    connection_bulk_action_response = ConnectionBulkActionResponse(
        success=[connection_id],
        errors=[],
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/connections/{self.connection_id}"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.get(self.connection_id)
        assert response == self.connection_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/connections"
            return httpx.Response(200, json=json.loads(self.connections_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.list()
        assert response == self.connections_response

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/connections"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.create(connection=self.connection)
        assert response == self.connection_response

    def test_bulk(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/connections"
            return httpx.Response(
                200, json=json.loads(self.connection_bulk_action_response.model_dump_json())
            )

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.bulk(connections=self.connection_bulk_body)
        assert response == self.connection_bulk_action_response

    def test_delete(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/connections/{self.connection_id}"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.delete(self.connection_id)
        assert response == self.connection_id

    def test_update(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/connections/{self.connection_id}"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.update(connection=self.connection)
        assert response == self.connection_response

    def test_test(self):
        connection_test_response = ConnectionTestResponse(
            status=True,
            message="message",
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/connections/test"
            return httpx.Response(200, json=json.loads(connection_test_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.test(connection=self.connection)
        assert response == connection_test_response


class TestDagOperations:
    dag_response = DAGResponse(
        dag_id="dag_id",
        dag_display_name="dag_display_name",
        is_paused=False,
        is_active=True,
        last_parsed_time=datetime.datetime(2024, 12, 31, 23, 59, 59),
        last_expired=datetime.datetime(2025, 1, 1, 0, 0, 0),
        default_view="grid",
        fileloc="fileloc",
        description="description",
        timetable_summary="timetable_summary",
        timetable_description="timetable_description",
        tags=[],
        max_active_tasks=1,
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
        has_task_concurrency_limits=True,
        has_import_errors=True,
        next_dagrun=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_create_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        owners=["apache-airflow"],
        file_token="file_token",
    )

    dag_details_response = DAGDetailsResponse(
        dag_id="dag_id",
        dag_display_name="dag_display_name",
        is_paused=False,
        is_active=True,
        last_parsed_time=datetime.datetime(2024, 12, 31, 23, 59, 59),
        last_expired=datetime.datetime(2025, 1, 1, 0, 0, 0),
        default_view="grid",
        fileloc="fileloc",
        description="description",
        timetable_summary="timetable_summary",
        timetable_description="timetable_description",
        tags=[],
        max_active_tasks=1,
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
        has_task_concurrency_limits=True,
        has_import_errors=True,
        next_dagrun=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_create_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        owners=["apache-airflow"],
        catchup=False,
        dag_run_timeout=datetime.timedelta(days=1),
        asset_expression=None,
        doc_md=None,
        start_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        is_paused_upon_creation=False,
        params={},
        render_template_as_native_obj=True,
        template_search_path=[],
        timezone="timezone",
        last_parsed=datetime.datetime(2024, 12, 31, 23, 59, 59),
        file_token="file_token",
        concurrency=1,
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/dags/dag_id"
            return httpx.Response(200, json=json.loads(self.dag_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get("dag_id")
        assert response == self.dag_response

    def test_get_details(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/dags/dag_id/details"
            return httpx.Response(200, json=json.loads(self.dag_details_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get_details("dag_id")
        assert response == self.dag_details_response


class TestDagRunOperations:
    dag_id = "dag_id"
    dag_run_id = "dag_run_id"
    dag_run_response = DAGRunResponse(
        dag_run_id=dag_run_id,
        dag_id=dag_id,
        logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        queued_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        start_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        last_scheduling_decision=datetime.datetime(2025, 1, 1, 0, 0, 0),
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        external_trigger=True,
        triggered_by=DagRunTriggeredByType.UI,
        conf={},
        note=None,
    )

    dag_run_collection_response = DAGRunCollectionResponse(
        dag_runs=[dag_run_response],
        total_entries=1,
    )

    trigger_dag_run = TriggerDAGRunPostBody(
        dag_run_id=dag_run_id,
        data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        conf={},
        note="note",
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/dag_runs/{self.dag_run_id}"
            return httpx.Response(200, json=json.loads(self.dag_run_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dag_runs.get(dag_run_id=self.dag_run_id)
        assert response == self.dag_run_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/dag_runs"
            return httpx.Response(200, json=json.loads(self.dag_run_collection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dag_runs.list(
            dag_id=self.dag_id,
            start_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
            end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
            state=DagRunState.RUNNING,
            limit=1,
        )
        assert response == self.dag_run_collection_response

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/dag_runs/{self.dag_id}"
            return httpx.Response(200, json=json.loads(self.dag_run_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dag_runs.create(dag_id=self.dag_id, trigger_dag_run=self.trigger_dag_run)
        assert response == self.dag_run_response


class TestJobsOperations:
    job_response = JobResponse(
        id=1,
        dag_id="dag_id",
        state="state",
        job_type="job_type",
        start_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        latest_heartbeat=datetime.datetime(2025, 1, 1, 0, 0, 0),
        executor_class="LocalExecutor",
        hostname="hostname",
        unixname="unixname",
    )

    job_collection_response = JobCollectionResponse(
        jobs=[job_response],
        total_entries=1,
    )

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/jobs"
            return httpx.Response(200, json=json.loads(self.job_collection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.jobs.list(
            job_type="job_type",
            hostname="hostname",
            limit=1,
            is_alive=True,
        )
        assert response == self.job_collection_response


class TestPoolsOperations:
    pool_name = "pool_name"
    pool = PoolPostBody(
        name=pool_name,
        slots=1,
        description="description",
        include_deferred=True,
    )
    pools_bulk_body = PoolBulkBody(
        actions=[
            PoolBulkCreateAction(
                action=BulkAction.CREATE,
                pools=[pool],
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )
    pool_response = PoolResponse(
        name=pool_name,
        slots=1,
        description="description",
        include_deferred=True,
        occupied_slots=1,
        running_slots=1,
        queued_slots=1,
        scheduled_slots=1,
        open_slots=1,
        deferred_slots=1,
    )
    pool_response_collection = PoolCollectionResponse(
        pools=[pool_response],
        total_entries=1,
    )
    pool_bulk_action_response = PoolBulkActionResponse(
        success=[pool_name],
        errors=[],
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/pools/{self.pool_name}"
            return httpx.Response(200, json=json.loads(self.pool_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.get(self.pool_name)
        assert response == self.pool_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/pools"
            return httpx.Response(200, json=json.loads(self.pool_response_collection.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.list()
        assert response == self.pool_response_collection

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/pools"
            return httpx.Response(200, json=json.loads(self.pool_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.create(pool=self.pool)
        assert response == self.pool_response

    def test_bulk(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/pools"
            return httpx.Response(200, json=json.loads(self.pool_bulk_action_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.bulk(pools=self.pools_bulk_body)
        assert response == self.pool_bulk_action_response

    def test_delete(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/pools/{self.pool_name}"
            return httpx.Response(200, json=json.loads(self.pool_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.delete(self.pool_name)
        assert response == self.pool_name


class TestProvidersOperations:
    provider_response = ProviderResponse(
        package_name="package_name",
        version="version",
        description="description",
    )
    provider_collection_response = ProviderCollectionResponse(
        providers=[provider_response],
        total_entries=1,
    )

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/providers"
            return httpx.Response(200, json=json.loads(self.provider_collection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.providers.list()
        assert response == self.provider_collection_response


class TestVariablesOperations:
    key = "key"
    value = "val"
    description = "description"
    variable = VariableBody(
        key=key,
        value=value,
        description=description,
    )
    variable_response = VariableResponse(
        key=key,
        value=value,
        description=description,
        is_encrypted=False,
    )
    variable_collection_response = VariableCollectionResponse(
        variables=[variable_response],
        total_entries=1,
    )
    variable_bulk = VariableBulkBody(
        actions=[
            VariableBulkCreateAction(
                action=BulkAction.CREATE,
                variables=[variable],
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )
    variable_bulk_response = VariableBulkActionResponse(
        success=[key],
        errors=[],
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/variables/{self.key}"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.get(self.key)
        assert response == self.variable_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/variables"
            return httpx.Response(200, json=json.loads(self.variable_collection_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.list()
        assert response == self.variable_collection_response

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/variables"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.create(variable=self.variable)
        assert response == self.variable_response

    def test_bulk(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/variables"
            return httpx.Response(200, json=json.loads(self.variable_bulk_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.bulk(variables=self.variable_bulk)
        assert response == self.variable_bulk_response

    def test_delete(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/variables/{self.key}"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.delete(self.key)
        assert response == self.key

    def test_update(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/public/variables/{self.key}"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.update(variable=self.variable)
        assert response == self.variable_response


class TestVersionOperations:
    version_info = VersionInfo(
        version="version",
        git_version="git_version",
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/public/version"
            return httpx.Response(200, json=json.loads(self.version_info.model_dump_json()))

        client = make_cli_api_client(transport=httpx.MockTransport(handle_request))
        response = client.version.get()
        assert response == self.version_info

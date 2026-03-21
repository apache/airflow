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

import httpx
import pytest

from airflow.api.client import get_current_api_client, get_local_rest_client
from airflow.api.client.local_rest_client import LocalRESTClient
from airflow.models import Connection
from airflow.models.asset import AssetActive, AssetModel
from airflow.models.pool import Pool
from airflow.models.variable import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_connections,
    clear_db_dags,
    clear_db_pools,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_variables,
)

pytestmark = pytest.mark.db_test


@provide_session
def _create_pool(
    name: str, slots: int, description: str | None = None, include_deferred: bool = False, session=None
) -> None:
    pool = Pool(pool=name, slots=slots, description=description, include_deferred=include_deferred)
    session.add(pool)


class TestLocalRESTClient:
    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_pools()
        yield
        clear_db_pools()

    def test_instantiation(self):
        client = LocalRESTClient(process_type="dag_processor")
        assert client._process_type == "dag_processor"

    def test_default_process_type(self):
        client = LocalRESTClient()
        assert client._process_type == "unknown"

    def test_close_without_use(self):
        client = LocalRESTClient()
        client.close()

    def test_close_after_use(self):
        client = get_local_rest_client(process_type="test")
        # Access _http to force creation
        _ = client._http
        client.close()


class TestLocalRESTClientFactory:
    def test_get_local_rest_client_returns_instance(self):
        client = get_local_rest_client()
        assert isinstance(client, LocalRESTClient)

    def test_get_local_rest_client_with_process_type(self):
        client = get_local_rest_client(process_type="scheduler")
        assert client._process_type == "scheduler"

    def test_get_current_api_client_deprecation_warning(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            get_current_api_client()
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()
            assert "get_local_rest_client" in str(w[0].message)


class TestPoolsClientViaLocalREST:
    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_pools()
        yield
        clear_db_pools()

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    def test_create_pool(self, client):
        result = client.pools.create(name="test_pool", slots=5)
        assert result["name"] == "test_pool"
        assert result["slots"] == 5
        assert result["include_deferred"] is False

    def test_create_pool_with_description(self, client):
        result = client.pools.create(name="described_pool", slots=3, description="A test pool")
        assert result["name"] == "described_pool"
        assert result["description"] == "A test pool"

    def test_create_pool_with_include_deferred(self, client):
        result = client.pools.create(name="deferred_pool", slots=10, include_deferred=True)
        assert result["include_deferred"] is True

    def test_get_pool(self, client):
        _create_pool(name="my_pool", slots=7, description="desc")
        result = client.pools.get("my_pool")
        assert result["name"] == "my_pool"
        assert result["slots"] == 7
        assert result["description"] == "desc"

    def test_get_pool_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.pools.get("nonexistent")
        assert exc_info.value.response.status_code == 404

    def test_list_pools(self, client):
        _create_pool(name="pool_a", slots=1)
        _create_pool(name="pool_b", slots=2)
        result = client.pools.list()
        # default_pool + pool_a + pool_b
        assert result["total_entries"] >= 3
        pool_names = [p["name"] for p in result["pools"]]
        assert "pool_a" in pool_names
        assert "pool_b" in pool_names

    def test_list_pools_with_pagination(self, client):
        for i in range(5):
            _create_pool(name=f"paginated_{i}", slots=i + 1)
        result = client.pools.list(limit=2, offset=0)
        assert len(result["pools"]) == 2

    def test_update_pool(self, client):
        _create_pool(name="updatable", slots=3)
        result = client.pools.update("updatable", slots=10)
        assert result["name"] == "updatable"
        assert result["slots"] == 10

    def test_update_pool_description(self, client):
        _create_pool(name="updatable2", slots=5)
        result = client.pools.update("updatable2", description="new desc")
        assert result["description"] == "new desc"

    def test_delete_pool(self, client):
        _create_pool(name="deletable", slots=1)
        client.pools.delete("deletable")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.pools.get("deletable")
        assert exc_info.value.response.status_code == 404

    def test_delete_pool_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.pools.delete("nonexistent")
        assert exc_info.value.response.status_code == 404

    def test_delete_default_pool_rejected(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.pools.delete("default_pool")
        assert exc_info.value.response.status_code == 400

    def test_create_duplicate_pool(self, client):
        client.pools.create(name="dup_pool", slots=2)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.pools.create(name="dup_pool", slots=3)
        assert exc_info.value.response.status_code == 409


class TestConnectionsClientViaLocalREST:
    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_connections(add_default_connections_back=False)
        yield
        clear_db_connections(add_default_connections_back=False)

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    def test_create_connection(self, client):
        result = client.connections.create(connection_id="my_conn", conn_type="http")
        assert result["connection_id"] == "my_conn"
        assert result["conn_type"] == "http"

    def test_create_connection_with_all_fields(self, client):
        result = client.connections.create(
            connection_id="full_conn",
            conn_type="postgres",
            description="A test connection",
            host="localhost",
            login="admin",
            schema="mydb",
            port=5432,
            extra='{"sslmode": "require"}',
        )
        assert result["connection_id"] == "full_conn"
        assert result["conn_type"] == "postgres"
        assert result["description"] == "A test connection"
        assert result["host"] == "localhost"
        assert result["login"] == "admin"
        assert result["schema"] == "mydb"
        assert result["port"] == 5432

    @provide_session
    def _create_connection(self, conn_id, conn_type="http", host=None, session=None):
        conn = Connection(conn_id=conn_id, conn_type=conn_type, host=host)
        session.add(conn)

    def test_get_connection(self, client):
        self._create_connection("test_conn", conn_type="mysql", host="db.example.com")
        result = client.connections.get("test_conn")
        assert result["connection_id"] == "test_conn"
        assert result["conn_type"] == "mysql"
        assert result["host"] == "db.example.com"

    def test_get_connection_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.connections.get("nonexistent")
        assert exc_info.value.response.status_code == 404

    def test_list_connections(self, client):
        self._create_connection("conn_a")
        self._create_connection("conn_b")
        result = client.connections.list()
        assert result["total_entries"] >= 2
        conn_ids = [c["connection_id"] for c in result["connections"]]
        assert "conn_a" in conn_ids
        assert "conn_b" in conn_ids

    def test_list_connections_with_pagination(self, client):
        for i in range(5):
            self._create_connection(f"paginated_{i}")
        result = client.connections.list(limit=2, offset=0)
        assert len(result["connections"]) == 2

    def test_update_connection(self, client):
        self._create_connection("updatable_conn", conn_type="http")
        result = client.connections.update("updatable_conn", host="new-host.example.com")
        assert result["connection_id"] == "updatable_conn"
        assert result["host"] == "new-host.example.com"

    def test_delete_connection(self, client):
        self._create_connection("deletable_conn")
        client.connections.delete("deletable_conn")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.connections.get("deletable_conn")
        assert exc_info.value.response.status_code == 404

    def test_delete_connection_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.connections.delete("nonexistent")
        assert exc_info.value.response.status_code == 404

    def test_create_duplicate_connection(self, client):
        client.connections.create(connection_id="dup_conn", conn_type="http")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.connections.create(connection_id="dup_conn", conn_type="http")
        assert exc_info.value.response.status_code == 409


class TestVariablesClientViaLocalREST:
    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_variables()
        yield
        clear_db_variables()

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    def test_create_variable(self, client):
        result = client.variables.create(key="my_var", value="my_value")
        assert result["key"] == "my_var"

    def test_create_variable_with_description(self, client):
        result = client.variables.create(key="desc_var", value="val", description="A test variable")
        assert result["key"] == "desc_var"
        assert result["description"] == "A test variable"

    def test_get_variable(self, client):
        Variable.set(key="test_var", value="test_value", description="desc")
        result = client.variables.get("test_var")
        assert result["key"] == "test_var"

    def test_get_variable_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.variables.get("nonexistent")
        assert exc_info.value.response.status_code == 404

    def test_list_variables(self, client):
        Variable.set(key="var_a", value="a")
        Variable.set(key="var_b", value="b")
        result = client.variables.list()
        assert result["total_entries"] >= 2
        keys = [v["key"] for v in result["variables"]]
        assert "var_a" in keys
        assert "var_b" in keys

    def test_list_variables_with_pagination(self, client):
        for i in range(5):
            Variable.set(key=f"paginated_{i}", value=str(i))
        result = client.variables.list(limit=2, offset=0)
        assert len(result["variables"]) == 2

    def test_update_variable_value(self, client):
        Variable.set(key="updatable", value="old")
        result = client.variables.update("updatable", value="new")
        assert result["key"] == "updatable"

    def test_update_variable_description(self, client):
        Variable.set(key="updatable2", value="val")
        result = client.variables.update("updatable2", description="new desc")
        assert result["key"] == "updatable2"
        assert result["description"] == "new desc"

    def test_delete_variable(self, client):
        Variable.set(key="deletable", value="val")
        client.variables.delete("deletable")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.variables.get("deletable")
        assert exc_info.value.response.status_code == 404

    def test_delete_variable_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.variables.delete("nonexistent")
        assert exc_info.value.response.status_code == 404

    def test_create_duplicate_variable(self, client):
        client.variables.create(key="dup_var", value="val")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.variables.create(key="dup_var", value="val2")
        assert exc_info.value.response.status_code == 409


class TestDagsClientViaLocalREST:
    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup(self):
        self._clear_db()
        yield
        self._clear_db()

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    def test_get_dag(self, client, dag_maker):
        with dag_maker("test_dag", schedule=None):
            EmptyOperator(task_id="task1")
        result = client.dags.get("test_dag")
        assert result["dag_id"] == "test_dag"

    def test_get_dag_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.dags.get("nonexistent")
        assert exc_info.value.response.status_code == 404

    def test_list_dags(self, client, dag_maker):
        with dag_maker("dag_alpha", schedule=None):
            EmptyOperator(task_id="t")
        with dag_maker("dag_beta", schedule=None):
            EmptyOperator(task_id="t")
        result = client.dags.list()
        dag_ids = [d["dag_id"] for d in result["dags"]]
        assert "dag_alpha" in dag_ids
        assert "dag_beta" in dag_ids

    def test_pause_dag(self, client, dag_maker):
        with dag_maker("pausable_dag", schedule=None):
            EmptyOperator(task_id="t")
        result = client.dags.pause("pausable_dag")
        assert result["is_paused"] is True

    def test_unpause_dag(self, client, dag_maker):
        with dag_maker("unpausable_dag", schedule=None):
            EmptyOperator(task_id="t")
        # Ensure it's paused first
        client.dags.pause("unpausable_dag")
        result = client.dags.unpause("unpausable_dag")
        assert result["is_paused"] is False

    def test_delete_dag(self, client, dag_maker):
        with dag_maker("deletable_dag", schedule=None):
            EmptyOperator(task_id="t")
        client.dags.delete("deletable_dag")
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.dags.get("deletable_dag")
        assert exc_info.value.response.status_code == 404

    def test_get_details(self, client, dag_maker):
        with dag_maker("detail_dag", schedule=None):
            EmptyOperator(task_id="t")
        result = client.dags.get_details("detail_dag")
        assert result["dag_id"] == "detail_dag"


class TestDagRunsClientViaLocalREST:
    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup(self):
        self._clear_db()
        yield
        self._clear_db()

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    def test_trigger_dag_run(self, client, dag_maker):
        with dag_maker("trigger_dag", schedule=None):
            EmptyOperator(task_id="t")
        client.dags.unpause("trigger_dag")
        result = client.dag_runs.trigger("trigger_dag")
        assert result["dag_id"] == "trigger_dag"
        assert result["state"] == "queued"

    def test_trigger_dag_run_with_conf(self, client, dag_maker):
        with dag_maker("conf_dag", schedule=None):
            EmptyOperator(task_id="t")
        client.dags.unpause("conf_dag")
        result = client.dag_runs.trigger("conf_dag", conf={"key": "value"})
        assert result["dag_id"] == "conf_dag"
        assert result["conf"] == {"key": "value"}

    def test_get_dag_run(self, client, dag_maker):
        with dag_maker("get_run_dag", schedule=None):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        result = client.dag_runs.get("get_run_dag", dr.run_id)
        assert result["dag_id"] == "get_run_dag"
        assert result["dag_run_id"] == dr.run_id

    def test_list_dag_runs(self, client, dag_maker):
        with dag_maker("list_runs_dag", schedule=None):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        result = client.dag_runs.list("list_runs_dag")
        assert result["total_entries"] >= 1
        assert result["dag_runs"][0]["dag_id"] == "list_runs_dag"

    def test_update_dag_run_note(self, client, dag_maker):
        with dag_maker("note_dag", schedule=None):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        result = client.dag_runs.update("note_dag", dr.run_id, note="my note")
        assert result["note"] == "my note"

    def test_delete_dag_run(self, client, dag_maker):
        with dag_maker("del_run_dag", schedule=None):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        run_id = dr.run_id
        client.dag_runs.delete("del_run_dag", run_id)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.dag_runs.get("del_run_dag", run_id)
        assert exc_info.value.response.status_code == 404

    def test_clear_dag_run(self, client, dag_maker):
        with dag_maker("clear_dag", schedule=None):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(state=DagRunState.FAILED)
        result = client.dag_runs.clear("clear_dag", dr.run_id, dry_run=True)
        # dry_run=True returns a TaskInstanceCollectionResponse
        assert "task_instances" in result or "total_entries" in result


class TestTaskInstancesClientViaLocalREST:
    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup(self):
        self._clear_db()
        yield
        self._clear_db()

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    def test_list_task_instances(self, client, dag_maker):
        with dag_maker("ti_dag", schedule=None):
            EmptyOperator(task_id="task_a")
            EmptyOperator(task_id="task_b")
        dr = dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        result = client.task_instances.list("ti_dag", dr.run_id)
        assert result["total_entries"] == 2
        task_ids = [ti["task_id"] for ti in result["task_instances"]]
        assert "task_a" in task_ids
        assert "task_b" in task_ids

    def test_get_task_instance(self, client, dag_maker):
        with dag_maker("get_ti_dag", schedule=None):
            EmptyOperator(task_id="my_task")
        dr = dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        result = client.task_instances.get("get_ti_dag", dr.run_id, "my_task")
        assert result["task_id"] == "my_task"
        assert result["dag_id"] == "get_ti_dag"

    def test_get_task_instance_not_found(self, client, dag_maker):
        with dag_maker("notfound_ti_dag", schedule=None):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.task_instances.get("notfound_ti_dag", dr.run_id, "nonexistent_task")
        assert exc_info.value.response.status_code == 404

    def test_list_task_instances_across_dags(self, client, dag_maker):
        with dag_maker("cross_dag_a", schedule=None):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        with dag_maker("cross_dag_b", schedule=None):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(state=DagRunState.SUCCESS)
        # ~ means "all" for both dag_id and dag_run_id
        result = client.task_instances.list("~", "~")
        assert result["total_entries"] >= 2

    def test_clear_task_instances(self, client, dag_maker):
        with dag_maker("clear_ti_dag", schedule=None):
            EmptyOperator(task_id="t1")
            EmptyOperator(task_id="t2")
        dag_maker.create_dagrun(state=DagRunState.FAILED)
        result = client.task_instances.clear("clear_ti_dag", task_ids=["t1"], dry_run=True)
        assert "task_instances" in result or "total_entries" in result


class TestConfigClientViaLocalREST:
    @pytest.fixture(autouse=True)
    def setup(self):
        with conf_vars({("api", "expose_config"): "True"}):
            yield

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    def test_get_full_config(self, client):
        result = client.config.get()
        assert "sections" in result
        assert len(result["sections"]) > 0

    def test_get_config_section(self, client):
        result = client.config.get(section="core")
        assert "sections" in result
        sections = [s["name"] for s in result["sections"]]
        assert "core" in sections

    def test_get_config_value(self, client):
        result = client.config.get_value("core", "executor")
        assert "sections" in result
        options = result["sections"][0]["options"]
        assert any(o["key"] == "executor" for o in options)


class TestAssetsClientViaLocalREST:
    @staticmethod
    def _clear_db():
        clear_db_assets()
        clear_db_runs()
        clear_db_dags()

    @pytest.fixture(autouse=True)
    def setup(self):
        self._clear_db()
        yield
        self._clear_db()

    @pytest.fixture
    def client(self):
        c = get_local_rest_client(process_type="test")
        yield c
        c.close()

    @staticmethod
    def _create_asset(session, name="test_asset", uri="s3://bucket/key/1"):
        from airflow._shared.timezones import timezone

        dt = timezone.datetime(2020, 6, 11, 18, 0, 0)
        asset = AssetModel(
            name=name,
            uri=uri,
            group="asset",
            extra={"foo": "bar"},
            created_at=dt,
            updated_at=dt,
        )
        session.add(asset)
        session.add(AssetActive.for_asset(asset))
        session.commit()
        return asset

    def test_list_assets(self, client, session):
        self._create_asset(session, name="asset_a", uri="s3://a/1")
        self._create_asset(session, name="asset_b", uri="s3://b/2")
        result = client.assets.list()
        assert result["total_entries"] >= 2

    def test_get_asset(self, client, session):
        asset = self._create_asset(session)
        result = client.assets.get(asset.id)
        assert result["name"] == "test_asset"

    def test_get_asset_not_found(self, client):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.assets.get(99999)
        assert exc_info.value.response.status_code == 404

    def test_list_events_empty(self, client, session):
        self._create_asset(session)
        result = client.assets.list_events()
        assert result["total_entries"] == 0

    def test_get_queued_events(self, client, session):
        asset = self._create_asset(session)
        result = client.assets.get_queued_events(asset.id)
        assert "queued_events" in result or "total_entries" in result

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

from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from airflow.sdk import BaseOperator, get_current_context, timezone
from airflow.sdk.api.datamodels._generated import AssetEventResponse, AssetResponse
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAliasEvent,
    AssetAliasUniqueKey,
    AssetUniqueKey,
)
from airflow.sdk.definitions.connection import Connection
from airflow.sdk.definitions.variable import Variable
from airflow.sdk.exceptions import AirflowNotFoundException, ErrorType
from airflow.sdk.execution_time.comms import (
    AssetEventDagRunReferenceResult,
    AssetEventResult,
    AssetEventSourceTaskInstance,
    AssetEventsResult,
    AssetResult,
    ConnectionResult,
    DagRunResult,
    ErrorResponse,
    GetAssetByName,
    GetAssetByUri,
    GetAssetEventByAsset,
    GetDagRun,
    GetXCom,
    VariableResult,
    XComResult,
)
from airflow.sdk.execution_time.context import (
    ConnectionAccessor,
    InletEventsAccessors,
    OutletEventAccessor,
    OutletEventAccessors,
    TriggeringAssetEventsAccessor,
    VariableAccessor,
    _AssetRefResolutionMixin,
    _async_get_connection,
    _convert_variable_result_to_variable,
    _get_connection,
    _process_connection_result_conn,
    context_to_airflow_vars,
    set_current_context,
)
from airflow.sdk.execution_time.secrets import ExecutionAPISecretsBackend


def test_convert_connection_result_conn():
    """Test that the ConnectionResult is converted to a Connection object."""
    conn = ConnectionResult(
        conn_id="test_conn",
        conn_type="mysql",
        host="mysql",
        schema="airflow",
        login="root",
        password="password",
        port=1234,
        extra='{"extra_key": "extra_value"}',
    )
    conn = _process_connection_result_conn(conn)
    assert conn == Connection(
        conn_id="test_conn",
        conn_type="mysql",
        host="mysql",
        schema="airflow",
        login="root",
        password="password",
        port=1234,
        extra='{"extra_key": "extra_value"}',
    )


def test_convert_variable_result_to_variable():
    """Test that the VariableResult is converted to a Variable object."""
    var = VariableResult(
        key="test_key",
        value="test_value",
    )
    var = _convert_variable_result_to_variable(var, deserialize_json=False)
    assert var == Variable(
        key="test_key",
        value="test_value",
    )


def test_convert_variable_result_to_variable_with_deserialize_json():
    """Test that the VariableResult is converted to a Variable object with deserialize_json set to True."""
    var = VariableResult(
        key="test_key",
        value='{\r\n  "key1": "value1",\r\n  "key2": "value2",\r\n  "enabled": true,\r\n  "threshold": 42\r\n}',
    )
    var = _convert_variable_result_to_variable(var, deserialize_json=True)
    assert var == Variable(
        key="test_key", value={"key1": "value1", "key2": "value2", "enabled": True, "threshold": 42}
    )


class TestAirflowContextHelpers:
    def test_context_to_airflow_vars_empty_context(self):
        assert context_to_airflow_vars({}) == {}

    def test_context_to_airflow_vars_all_context(self, create_runtime_ti):
        task = BaseOperator(
            task_id="test_context_vars",
            owner=["owner1", "owner2"],
            email="email1@test.com",
        )

        rti = create_runtime_ti(
            task=task,
            dag_id="dag_id",
            run_id="dag_run_id",
            logical_date="2017-05-21T00:00:00Z",
            try_number=1,
        )
        context = rti.get_template_context()
        assert context_to_airflow_vars(context) == {
            "airflow.ctx.dag_id": "dag_id",
            "airflow.ctx.logical_date": "2017-05-21T00:00:00+00:00",
            "airflow.ctx.task_id": "test_context_vars",
            "airflow.ctx.dag_run_id": "dag_run_id",
            "airflow.ctx.try_number": "1",
            "airflow.ctx.dag_owner": "owner1,owner2",
            "airflow.ctx.dag_email": "email1@test.com",
        }

        assert context_to_airflow_vars(context, in_env_var_format=True) == {
            "AIRFLOW_CTX_DAG_ID": "dag_id",
            "AIRFLOW_CTX_LOGICAL_DATE": "2017-05-21T00:00:00+00:00",
            "AIRFLOW_CTX_TASK_ID": "test_context_vars",
            "AIRFLOW_CTX_TRY_NUMBER": "1",
            "AIRFLOW_CTX_DAG_RUN_ID": "dag_run_id",
            "AIRFLOW_CTX_DAG_OWNER": "owner1,owner2",
            "AIRFLOW_CTX_DAG_EMAIL": "email1@test.com",
        }

    def test_context_to_airflow_vars_from_policy(self):
        with mock.patch("airflow.settings.get_airflow_context_vars") as mock_method:
            airflow_cluster = "cluster-a"
            mock_method.return_value = {"airflow_cluster": airflow_cluster}

            context_vars = context_to_airflow_vars({})
            assert context_vars["airflow.ctx.airflow_cluster"] == airflow_cluster

            context_vars = context_to_airflow_vars({}, in_env_var_format=True)
            assert context_vars["AIRFLOW_CTX_AIRFLOW_CLUSTER"] == airflow_cluster

        with mock.patch("airflow.settings.get_airflow_context_vars") as mock_method:
            mock_method.return_value = {"airflow_cluster": [1, 2]}
            with pytest.raises(TypeError) as error:
                context_to_airflow_vars({})
            assert str(error.value) == "value of key <airflow_cluster> must be string, not <class 'list'>"

        with mock.patch("airflow.settings.get_airflow_context_vars") as mock_method:
            mock_method.return_value = {1: "value"}
            with pytest.raises(TypeError) as error:
                context_to_airflow_vars({})
            assert str(error.value) == "key <1> must be string"


class TestConnectionAccessor:
    def test_getattr_connection(self, mock_supervisor_comms):
        """
        Test that the connection is fetched when accessed via __getattr__.

        The __getattr__ method is used for template rendering. Example: ``{{ conn.mysql_conn.host }}``.
        """
        accessor = ConnectionAccessor()

        # Conn from the supervisor / API Server
        conn_result = ConnectionResult(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

        mock_supervisor_comms.send.return_value = conn_result

        # Fetch the connection; triggers __getattr__
        conn = accessor.mysql_conn

        expected_conn = Connection(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)
        assert conn == expected_conn

    def test_get_method_valid_connection(self, mock_supervisor_comms):
        """Test that the get method returns the requested connection using `conn.get`."""
        accessor = ConnectionAccessor()
        conn_result = ConnectionResult(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

        mock_supervisor_comms.send.return_value = conn_result

        conn = accessor.get("mysql_conn")
        assert conn == Connection(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

    def test_get_method_with_default(self, mock_supervisor_comms):
        """Test that the get method returns the default connection when the requested connection is not found."""
        accessor = ConnectionAccessor()
        default_conn = {"conn_id": "default_conn", "conn_type": "sqlite"}
        error_response = ErrorResponse(
            error=ErrorType.CONNECTION_NOT_FOUND, detail={"conn_id": "nonexistent_conn"}
        )

        mock_supervisor_comms.send.return_value = error_response

        conn = accessor.get("nonexistent_conn", default_conn=default_conn)
        assert conn == default_conn

    def test_getattr_connection_for_extra_dejson(self, mock_supervisor_comms):
        accessor = ConnectionAccessor()

        # Conn from the supervisor / API Server
        conn_result = ConnectionResult(
            conn_id="mysql_conn",
            conn_type="mysql",
            host="mysql",
            port=3306,
            extra='{"extra_key": "extra_value"}',
        )

        mock_supervisor_comms.send.return_value = conn_result

        # Fetch the connection's dejson; triggers __getattr__
        dejson = accessor.mysql_conn.extra_dejson

        assert dejson == {"extra_key": "extra_value"}

    @patch("airflow.sdk.definitions.connection.log", create=True)
    def test_getattr_connection_for_extra_dejson_decode_error(self, mock_log, mock_supervisor_comms):
        mock_log.return_value = MagicMock()

        accessor = ConnectionAccessor()

        # Conn from the supervisor / API Server
        conn_result = ConnectionResult(
            conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306, extra="This is not JSON!"
        )

        mock_supervisor_comms.send.return_value = conn_result

        # Fetch the connection's dejson; triggers __getattr__
        dejson = accessor.mysql_conn.extra_dejson

        # empty in case of failed deserialising
        assert dejson == {}

        mock_log.exception.assert_any_call(
            "Failed to deserialize extra property `extra`, returning empty dictionary"
        )


class TestVariableAccessor:
    def test_getattr_variable(self, mock_supervisor_comms):
        """
        Test that the variable is fetched when accessed via __getattr__.
        """
        accessor = VariableAccessor(deserialize_json=False)

        # Variable from the supervisor / API Server
        var_result = VariableResult(key="test_key", value="test_value")

        mock_supervisor_comms.send.return_value = var_result

        # Fetch the variable; triggers __getattr__
        value = accessor.test_key

        assert value == var_result.value

    def test_get_method_valid_variable(self, mock_supervisor_comms):
        """Test that the get method returns the requested variable using `var.get`."""
        accessor = VariableAccessor(deserialize_json=False)
        var_result = VariableResult(key="test_key", value="test_value")

        mock_supervisor_comms.send.return_value = var_result

        val = accessor.get("test_key")
        assert val == var_result.value

    def test_get_method_with_default(self, mock_supervisor_comms):
        """Test that the get method returns the default variable when the requested variable is not found."""

        accessor = VariableAccessor(deserialize_json=False)
        error_response = ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"test_key": "test_value"})

        mock_supervisor_comms.send.return_value = error_response

        val = accessor.get("nonexistent_var_key", default="default_value")
        assert val == "default_value"


class TestCurrentContext:
    def test_current_context_roundtrip(self):
        example_context = {"Hello": "World"}

        with set_current_context(example_context):
            assert get_current_context() == example_context

    def test_context_removed_after_exit(self):
        example_context = {"Hello": "World"}

        with set_current_context(example_context):
            pass
        with pytest.raises(RuntimeError):
            get_current_context()

    def test_nested_context(self):
        """
        Nested execution context should be supported in case the user uses multiple context managers.
        Each time the execute method of an operator is called, we set a new 'current' context.
        This test verifies that no matter how many contexts are entered - order is preserved
        """
        max_stack_depth = 15
        ctx_list = []
        for i in range(max_stack_depth):
            # Create all contexts in ascending order
            new_context = {"ContextId": i}
            # Like 15 nested with statements
            ctx_obj = set_current_context(new_context)
            ctx_obj.__enter__()
            ctx_list.append(ctx_obj)
        for i in reversed(range(max_stack_depth)):
            # Iterate over contexts in reverse order - stack is LIFO
            ctx = get_current_context()
            assert ctx["ContextId"] == i
            # End of with statement
            ctx_list[i].__exit__(None, None, None)


class TestOutletEventAccessor:
    @pytest.mark.parametrize(
        "add_args",
        [
            (Asset("name", "uri", extra={"extra": "from asset itself"}), {"extra": "from event"}),
            (Asset.ref(name="name"), {"extra": "from event"}),
            (Asset.ref(uri="uri"), {"extra": "from event"}),
        ],
        ids=["asset", "asset name ref", "asset uri ref"],
    )
    @pytest.mark.parametrize(
        ("key", "asset_alias_events"),
        (
            (AssetUniqueKey.from_asset(Asset("test_uri")), []),
            (
                AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias")),
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(name="name", uri="uri"),
                        dest_asset_extra={"extra": "from asset itself"},
                        extra={"extra": "from event"},
                    )
                ],
            ),
        ),
        ids=["inactive asset", "active asset"],
    )
    def test_add(self, add_args, key, asset_alias_events, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = AssetResponse(
            name="name", uri="uri", group="", extra={"extra": "from asset itself"}
        )

        outlet_event_accessor = OutletEventAccessor(key=key, extra={})
        outlet_event_accessor.add(*add_args)
        assert outlet_event_accessor.asset_alias_events == asset_alias_events

    @pytest.mark.parametrize(
        "add_args",
        [
            (Asset(name="name", uri="uri", extra={"extra": "from asset itself"}), {"extra": "from event"}),
            (Asset.ref(name="name"), {"extra": "from event"}),
            (Asset.ref(uri="uri"), {"extra": "from event"}),
        ],
        ids=["asset", "asset name ref", "asset uri ref"],
    )
    @pytest.mark.parametrize(
        ("key", "asset_alias_events"),
        (
            (AssetUniqueKey.from_asset(Asset("test_uri")), []),
            (
                AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias")),
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(name="name", uri="uri"),
                        dest_asset_extra={"extra": "from asset itself"},
                        extra={"extra": "from event"},
                    )
                ],
            ),
        ),
        ids=["inactive asset", "active asset"],
    )
    def test_add_with_db(self, add_args, key, asset_alias_events, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = AssetResponse(
            name="name", uri="uri", group="", extra={"extra": "from asset itself"}
        )

        outlet_event_accessor = OutletEventAccessor(key=key)
        outlet_event_accessor.add(*add_args)
        assert outlet_event_accessor.asset_alias_events == asset_alias_events


class TestTriggeringAssetEventsAccessor:
    @pytest.fixture(autouse=True)
    def clear_cache(self):
        _AssetRefResolutionMixin._asset_ref_cache = {}
        yield
        _AssetRefResolutionMixin._asset_ref_cache = {}

    @pytest.fixture
    def event_data(self):
        return [
            {
                "asset": {
                    "name": "1",
                    "uri": "1",
                    "extra": {},
                },
                "extra": {},
                "source_task_id": "t1",
                "source_dag_id": "d1",
                "source_run_id": "r1",
                "source_map_index": -1,
                "source_aliases": [],
                "timestamp": "2025-01-01T00:00:12Z",
            },
            {
                "asset": {
                    "name": "1",
                    "uri": "1",
                    "extra": {},
                },
                "extra": {},
                "source_task_id": "t2",
                "source_dag_id": "d1",
                "source_run_id": "r1",
                "source_map_index": -1,
                "source_aliases": [
                    {"name": "a"},
                    {"name": "b"},
                ],
                "timestamp": "2025-01-01T00:05:43Z",
            },
            {
                "asset": {
                    "name": "2",
                    "uri": "2",
                    "extra": {},
                },
                "extra": {},
                "source_task_id": "t2",
                "source_dag_id": "d1",
                "source_run_id": "r1",
                "source_map_index": -1,
                "source_aliases": [],
                "timestamp": "2025-01-01T00:06:07Z",
            },
        ]

    @pytest.fixture
    def accessor(self, event_data):
        return TriggeringAssetEventsAccessor.build(
            AssetEventDagRunReferenceResult.model_validate(d) for d in event_data
        )

    @pytest.mark.parametrize(
        ("key", "result_indexes"),
        [
            (Asset("1"), [0, 1]),
            (Asset("2"), [2]),
            (AssetAlias("a"), [1]),
            (AssetAlias("b"), [1]),
        ],
    )
    def test_getitem(self, event_data, accessor, key, result_indexes):
        expected = [AssetEventDagRunReferenceResult.model_validate(event_data[i]) for i in result_indexes]
        assert accessor[key] == expected

    @pytest.mark.parametrize(
        ("name", "resolved_asset", "result_indexes"),
        [
            ("1", AssetResult(name="1", uri="1", group="whatever"), [0, 1]),
            ("2", AssetResult(name="2", uri="2", group="whatever"), [2]),
        ],
    )
    def test_getitem_name_ref(
        self,
        mock_supervisor_comms,
        event_data,
        accessor,
        name,
        resolved_asset,
        result_indexes,
    ):
        mock_supervisor_comms.send.return_value = resolved_asset
        expected = [AssetEventDagRunReferenceResult.model_validate(event_data[i]) for i in result_indexes]
        assert accessor[Asset.ref(name=name)] == expected

        assert mock_supervisor_comms.send.mock_calls == [
            mock.call(GetAssetByName(name=name, type="GetAssetByName"))
        ]
        assert _AssetRefResolutionMixin._asset_ref_cache

    @pytest.mark.parametrize(
        ("uri", "resolved_asset", "result_indexes"),
        [
            ("1", AssetResult(name="1", uri="1", group="whatever"), [0, 1]),
            ("2", AssetResult(name="2", uri="2", group="whatever"), [2]),
        ],
    )
    def test_getitem_uri_ref(
        self,
        mock_supervisor_comms,
        event_data,
        accessor,
        uri,
        resolved_asset,
        result_indexes,
    ):
        mock_supervisor_comms.send.return_value = resolved_asset
        expected = [AssetEventDagRunReferenceResult.model_validate(event_data[i]) for i in result_indexes]
        assert accessor[Asset.ref(uri=uri)] == expected
        assert mock_supervisor_comms.send.mock_calls == [mock.call(GetAssetByUri(uri=uri))]
        assert _AssetRefResolutionMixin._asset_ref_cache

    def test_source_task_instance_xcom_pull(self, mock_supervisor_comms, accessor):
        events = accessor[Asset("2")]
        assert len(events) == 1

        mock_dag_run = mock.Mock(dag_id="d1", run_id="r1")
        mock_supervisor_comms.send.side_effect = [mock_dag_run]
        source = events[0].source_task_instance
        assert source == AssetEventSourceTaskInstance(dag_run=mock_dag_run, task_id="t2", map_index=-1)
        assert mock_supervisor_comms.send.mock_calls == [mock.call(GetDagRun(dag_id="d1", run_id="r1"))]

        mock_supervisor_comms.reset_mock()
        mock_supervisor_comms.send.side_effect = [
            XComResult(key=BaseXCom.XCOM_RETURN_KEY, value="__example_xcom_value__"),
        ]
        assert source.xcom_pull() == "__example_xcom_value__"
        assert mock_supervisor_comms.send.mock_calls == [
            mock.call(
                GetXCom(
                    key=BaseXCom.XCOM_RETURN_KEY,
                    dag_id="d1",
                    run_id="r1",
                    task_id="t2",
                    map_index=-1,
                ),
            )
        ]


TEST_ASSET = Asset(name="test_uri", uri="test://test")
TEST_ASSET_ALIAS = AssetAlias(name="name")
TEST_ASSET_REFS = [Asset.ref(name="test_uri"), Asset.ref(uri="test://test/")]
TEST_INLETS = [TEST_ASSET, TEST_ASSET_ALIAS] + TEST_ASSET_REFS


class TestOutletEventAccessors:
    @pytest.mark.parametrize(
        ("access_key", "internal_key"),
        (
            (Asset("test"), AssetUniqueKey.from_asset(Asset("test"))),
            (
                Asset(name="test", uri="test://asset"),
                AssetUniqueKey.from_asset(Asset(name="test", uri="test://asset")),
            ),
            (AssetAlias("test_alias"), AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias"))),
        ),
    )
    def test__get_item__dict_key_not_exists(self, access_key, internal_key):
        outlet_event_accessors = OutletEventAccessors()
        assert len(outlet_event_accessors) == 0
        outlet_event_accessor = outlet_event_accessors[access_key]
        assert len(outlet_event_accessors) == 1
        assert outlet_event_accessor.key == internal_key
        assert outlet_event_accessor.extra == {}

    @pytest.mark.parametrize(
        ("access_key", "asset"),
        (
            (Asset.ref(name="test"), Asset(name="test")),
            (Asset.ref(name="test1"), Asset(name="test1", uri="test://asset-uri")),
            (Asset.ref(uri="test://asset-uri"), Asset(uri="test://asset-uri")),
        ),
    )
    def test__get_item__asset_ref(self, access_key, asset, mock_supervisor_comms):
        """Test accessing OutletEventAccessors with AssetRef resolves to correct Asset."""
        internal_key = AssetUniqueKey.from_asset(asset)
        outlet_event_accessors = OutletEventAccessors()
        assert len(outlet_event_accessors) == 0

        # Asset from the API Server via the supervisor
        mock_supervisor_comms.send.return_value = AssetResult(
            name=asset.name,
            uri=asset.uri,
            group=asset.group,
        )

        outlet_event_accessor = outlet_event_accessors[access_key]
        assert len(outlet_event_accessors) == 1
        assert outlet_event_accessor.key == internal_key
        assert outlet_event_accessor.extra == {}

    @pytest.mark.parametrize(
        ("name", "uri", "expected_key"),
        (
            ("test_uri", "test://test/", TEST_ASSET),
            ("test_uri", None, TEST_ASSET_REFS[0]),
            (None, "test://test/", TEST_ASSET_REFS[1]),
        ),
    )
    @mock.patch("airflow.sdk.execution_time.context.OutletEventAccessors.__getitem__")
    def test_for_asset(self, mocked__getitem__, name, uri, expected_key):
        outlet_event_accessors = OutletEventAccessors()
        outlet_event_accessors.for_asset(name=name, uri=uri)
        assert mocked__getitem__.call_args[0][0] == expected_key

    @mock.patch("airflow.sdk.execution_time.context.OutletEventAccessors.__getitem__")
    def test_for_asset_alias(self, mocked__getitem__):
        outlet_event_accessors = OutletEventAccessors()
        outlet_event_accessors.for_asset_alias(name="name")
        assert mocked__getitem__.call_args[0][0] == TEST_ASSET_ALIAS


class TestInletEventAccessor:
    @pytest.fixture
    def sample_inlet_evnets_accessor(self, mock_supervisor_comms):
        mock_supervisor_comms.send.side_effect = [
            AssetResult(name="test_uri", uri="test://test", group="asset"),
            AssetResult(name="test_uri", uri="test://test", group="asset"),
        ]
        obj = InletEventsAccessors(inlets=TEST_INLETS)
        mock_supervisor_comms.reset_mock()
        return obj

    @pytest.mark.usefixtures("mock_supervisor_comms")
    def test__iter__(self, sample_inlet_evnets_accessor):
        for actual, expected in zip(sample_inlet_evnets_accessor, TEST_INLETS):
            assert actual == expected

    @pytest.mark.usefixtures("mock_supervisor_comms")
    def test__len__(self, sample_inlet_evnets_accessor):
        assert len(sample_inlet_evnets_accessor) == 4

    @pytest.mark.parametrize("key", TEST_INLETS + [0, 1, 2, 3])
    def test__get_item__(self, key, sample_inlet_evnets_accessor, mock_supervisor_comms):
        # This test only verifies a valid key can be used to access inlet events,
        # but not access asset events are fetched. That is verified in test_asset_events in execution_api
        asset_event_resp = AssetEventResult(
            id=1,
            created_dagruns=[],
            timestamp=timezone.utcnow(),
            asset=AssetResponse(name="test", uri="test", group="asset"),
        )
        events_result = AssetEventsResult(asset_events=[asset_event_resp])
        mock_supervisor_comms.send.side_effect = [events_result] * 4

        assert list(sample_inlet_evnets_accessor[key]) == [asset_event_resp]

    @pytest.mark.usefixtures("mock_supervisor_comms")
    def test__get_item__out_of_index(self, sample_inlet_evnets_accessor):
        with pytest.raises(IndexError):
            sample_inlet_evnets_accessor[5]

    def test__get_item__with_filters(self, sample_inlet_evnets_accessor, mock_supervisor_comms):
        asset_event_resp = AssetEventResult(
            id=1,
            created_dagruns=[],
            timestamp=timezone.utcnow(),
            asset=AssetResponse(name="test_uri", uri="test_uri", group="asset"),
        )
        events_result = AssetEventsResult(asset_events=[asset_event_resp])
        mock_supervisor_comms.send.side_effect = [events_result] * 10

        list(sample_inlet_evnets_accessor[TEST_ASSET])
        list(sample_inlet_evnets_accessor[TEST_ASSET].after("2024-01-01T00:00:00Z"))
        list(sample_inlet_evnets_accessor[TEST_ASSET].before("2024-01-01T00:00:00Z"))
        list(sample_inlet_evnets_accessor[TEST_ASSET].limit(10))
        list(
            sample_inlet_evnets_accessor[TEST_ASSET]
            .after("2024-01-01T00:00:00Z")
            .before("2024-01-02T00:00:00Z")
            .limit(10)
        )
        list(sample_inlet_evnets_accessor[TEST_ASSET].ascending(False).limit(10))

        assert mock_supervisor_comms.send.call_count == 6

        # test accessing the accessor without list() or []
        sample_inlet_evnets_accessor[TEST_ASSET].ascending(False).limit(10)

        assert mock_supervisor_comms.send.call_count == 6

        # test accessing one of the elements
        res = sample_inlet_evnets_accessor[TEST_ASSET].ascending(False).limit(10)[0]
        assert res == asset_event_resp
        assert mock_supervisor_comms.send.call_count == 7

        # test evaluating the accessor multiple times with the same filters
        res = sample_inlet_evnets_accessor[TEST_ASSET].ascending(False).limit(10)
        assert res[0] == asset_event_resp
        assert res[0] == asset_event_resp

        assert mock_supervisor_comms.send.call_count == 8

        # test changing one of the filters
        assert res.after("2024-01-01T00:00:00Z")[0] == asset_event_resp

        assert mock_supervisor_comms.send.call_count == 9

        # test len()
        assert len(sample_inlet_evnets_accessor[TEST_ASSET].ascending(True).limit(10)) == 1
        assert mock_supervisor_comms.send.call_count == 10

        calls = mock_supervisor_comms.send.call_args_list
        assert calls[0][0][0] == GetAssetEventByAsset(
            name="test_uri", uri="test://test/", after=None, before=None, limit=None, ascending=True
        )
        assert calls[1][0][0] == GetAssetEventByAsset(
            name="test_uri",
            uri="test://test/",
            after="2024-01-01T00:00:00Z",
            before=None,
            limit=None,
            ascending=True,
        )
        assert calls[2][0][0] == GetAssetEventByAsset(
            name="test_uri",
            uri="test://test/",
            after=None,
            before="2024-01-01T00:00:00Z",
            limit=None,
            ascending=True,
        )
        assert calls[3][0][0] == GetAssetEventByAsset(
            name="test_uri", uri="test://test/", after=None, before=None, limit=10, ascending=True
        )
        assert calls[4][0][0] == GetAssetEventByAsset(
            name="test_uri",
            uri="test://test/",
            after="2024-01-01T00:00:00Z",
            before="2024-01-02T00:00:00Z",
            limit=10,
            ascending=True,
        )
        assert calls[5][0][0] == GetAssetEventByAsset(
            name="test_uri", uri="test://test/", after=None, before=None, limit=10, ascending=False
        )

    @pytest.mark.parametrize(
        ("name", "uri", "expected_key"),
        (
            ("test_uri", "test://test/", TEST_ASSET),
            ("test_uri", None, TEST_ASSET_REFS[0]),
            (None, "test://test/", TEST_ASSET_REFS[1]),
        ),
    )
    @mock.patch("airflow.sdk.execution_time.context.InletEventsAccessors.__getitem__")
    def test_for_asset(self, mocked__getitem__, sample_inlet_evnets_accessor, name, uri, expected_key):
        sample_inlet_evnets_accessor.for_asset(name=name, uri=uri)
        assert mocked__getitem__.call_args[0][0] == expected_key

    @mock.patch("airflow.sdk.execution_time.context.InletEventsAccessors.__getitem__")
    def test_for_asset_alias(self, mocked__getitem__, sample_inlet_evnets_accessor):
        sample_inlet_evnets_accessor.for_asset_alias(name="name")
        assert mocked__getitem__.call_args[0][0] == TEST_ASSET_ALIAS

    def test_source_task_instance_xcom_pull(self, sample_inlet_evnets_accessor, mock_supervisor_comms):
        mock_supervisor_comms.send.side_effect = [
            AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        timestamp=timezone.utcnow(),
                        asset=AssetResponse(name="test_uri", uri="test://test", group="asset"),
                        created_dagruns=[],
                        source_dag_id="__dag__",
                        source_run_id="__run__",
                        source_task_id="__task__",
                        source_map_index=0,
                    ),
                    AssetEventResponse(
                        id=1,
                        timestamp=timezone.utcnow(),
                        asset=AssetResponse(name="test_uri", uri="test://test", group="asset"),
                        created_dagruns=[],
                    ),
                ],
            )
        ]
        events = list(sample_inlet_evnets_accessor[Asset.ref(name="test_uri")])
        assert mock_supervisor_comms.send.mock_calls == [
            mock.call(
                GetAssetEventByAsset(
                    name="test_uri",
                    uri=None,
                    after=None,
                    before=None,
                    limit=None,
                    ascending=True,
                )
            )
        ]

        assert len(events) == 2

        dag_run_result = DagRunResult(
            dag_id="__dag__",
            run_id="__run__",
            run_after=timezone.utcnow(),
            start_date=timezone.utcnow(),
            run_type="scheduled",
            state="success",
            consumed_asset_events=[],
        )
        mock_supervisor_comms.reset_mock()
        mock_supervisor_comms.send.side_effect = [dag_run_result]
        assert events[1].source_task_instance is None
        source = events[0].source_task_instance
        assert source == AssetEventSourceTaskInstance(dag_run=dag_run_result, task_id="__task__", map_index=0)
        assert mock_supervisor_comms.send.mock_calls == [
            mock.call(GetDagRun(dag_id="__dag__", run_id="__run__"))
        ]

        mock_supervisor_comms.reset_mock()
        mock_supervisor_comms.send.side_effect = [
            XComResult(key=BaseXCom.XCOM_RETURN_KEY, value="__example_xcom_value__"),
        ]
        assert source.xcom_pull() == "__example_xcom_value__"
        assert mock_supervisor_comms.send.mock_calls == [
            mock.call(
                GetXCom(
                    key=BaseXCom.XCOM_RETURN_KEY,
                    dag_id="__dag__",
                    run_id="__run__",
                    task_id="__task__",
                    map_index=0,
                ),
            )
        ]


class TestAsyncGetConnection:
    """Test async connection retrieval with secrets backends."""

    @pytest.mark.asyncio
    async def test_async_get_connection_from_secrets_backend(self, mock_supervisor_comms):
        """Test that _async_get_connection successfully retrieves from secrets backend using sync_to_async."""
        sample_connection = Connection(
            conn_id="test_conn", conn_type="postgres", host="localhost", port=5432, login="user"
        )

        class MockSecretsBackend:
            """Simple mock secrets backend for testing."""

            def __init__(self, connections: dict[str, Connection | None] | None = None):
                self.connections = connections or {}

            def get_connection(self, conn_id: str) -> Connection | None:
                return self.connections.get(conn_id)

        backend = MockSecretsBackend({"test_conn": sample_connection})

        with patch(
            "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded", autospec=True
        ) as mock_load:
            mock_load.return_value = [backend]

            result = await _async_get_connection("test_conn")

            assert result == sample_connection
            # Should not have tried SUPERVISOR_COMMS since secrets backend had the connection
            mock_supervisor_comms.send.assert_not_called()
            mock_supervisor_comms.asend.assert_not_called()


class TestSecretsBackend:
    """Test that connection resolution uses the backend chain correctly."""

    def test_execution_api_backend_in_worker_chain(self):
        """Test that ExecutionAPISecretsBackend is in the worker search path."""
        from airflow.sdk.execution_time.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

        assert (
            "airflow.sdk.execution_time.secrets.execution_api.ExecutionAPISecretsBackend"
            in DEFAULT_SECRETS_SEARCH_PATH_WORKERS
        )

    def test_metastore_backend_in_server_chain(self):
        """Test that MetastoreBackend is in the API server search path."""
        from airflow.sdk._shared.secrets_backend import DEFAULT_SECRETS_SEARCH_PATH

        assert "airflow.secrets.metastore.MetastoreBackend" in DEFAULT_SECRETS_SEARCH_PATH
        assert (
            "airflow.sdk.execution_time.secrets.execution_api.ExecutionAPISecretsBackend"
            not in DEFAULT_SECRETS_SEARCH_PATH
        )

    def test_get_connection_uses_backend_chain(self, mock_supervisor_comms):
        """Test that _get_connection properly iterates through backends."""
        from airflow.sdk.api.datamodels._generated import ConnectionResponse
        from airflow.sdk.execution_time.comms import ConnectionResult

        # Mock connection response
        conn_response = ConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
            host="example.com",
            port=443,
        )
        conn_result = ConnectionResult.from_conn_response(conn_response)
        mock_supervisor_comms.send.return_value = conn_result

        # Mock the backend loading to include our SupervisorComms backend
        supervisor_backend = ExecutionAPISecretsBackend()

        with patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded") as mock_load:
            mock_load.return_value = [supervisor_backend]

            conn = _get_connection("test_conn")

            assert conn is not None
            assert conn.conn_id == "test_conn"
            assert conn.host == "example.com"
            mock_supervisor_comms.send.assert_called_once()

    def test_get_connection_backend_fallback(self, mock_supervisor_comms):
        """Test that _get_connection falls through backends correctly."""
        from airflow.sdk.api.datamodels._generated import ConnectionResponse
        from airflow.sdk.execution_time.comms import ConnectionResult

        # First backend returns nothing (simulating env var backend with no env var)
        class EmptyBackend:
            def get_connection(self, conn_id):
                return None

        # Second backend returns the connection
        conn_response = ConnectionResponse(
            conn_id="test_conn",
            conn_type="postgres",
            host="db.example.com",
        )
        conn_result = ConnectionResult.from_conn_response(conn_response)
        mock_supervisor_comms.send.return_value = conn_result

        supervisor_backend = ExecutionAPISecretsBackend()

        with patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded") as mock_load:
            mock_load.return_value = [EmptyBackend(), supervisor_backend]

            conn = _get_connection("test_conn")

            assert conn is not None
            assert conn.conn_id == "test_conn"
            # SupervisorComms backend was called (first backend returned None)
            mock_supervisor_comms.send.assert_called_once()

    def test_get_connection_not_found_raises_error(self, mock_supervisor_comms):
        """Test that _get_connection raises error when no backend finds connection."""

        # Backend returns None (not found)
        class EmptyBackend:
            def get_connection(self, conn_id):
                return None

        with patch("airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded") as mock_load:
            mock_load.return_value = [EmptyBackend()]

            with pytest.raises(AirflowNotFoundException, match="isn't defined"):
                _get_connection("nonexistent_conn")

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

from unittest.mock import MagicMock, patch

import pytest

import airflow.models.xcom
from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend
from airflow.providers.standard.operators.empty import EmptyOperator

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

from tests_common.test_utils import db
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS, XCOM_RETURN_KEY

pytestmark = [pytest.mark.db_test]

if AIRFLOW_V_3_1_PLUS:
    from airflow.models.xcom import XComModel
if AIRFLOW_V_3_0_PLUS:
    from airflow.models.xcom import XComModel
    from airflow.sdk import ObjectStoragePath
    from airflow.sdk.execution_time.comms import XComResult
    from airflow.sdk.execution_time.xcom import resolve_xcom_backend
else:
    from airflow.io.path import ObjectStoragePath  # type: ignore[no-redef]
    from airflow.models.xcom import BaseXCom, resolve_xcom_backend  # type: ignore[no-redef]


@pytest.fixture(autouse=True)
def reset_db():
    """Reset XCom entries."""
    db.clear_db_runs()
    db.clear_db_xcom()
    yield
    db.clear_db_runs()
    db.clear_db_xcom()


@pytest.fixture(autouse=True)
def reset_cache():
    from airflow.providers.common.io.xcom import backend

    backend._get_base_path.cache_clear()
    backend._get_compression.cache_clear()
    backend._get_threshold.cache_clear()
    yield
    backend._get_base_path.cache_clear()
    backend._get_compression.cache_clear()
    backend._get_threshold.cache_clear()


@pytest.fixture
def task_instance(create_task_instance_of_operator, session):
    return create_task_instance_of_operator(
        EmptyOperator,
        dag_id="test-dag-id",
        task_id="test-task-id",
        logical_date=timezone.datetime(2021, 12, 3, 4, 56),
        session=session,
    )


class TestXComObjectStorageBackend:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, tmp_path):
        xcom_path = tmp_path / "xcom"
        xcom_path.mkdir()
        self.path = f"file://{xcom_path.as_posix()}"
        configuration = {
            ("core", "xcom_backend"): "airflow.providers.common.io.xcom.backend.XComObjectStorageBackend",
            ("common.io", "xcom_objectstorage_path"): self.path,
            ("common.io", "xcom_objectstorage_threshold"): "50",
        }
        with conf_vars(configuration):
            yield

    def test_value_db(self, task_instance, mock_supervisor_comms, session):
        session.add(task_instance)
        session.commit()
        XCom = resolve_xcom_backend()
        airflow.models.xcom.XCom = XCom

        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "value"},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
        )

        if AIRFLOW_V_3_0_PLUS:
            # When using XComObjectStorageBackend, the value is stored in the db is serialized with json dumps
            # so we need to mimic that same behavior below.
            mock_supervisor_comms.send.return_value = XComResult(key="return_value", value={"key": "value"})

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
        )
        assert value == {"key": "value"}

    def test_value_storage(self, task_instance, mock_supervisor_comms, session):
        session.add(task_instance)
        session.commit()
        XCom = resolve_xcom_backend()
        airflow.models.xcom.XCom = XCom

        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "bigvaluebigvaluebigvalue" * 100},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
        )

        if AIRFLOW_V_3_0_PLUS:
            XComModel.set(
                key=XCOM_RETURN_KEY,
                value=self.path,
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
            )
            if AIRFLOW_V_3_1_PLUS:
                res = session.execute(
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                    ).with_only_columns(XComModel.value)
                ).first()
            else:
                res = (
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                        session=session,
                    )
                    .with_entities(XComModel.value)
                    .first()
                )
            data = XComModel.deserialize_value(res)
        else:
            res = (
                XCom.get_many(
                    key=XCOM_RETURN_KEY,
                    dag_ids=task_instance.dag_id,
                    task_ids=task_instance.task_id,
                    run_id=task_instance.run_id,
                    session=session,
                )
                .with_entities(BaseXCom.value)
                .first()
            )
            data = BaseXCom.deserialize_value(res)

        p = XComObjectStorageBackend._get_full_path(data)
        assert p.exists() is True

        if AIRFLOW_V_3_0_PLUS:
            mock_supervisor_comms.send.return_value = XComResult(
                key=XCOM_RETURN_KEY, value={"key": "bigvaluebigvaluebigvalue" * 100}
            )

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
        )
        assert value == {"key": "bigvaluebigvaluebigvalue" * 100}

        if AIRFLOW_V_3_0_PLUS:
            if AIRFLOW_V_3_1_PLUS:
                value = session.execute(
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                    ).with_only_columns(XComModel.value)
                ).first()
            else:
                value = (
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                        session=session,
                    )
                    .with_entities(XComModel.value)
                    .first()
                )
            assert str(p) == XComModel.deserialize_value(value)
        else:
            qry = XCom.get_many(
                key=XCOM_RETURN_KEY,
                dag_ids=task_instance.dag_id,
                task_ids=task_instance.task_id,
                run_id=task_instance.run_id,
                session=session,
            )
            assert str(p) == qry.first().value

    def test_clear(self, task_instance, session, mock_supervisor_comms):
        session.add(task_instance)
        session.commit()
        XCom = resolve_xcom_backend()
        airflow.models.xcom.XCom = XCom

        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "superlargevalue" * 100},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
        )

        if AIRFLOW_V_3_0_PLUS:
            if hasattr(mock_supervisor_comms, "send_request"):
                # Back-compat of task-sdk. Only affects us when we manually create these objects in tests.
                last_call = mock_supervisor_comms.send_request.call_args_list[-1]
            else:
                last_call = mock_supervisor_comms.send.call_args_list[-1]
            path = (last_call.kwargs.get("msg") or last_call.args[0]).value
            XComModel.set(
                key=XCOM_RETURN_KEY,
                value=path,
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
            )
            if AIRFLOW_V_3_1_PLUS:
                res = session.execute(
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                    ).with_only_columns(XComModel.value)
                ).first()
            else:
                res = (
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                        session=session,
                    )
                    .with_entities(XComModel.value)
                    .first()
                )
            data = XComModel.deserialize_value(res)
        else:
            res = (
                XCom.get_many(
                    key=XCOM_RETURN_KEY,
                    dag_ids=task_instance.dag_id,
                    task_ids=task_instance.task_id,
                    run_id=task_instance.run_id,
                    session=session,
                )
                .with_entities(BaseXCom.value)
                .first()
            )
            data = BaseXCom.deserialize_value(res)
        p = XComObjectStorageBackend._get_full_path(data)
        assert p.exists() is True

        if AIRFLOW_V_3_0_PLUS:
            mock_supervisor_comms.send.return_value = XComResult(
                key=XCOM_RETURN_KEY, value={"key": "superlargevalue" * 100}
            )
        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
        )
        assert value

        if AIRFLOW_V_3_0_PLUS:
            mock_supervisor_comms.send.return_value = XComResult(key=XCOM_RETURN_KEY, value=path)
            XCom.delete(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
                key=XCOM_RETURN_KEY,
                map_index=task_instance.map_index,
            )
            XComModel.clear(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
                map_index=task_instance.map_index,
            )
            if AIRFLOW_V_3_1_PLUS:
                value = session.execute(
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                    ).with_only_columns(XComModel.value)
                ).first()
            else:
                value = (
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                        session=session,
                    )
                    .with_entities(XComModel.value)
                    .first()
                )
        else:
            XCom.clear(
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
                session=session,
            )
            value = XCom.get_value(
                key=XCOM_RETURN_KEY,
                ti_key=task_instance.key,
                session=session,
            )

        assert p.exists() is False
        assert not value

    @conf_vars({("common.io", "xcom_objectstorage_compression"): "gzip"})
    def test_compression(self, task_instance, session, mock_supervisor_comms):
        session.add(task_instance)
        session.commit()

        XCom = resolve_xcom_backend()
        airflow.models.xcom.XCom = XCom

        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "superlargevalue" * 100},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
        )

        if AIRFLOW_V_3_0_PLUS:
            XComModel.set(
                key=XCOM_RETURN_KEY,
                value=self.path + ".gz",
                dag_id=task_instance.dag_id,
                task_id=task_instance.task_id,
                run_id=task_instance.run_id,
            )
            if AIRFLOW_V_3_1_PLUS:
                res = session.execute(
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                    ).with_only_columns(XComModel.value)
                ).first()
            else:
                res = (
                    XComModel.get_many(
                        key=XCOM_RETURN_KEY,
                        dag_ids=task_instance.dag_id,
                        task_ids=task_instance.task_id,
                        run_id=task_instance.run_id,
                        session=session,
                    )
                    .with_entities(XComModel.value)
                    .first()
                )
            data = XComModel.deserialize_value(res)
        else:
            res = (
                XCom.get_many(
                    key=XCOM_RETURN_KEY,
                    dag_ids=task_instance.dag_id,
                    task_ids=task_instance.task_id,
                    run_id=task_instance.run_id,
                    session=session,
                )
                .with_entities(BaseXCom.value)
                .first()
            )
            data = BaseXCom.deserialize_value(res)

        assert data.endswith(".gz")

        if AIRFLOW_V_3_0_PLUS:
            mock_supervisor_comms.send.return_value = XComResult(
                key=XCOM_RETURN_KEY, value={"key": "superlargevalue" * 100}
            )

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
        )

        assert value == {"key": "superlargevalue" * 100}

    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            pytest.param(
                "file://airflow/xcoms/non_existing_file.json",
                "file://airflow/xcoms/non_existing_file.json",
                id="str",
            ),
            pytest.param(1, 1, id="int"),
            pytest.param(1.0, 1.0, id="float"),
            pytest.param("string", "string", id="str"),
            pytest.param(True, True, id="bool"),
            pytest.param({"key": "value"}, {"key": "value"}, id="dict"),
            pytest.param({"key": {"key": "value"}}, {"key": {"key": "value"}}, id="nested_dict"),
            pytest.param([1, 2, 3], [1, 2, 3], id="list"),
            pytest.param((1, 2, 3), (1, 2, 3), id="tuple"),
            pytest.param(None, None, id="none"),
        ],
    )
    def test_serialization_deserialization_basic(self, value, expected_value):
        def conditional_side_effect(data) -> ObjectStoragePath:
            if isinstance(data, str) and data.startswith("file://"):
                return ObjectStoragePath(data)
            return original_get_full_path(data)

        original_get_full_path = XComObjectStorageBackend._get_full_path

        with patch.object(
            XComObjectStorageBackend,
            "_get_full_path",
            side_effect=conditional_side_effect,
        ):
            XCom = resolve_xcom_backend()
            airflow.models.xcom.XCom = XCom

            serialized_data = XCom.serialize_value(value)
            mock_xcom_ser = MagicMock(value=serialized_data)
            deserialized_data = XCom.deserialize_value(mock_xcom_ser)

            assert deserialized_data == expected_value

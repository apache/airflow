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

import pytest

from tests.test_utils.compat import AIRFLOW_V_2_9_PLUS, ignore_provider_compatibility_error
from tests.www.test_utils import is_db_isolation_mode

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skipif(not AIRFLOW_V_2_9_PLUS, reason="Tests for Airflow 2.9.0+ only"),
]


import airflow.models.xcom
from airflow.models.xcom import BaseXCom, resolve_xcom_backend
from airflow.operators.empty import EmptyOperator

with ignore_provider_compatibility_error("2.8.0", __file__):
    from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend

from airflow.utils import timezone
from airflow.utils.xcom import XCOM_RETURN_KEY
from tests.test_utils import db
from tests.test_utils.config import conf_vars


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
        execution_date=timezone.datetime(2021, 12, 3, 4, 56),
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

    def test_value_db(self, task_instance, session):
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
            session=session,
        )

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
            session=session,
        )
        assert value == {"key": "value"}

        if not is_db_isolation_mode():
            qry = XCom.get_many(
                key=XCOM_RETURN_KEY,
                dag_ids=task_instance.dag_id,
                task_ids=task_instance.task_id,
                run_id=task_instance.run_id,
                session=session,
            )
            assert qry.first().value == {"key": "value"}

    def test_value_storage(self, task_instance, session):
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
            session=session,
        )

        if not is_db_isolation_mode():
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

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
            session=session,
        )
        assert value == {"key": "bigvaluebigvaluebigvalue" * 100}

        if not is_db_isolation_mode():
            qry = XCom.get_many(
                key=XCOM_RETURN_KEY,
                dag_ids=task_instance.dag_id,
                task_ids=task_instance.task_id,
                run_id=task_instance.run_id,
                session=session,
            )
            assert str(p) == qry.first().value

    def test_clear(self, task_instance, session):
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
            session=session,
        )

        if not is_db_isolation_mode():
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

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
            session=session,
        )
        assert value

        XCom.clear(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )

        if not is_db_isolation_mode():
            assert p.exists() is False

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
            session=session,
        )
        assert not value

    @conf_vars({("common.io", "xcom_objectstorage_compression"): "gzip"})
    def test_compression(self, task_instance, session):
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
            session=session,
        )

        if not is_db_isolation_mode():
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
            assert p.suffix == ".gz"

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
            session=session,
        )

        assert value == {"key": "superlargevalue" * 100}

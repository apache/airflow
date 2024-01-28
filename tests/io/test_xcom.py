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

from typing import TYPE_CHECKING

import pytest

import airflow.models.xcom
from airflow import settings
from airflow.configuration import conf
from airflow.io.path import ObjectStoragePath
from airflow.io.xcom import XComObjectStoreBackend
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import BaseXCom, resolve_xcom_backend
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture(autouse=True)
def reset_db():
    """Reset XCom entries."""
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(airflow.models.xcom.XCom).delete()


@pytest.fixture()
def task_instance_factory(request, session: Session):
    def func(*, dag_id, task_id, execution_date):
        run_id = DagRun.generate_run_id(DagRunType.SCHEDULED, execution_date)
        run = DagRun(
            dag_id=dag_id,
            run_type=DagRunType.SCHEDULED,
            run_id=run_id,
            execution_date=execution_date,
        )
        session.add(run)
        ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id)
        ti.dag_id = dag_id
        session.add(ti)
        session.commit()

        def cleanup_database():
            # This should also clear task instances by cascading.
            session.query(DagRun).filter_by(id=run.id).delete()
            session.commit()

        request.addfinalizer(cleanup_database)
        return ti

    return func


@pytest.fixture()
def task_instance(task_instance_factory):
    return task_instance_factory(
        dag_id="dag",
        task_id="task_1",
        execution_date=timezone.datetime(2021, 12, 3, 4, 56),
    )


class TestXcomObjectStoreBackend:
    path = "file:/tmp/xcom"

    def setup_method(self):
        conf.set("core", "xcom_backend", "airflow.io.xcom.XComObjectStoreBackend")
        conf.set("core", "xcom_objectstore_path", self.path)
        conf.set("core", "xcom_objectstore_threshold", "50")
        settings.configure_vars()

    def teardown_method(self):
        conf.remove_option("core", "xcom_backend")
        conf.remove_option("core", "xcom_objectstore_path")
        conf.remove_option("core", "xcom_objectstore_threshold")
        settings.configure_vars()
        p = ObjectStoragePath(self.path)
        if p.exists():
            p.rmdir(recursive=True)

    def test_value_db(self, task_instance, session):
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

        qry = XCom.get_many(
            key=XCOM_RETURN_KEY,
            dag_ids=task_instance.dag_id,
            task_ids=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        assert qry.first().value == {"key": "value"}

    def test_value_storage(self, task_instance, session):
        XCom = resolve_xcom_backend()
        airflow.models.xcom.XCom = XCom

        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "bigvaluebigvaluebigvalue" * 10},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )

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
        p = ObjectStoragePath(self.path) / XComObjectStoreBackend._get_key(data)
        assert p.exists() is True

        value = XCom.get_value(
            key=XCOM_RETURN_KEY,
            ti_key=task_instance.key,
            session=session,
        )
        assert value == {"key": "bigvaluebigvaluebigvalue" * 10}

        qry = XCom.get_many(
            key=XCOM_RETURN_KEY,
            dag_ids=task_instance.dag_id,
            task_ids=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )
        assert self.path in qry.first().value

    def test_clear(self, task_instance, session):
        XCom = resolve_xcom_backend()
        airflow.models.xcom.XCom = XCom

        XCom.set(
            key=XCOM_RETURN_KEY,
            value={"key": "superlargevalue" * 10},
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )

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
        p = ObjectStoragePath(self.path) / XComObjectStoreBackend._get_key(data)
        assert p.exists() is True

        XCom.clear(
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=task_instance.run_id,
            session=session,
        )

        assert p.exists() is False

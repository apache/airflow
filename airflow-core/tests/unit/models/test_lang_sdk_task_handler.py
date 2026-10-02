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

import json
from typing import Any

import pytest
import uuid6
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError

from airflow.executors.workloads import BundleInfo, TaskHandlerArtifactRef
from airflow.models.dag import DagModel
from airflow.models.lang_sdk_task_handler import (
    LangSDKTaskHandler,
    LangSDKTaskHandlerArtifact,
    compute_fileloc_hash,
    get_task_handler_artifact_refs,
)
from airflow.sdk import task
from airflow.sdk.execution_time.coordinator import reset_coordinator_manager
from airflow.utils.session import create_session

from tests_common.test_utils.asserts import capture_orm_selects
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags

pytestmark = pytest.mark.db_test

ARTIFACT_BUNDLE = "java-task-handlers"
# 6000 bytes in UTF-8, over both the MySQL key limit and the Postgres btree entry limit.
LONG_NON_ASCII_PATH = "任" * 2000
TASK_HANDLERS = {
    "etl": [
        {
            "task_id": "extract",
            "binding": "positional",
            "params": [
                {"name": "path", "value_schema": {"type": "string"}, "required": True, "exact_name": True},
                {"name": None, "value_schema": None, "required": False, "exact_name": False},
            ],
        }
    ],
}


def _make_artifact(
    *,
    bundle_name: str = ARTIFACT_BUNDLE,
    relative_fileloc: str = "etl.jar",
    cache_digest: str | None = "0" * 64,
    task_handlers: dict[str, list[dict[str, Any]]] | None = None,
) -> LangSDKTaskHandlerArtifact:
    return LangSDKTaskHandlerArtifact(
        bundle_name=bundle_name,
        relative_fileloc=relative_fileloc,
        size_bytes=1024,
        cache_digest=cache_digest,
        task_handlers=TASK_HANDLERS if task_handlers is None else task_handlers,
    )


def _make_handler(*, dag_id: str, artifact: LangSDKTaskHandlerArtifact) -> LangSDKTaskHandler:
    return LangSDKTaskHandler(
        dag_id=dag_id,
        task_id="extract",
        artifact_id=artifact.id,
        dag_bundle_name="testing",
        dag_relative_fileloc=f"{dag_id}.py",
    )


def _add_dags_and_artifact(*dag_ids: str, session) -> LangSDKTaskHandlerArtifact:
    session.add_all(DagModel(dag_id=dag_id, bundle_name="testing") for dag_id in dag_ids)
    artifact = _make_artifact()
    session.add(artifact)
    session.flush()
    return artifact


def test_deleting_dag_deletes_its_handlers(testing_dag_bundle, session):
    artifact = _add_dags_and_artifact("dag_a", "dag_b", session=session)
    session.add_all(_make_handler(dag_id=dag_id, artifact=artifact) for dag_id in ("dag_a", "dag_b"))
    session.flush()

    session.execute(delete(DagModel).where(DagModel.dag_id == "dag_a"))

    assert session.scalars(select(LangSDKTaskHandler.dag_id)).all() == ["dag_b"]
    assert session.scalars(select(LangSDKTaskHandlerArtifact.id)).all() == [artifact.id]


@pytest.mark.parametrize(
    "task_handlers",
    [
        pytest.param(TASK_HANDLERS, id="handlers"),
        pytest.param({}, id="none"),
    ],
)
def test_artifact_stores_its_task_handlers(session, task_handlers):
    session.add(_make_artifact(task_handlers=task_handlers))
    session.flush()
    session.expire_all()

    assert session.scalar(select(LangSDKTaskHandlerArtifact.task_handlers)) == task_handlers


def test_artifact_can_store_no_cache_digest(session):
    session.add(_make_artifact(cache_digest=None))
    session.flush()
    session.expire_all()

    assert session.scalar(select(LangSDKTaskHandlerArtifact.cache_digest)) is None


def test_deleting_referenced_artifact_fails(testing_dag_bundle, session):
    artifact = _add_dags_and_artifact("dag_a", session=session)
    session.add(_make_handler(dag_id="dag_a", artifact=artifact))
    session.flush()

    with pytest.raises(IntegrityError):
        session.execute(
            delete(LangSDKTaskHandlerArtifact).where(LangSDKTaskHandlerArtifact.id == artifact.id)
        )


@pytest.mark.parametrize(
    "relative_fileloc",
    [
        pytest.param("etl.jar", id="short"),
        pytest.param(LONG_NON_ASCII_PATH, id="long-non-ascii"),
    ],
)
def test_artifact_path_is_unique_per_bundle(session, relative_fileloc):
    session.add_all(
        [
            _make_artifact(relative_fileloc=relative_fileloc),
            _make_artifact(bundle_name="go-task-handlers", relative_fileloc=relative_fileloc),
        ]
    )
    session.flush()

    session.add(_make_artifact(relative_fileloc=relative_fileloc))
    with pytest.raises(IntegrityError):
        session.flush()


def test_core_insert_fills_fileloc_hashes(testing_dag_bundle, session):
    session.add(DagModel(dag_id="dag_a", bundle_name="testing"))
    session.flush()
    artifact_id = uuid6.uuid7()

    session.execute(
        insert(LangSDKTaskHandlerArtifact).values(
            id=artifact_id,
            bundle_name=ARTIFACT_BUNDLE,
            relative_fileloc="etl.jar",
            size_bytes=1024,
            cache_digest="0" * 64,
            task_handlers={},
        )
    )
    session.execute(
        insert(LangSDKTaskHandler).values(
            dag_id="dag_a",
            task_id="extract",
            artifact_id=artifact_id,
            dag_bundle_name="testing",
            dag_relative_fileloc="dags/etl.py",
        )
    )

    assert session.scalar(select(LangSDKTaskHandlerArtifact.relative_fileloc_hash)) == compute_fileloc_hash(
        "etl.jar"
    )
    assert session.scalar(select(LangSDKTaskHandler.dag_relative_fileloc_hash)) == compute_fileloc_hash(
        "dags/etl.py"
    )


def _add_artifact(*, session) -> LangSDKTaskHandlerArtifact:
    artifact = _make_artifact()
    session.add(artifact)
    session.flush()
    return artifact


def _add_handler(*, session) -> LangSDKTaskHandler:
    handler = _make_handler(dag_id="dag_a", artifact=_add_dags_and_artifact("dag_a", session=session))
    session.add(handler)
    session.flush()
    return handler


@pytest.mark.parametrize(
    ("add_row", "path_attr", "hash_column", "other_attr", "other_value"),
    [
        pytest.param(
            _add_artifact,
            "relative_fileloc",
            LangSDKTaskHandlerArtifact.relative_fileloc_hash,
            "size_bytes",
            2048,
            id="artifact",
        ),
        pytest.param(
            _add_handler,
            "dag_relative_fileloc",
            LangSDKTaskHandler.dag_relative_fileloc_hash,
            "dag_bundle_name",
            "other-bundle",
            id="handler",
        ),
    ],
)
def test_fileloc_hash_follows_orm_updates(
    testing_dag_bundle, session, add_row, path_attr, hash_column, other_attr, other_value
):
    row = add_row(session=session)
    original_path = getattr(row, path_attr)

    setattr(row, other_attr, other_value)
    session.flush()
    assert session.scalar(select(hash_column)) == compute_fileloc_hash(original_path)

    setattr(row, path_attr, "moved/file")
    session.flush()
    assert session.scalar(select(hash_column)) == compute_fileloc_hash("moved/file")


def clear_db_lang_sdk_task_handler_artifacts():
    with create_session() as session:
        session.execute(delete(LangSDKTaskHandler))
        session.execute(delete(LangSDKTaskHandlerArtifact))


GO_TASK_HANDLERS = "go-task-handlers"
GO_COORDINATOR = {
    "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
    "kwargs": {"task_handler_bundle_name": GO_TASK_HANDLERS},
}


def _sdk_config(queue_to_coordinator: dict[str, Any]) -> dict[tuple[str, str], str]:
    return {
        ("sdk", "coordinators"): json.dumps({"go": GO_COORDINATOR}),
        ("sdk", "queue_to_coordinator"): json.dumps(queue_to_coordinator),
    }


class TestGetTaskHandlerArtifactRefs:
    @pytest.fixture(autouse=True)
    def _reset_coordinator_manager(self):
        reset_coordinator_manager()
        yield
        reset_coordinator_manager()

    @pytest.fixture(autouse=True)
    def _clean_task_handler_rows(self):
        # The Dags that dag_maker commits take their bindings with them, but the artifacts stay.
        clear_db_lang_sdk_task_handler_artifacts()
        yield
        clear_db_dags()
        clear_db_lang_sdk_task_handler_artifacts()

    @pytest.fixture
    def routed(self, configure_dag_bundles, tmp_path):
        with (
            configure_dag_bundles({GO_TASK_HANDLERS: tmp_path, "dags": tmp_path}),
            conf_vars(_sdk_config({"golang": "go"})),
        ):
            yield

    @staticmethod
    def _make_task_instances(dag_maker, dag_id: str, queues: dict[str, str | None]):
        with dag_maker(dag_id):
            for task_id, queue in queues.items():

                @task.stub(task_id=task_id, queue=queue)
                def handler(): ...

                handler()
        dag_run = dag_maker.create_dagrun()
        return {ti.task_id: ti for ti in dag_run.get_task_instances()}

    @staticmethod
    def _bind(session, dag_id: str, task_id: str, *, bundle_name: str, relative_fileloc: str) -> None:
        artifact = _make_artifact(bundle_name=bundle_name, relative_fileloc=relative_fileloc)
        session.add(artifact)
        session.flush()
        session.add(
            LangSDKTaskHandler(
                dag_id=dag_id,
                task_id=task_id,
                artifact_id=artifact.id,
                dag_bundle_name="dags",
                dag_relative_fileloc=f"{dag_id}.py",
            )
        )
        session.flush()

    @pytest.mark.parametrize(
        ("bundle_name", "relative_fileloc"),
        [
            pytest.param(GO_TASK_HANDLERS, "bin/etl", id="named-bundle"),
            pytest.param("dags", "bin/etl", id="own-bundle"),
        ],
    )
    @pytest.mark.usefixtures("routed")
    def test_routed_stub_task_gets_the_artifact_it_is_bound_to(
        self, dag_maker, session, bundle_name, relative_fileloc
    ):
        tis = self._make_task_instances(dag_maker, "etl", {"load": "golang"})
        self._bind(session, "etl", "load", bundle_name=bundle_name, relative_fileloc=relative_fileloc)

        refs = get_task_handler_artifact_refs(tis.values(), session=session)

        assert refs == {
            ("etl", "load"): TaskHandlerArtifactRef(
                bundle_info=BundleInfo(name=bundle_name), rel_path=relative_fileloc
            )
        }

    @pytest.mark.usefixtures("routed")
    def test_task_instance_without_binding_or_on_unrouted_queue_gets_no_entry(self, dag_maker, session):
        tis = self._make_task_instances(
            dag_maker, "etl", {"bound": "golang", "unbound": "golang", "unrouted": "other"}
        )
        self._bind(session, "etl", "bound", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/etl")
        # Bound on a queue that no coordinator serves, as after the queue was re-routed.
        self._bind(session, "etl", "unrouted", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/other")

        refs = get_task_handler_artifact_refs(tis.values(), session=session)

        assert list(refs) == [("etl", "bound")]

    @pytest.mark.usefixtures("routed")
    def test_only_the_queried_task_instances_get_an_entry(self, dag_maker, session):
        tis = self._make_task_instances(dag_maker, "etl", {"first": "golang", "second": "golang"})
        self._bind(session, "etl", "first", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/first")
        self._bind(session, "etl", "second", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/second")

        refs = get_task_handler_artifact_refs([tis["second"]], session=session)

        assert refs == {
            ("etl", "second"): TaskHandlerArtifactRef(
                bundle_info=BundleInfo(name=GO_TASK_HANDLERS), rel_path="bin/second"
            )
        }

    @pytest.mark.usefixtures("routed")
    def test_mapped_task_instances_of_one_task_look_up_one_key(self, dag_maker, session):
        tis = self._make_task_instances(dag_maker, "etl", {"load": "golang"})
        self._bind(session, "etl", "load", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/etl")
        load = tis["load"]
        mapped = [
            type(load)(task=load.task, dag_version_id=load.dag_version_id, map_index=i) for i in range(3)
        ]

        with (
            capture_orm_selects("lang_sdk_task_handler") as statements,
            capture_orm_selects("lang_sdk_task_handler_artifact") as artifact_statements,
        ):
            refs = get_task_handler_artifact_refs(mapped, session=session)

        assert list(refs) == [("etl", "load")]
        assert len(statements) == 1
        assert statements[0].count("'etl'") == 1
        assert "FOR UPDATE" not in statements[0]
        assert artifact_statements == []

    def test_task_instance_without_queue_routes_as_the_default_queue(
        self, dag_maker, session, configure_dag_bundles, tmp_path
    ):
        tis = self._make_task_instances(dag_maker, "etl", {"load": "golang"})
        self._bind(session, "etl", "load", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/etl")
        tis["load"].queue = None

        with (
            configure_dag_bundles({GO_TASK_HANDLERS: tmp_path}),
            conf_vars(_sdk_config({"default": "go"})),
        ):
            refs = get_task_handler_artifact_refs(tis.values(), session=session)

        assert list(refs) == [("etl", "load")]

    @pytest.mark.usefixtures("routed")
    def test_one_query_serves_every_routed_task_instance(self, dag_maker, session):
        tis = {}
        for dag_id in ("etl_a", "etl_b"):
            tis.update(
                {
                    (dag_id, task_id): ti
                    for task_id, ti in self._make_task_instances(
                        dag_maker, dag_id, {f"load_{n}": "golang" for n in range(5)}
                    ).items()
                }
            )
            for n in range(5):
                self._bind(
                    session,
                    dag_id,
                    f"load_{n}",
                    bundle_name=GO_TASK_HANDLERS,
                    relative_fileloc=f"bin/{dag_id}_{n}",
                )

        with (
            capture_orm_selects("lang_sdk_task_handler") as statements,
            capture_orm_selects("lang_sdk_task_handler_artifact") as artifact_statements,
        ):
            refs = get_task_handler_artifact_refs(tis.values(), session=session)

        assert len(refs) == 10
        assert len(statements) == 1
        assert "FOR UPDATE" not in statements[0]
        assert artifact_statements == []

    @pytest.mark.usefixtures("routed")
    def test_no_query_without_a_routed_task_instance(self, dag_maker, session):
        tis = self._make_task_instances(dag_maker, "etl", {"load": "other"})
        self._bind(session, "etl", "load", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/etl")

        with capture_orm_selects("lang_sdk_task_handler") as statements:
            assert get_task_handler_artifact_refs(tis.values(), session=session) == {}
            assert get_task_handler_artifact_refs([], session=session) == {}

        assert statements == []

    def test_no_query_without_the_sdk_configuration(self, dag_maker, session):
        tis = self._make_task_instances(dag_maker, "etl", {"load": "golang"})
        self._bind(session, "etl", "load", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/etl")

        with capture_orm_selects("lang_sdk_task_handler") as statements:
            assert get_task_handler_artifact_refs(tis.values(), session=session) == {}

        assert statements == []

    @pytest.mark.parametrize(
        "sdk_config",
        [
            pytest.param(_sdk_config({"golang": "missing"}), id="unknown-coordinator"),
            pytest.param(_sdk_config({"golang": "go"}), id="task-handler-bundle-not-configured"),
            pytest.param({("sdk", "coordinators"): "{not json"}, id="malformed-json"),
            pytest.param(
                {
                    **_sdk_config({"golang": "go"}),
                    ("dag_processor", "dag_bundle_config_list"): json.dumps({"name": GO_TASK_HANDLERS}),
                },
                id="malformed-dag-bundle-config-list",
            ),
            pytest.param({("sdk", "coordinators"): "[]"}, id="coordinators-as-array"),
            pytest.param({("sdk", "coordinators"): "null"}, id="coordinators-as-null"),
            pytest.param({("sdk", "queue_to_coordinator"): '["go"]'}, id="queue-to-coordinator-as-array"),
            pytest.param(_sdk_config({"golang": ["go"]}), id="queue-to-coordinator-with-list-value"),
        ],
    )
    def test_invalid_sdk_configuration_warns_each_time_and_gives_no_entry(
        self, dag_maker, session, caplog, sdk_config
    ):
        tis = self._make_task_instances(dag_maker, "etl", {"load": "golang"})
        self._bind(session, "etl", "load", bundle_name=GO_TASK_HANDLERS, relative_fileloc="bin/etl")

        with conf_vars(sdk_config), capture_orm_selects("lang_sdk_task_handler") as selects:
            assert get_task_handler_artifact_refs(tis.values(), session=session) == {}
            assert get_task_handler_artifact_refs(tis.values(), session=session) == {}
            # An idle scheduling loop does not read, or warn about, the configuration.
            assert get_task_handler_artifact_refs([], session=session) == {}

        assert selects == []
        warnings = [e for e in caplog.entries if "[sdk] coordinator configuration" in e["event"]]
        assert len(warnings) == 2
        assert all(e.get("exception") for e in warnings)
        assert all(e["log_level"] == "warning" for e in warnings)

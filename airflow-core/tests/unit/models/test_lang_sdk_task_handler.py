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

from typing import Any

import pytest
import uuid6
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError

from airflow.models.dag import DagModel
from airflow.models.lang_sdk_task_handler import (
    LangSDKTaskHandler,
    LangSDKTaskHandlerArtifact,
    compute_fileloc_hash,
)

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
    task_handlers: dict[str, list[dict[str, Any]]] | None = None,
) -> LangSDKTaskHandlerArtifact:
    return LangSDKTaskHandlerArtifact(
        bundle_name=bundle_name,
        relative_fileloc=relative_fileloc,
        size_bytes=1024,
        cache_digest="0" * 64,
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

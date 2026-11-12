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

from unittest import mock

import pytest
from sqlalchemy import exc, func, insert, select

from airflow.assets.manager import _APDR_PENDING_MARKER_UQ, AssetManager, _is_pending_marker_conflict
from airflow.models.asset import AssetAliasModel, AssetPartitionDagRun
from airflow.models.dag import DagModel

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize(
    ("model", "values", "expected"),
    [
        pytest.param(
            AssetPartitionDagRun,
            {"target_dag_id": "classifier", "partition_key": "k", "pending_partition_key": "k"},
            True,
            id="marker",
        ),
        pytest.param(
            AssetPartitionDagRun,
            {
                "target_dag_id": "classifier",
                "partition_key": "for key 'unrelated'",
                "pending_partition_key": "for key 'unrelated'",
            },
            True,
            id="marker-with-misleading-key-fragment",
        ),
        pytest.param(
            AssetPartitionDagRun,
            {"id": 9001, "target_dag_id": "classifier", "partition_key": "k"},
            False,
            id="primary-key",
        ),
        pytest.param(
            AssetPartitionDagRun,
            {"id": 9001, "target_dag_id": "classifier", "partition_key": "unique-but-unrelated"},
            False,
            id="primary-key-with-unique-parameter",
        ),
        pytest.param(
            AssetPartitionDagRun,
            {"id": 9001, "target_dag_id": "classifier", "partition_key": _APDR_PENDING_MARKER_UQ},
            False,
            id="primary-key-with-marker-parameter",
        ),
        pytest.param(
            AssetPartitionDagRun,
            {"target_dag_id": None, "partition_key": _APDR_PENDING_MARKER_UQ},
            False,
            id="not-null-with-marker-parameter",
        ),
        pytest.param(
            AssetAliasModel,
            {"name": _APDR_PENDING_MARKER_UQ, "group": "asset"},
            False,
            id="unrelated-unique-with-marker-value",
        ),
        pytest.param(
            AssetAliasModel,
            {"name": f"for key '{_APDR_PENDING_MARKER_UQ}'", "group": "asset"},
            False,
            id="unrelated-unique-with-marker-key-fragment",
        ),
    ],
)
def test_classifies_real_constraint_violations(session, model, values, expected):
    with pytest.raises(exc.IntegrityError) as caught:
        session.execute(insert(model.__table__), [values, values])

    assert _is_pending_marker_conflict(caught.value) is expected


@mock.patch.object(AssetManager, "_get_latest_pending_apdr", autospec=True)
def test_helper_recovers_from_marker_conflict(lookup, session):
    key = "for key 'unrelated'"
    dag = DagModel(dag_id="classifier_recovery")
    winner = AssetPartitionDagRun(target_dag_id=dag.dag_id, partition_key=key, pending_partition_key=key)
    session.add(winner)
    session.flush()
    lookup.side_effect = [None, winner]

    actual = AssetManager._get_or_create_apdr(
        target_key=key,
        target_partition_date=None,
        target_dag=dag,
        rollup_fingerprint={},
        allow_reuse=True,
        session=session,
    )

    assert actual.id == winner.id
    assert lookup.call_count == 2
    assert (
        session.scalar(
            select(func.count())
            .select_from(AssetPartitionDagRun)
            .where(AssetPartitionDagRun.target_dag_id == dag.dag_id)
        )
        == 1
    )


@mock.patch.object(
    AssetManager,
    "_get_latest_pending_apdr",
    autospec=True,
    side_effect=AssetManager._get_latest_pending_apdr,
)
def test_helper_propagates_unrelated_error_without_recovery(lookup, session):
    with pytest.raises(exc.IntegrityError) as caught:
        AssetManager._get_or_create_apdr(
            target_key="classifier_invalid_dag",
            target_partition_date=None,
            target_dag=DagModel(dag_id=None),
            rollup_fingerprint={},
            allow_reuse=True,
            session=session,
        )

    assert not _is_pending_marker_conflict(caught.value)
    assert lookup.call_count == 1


def test_helper_preserves_error_from_flushing_callers_pending_rows(session):
    session.add(AssetPartitionDagRun(target_dag_id=None, partition_key="invalid_caller_row"))

    # begin_nested() flushes these rows before opening the SAVEPOINT. Recovery cannot query
    # the failed parent transaction: doing so would mask the original IntegrityError.
    with session.no_autoflush, pytest.raises(exc.IntegrityError) as caught:
        AssetManager._get_or_create_apdr(
            target_key="valid_key",
            target_partition_date=None,
            target_dag=DagModel(dag_id="classifier_caller_flush"),
            rollup_fingerprint={},
            allow_reuse=True,
            session=session,
        )

    assert not _is_pending_marker_conflict(caught.value)

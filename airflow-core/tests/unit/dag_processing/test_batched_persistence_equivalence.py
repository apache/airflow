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
"""
Persisting a sweep together must leave the database where writing it a file at a time would.

Everything a sweep carries used to be scoped to one file per write: import errors, warnings,
Dag rows, versions. Merging them makes that state shared, and each kind of it can be merged
wrongly on its own. Rather than argue each kind through, write the same sweep both ways against
a real database and compare what is in it afterwards.

Both halves go through ``_persist_sweep`` so the only difference is how much is handed over at
once, and the shapes below are the ones where merging has something to get wrong.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import delete, select

from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager, DagFileStat, FileParseResult
from airflow.dag_processing.processor import DagFileParsingResult
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetModel,
    DagScheduleAssetReference,
    TaskOutletAssetReference,
)
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.models.errors import ParseImportError
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset
from airflow.serialization.serialized_objects import LazyDeserializedDAG

from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

BUNDLE = "equiv"
OTHER_BUNDLE = "equiv-other"


def _dag(
    tmp_path: Path,
    dag_id: str,
    rel_path: str,
    schedule_on: Asset | None = None,
    outlet: Asset | None = None,
) -> LazyDeserializedDAG:
    # DagCode reads the source off disk; without a real file the Dag fails to serialize.
    (tmp_path / rel_path).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / rel_path).write_text("# equivalence fixture\n")
    dag = DAG(dag_id=dag_id, schedule=[schedule_on] if schedule_on else "@daily")
    EmptyOperator(task_id="task", dag=dag, outlets=[outlet] if outlet else [])
    dag.fileloc = str(tmp_path / rel_path)
    dag.relative_fileloc = rel_path
    return LazyDeserializedDAG.from_dag(dag)


def _file(
    tmp_path: Path,
    rel_path: str,
    dags: list[tuple[str, str]] | None = None,
    assets: list[tuple[Asset | None, Asset | None]] | None = None,
    errors: dict[str, str] | None = None,
    warnings: list[dict] | None = None,
    bundle: str = BUNDLE,
    version: str | None = "v1",
) -> FileParseResult:
    """One finished parse. ``dags`` is (dag_id, the file the Dag is filed under)."""
    (tmp_path / rel_path).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / rel_path).write_text("# equivalence fixture\n")
    return FileParseResult(
        file=DagFileInfo(bundle_name=bundle, rel_path=Path(rel_path), bundle_path=tmp_path),
        parsing_result=DagFileParsingResult(
            fileloc=str(tmp_path / rel_path),
            serialized_dags=[
                _dag(tmp_path, dag_id, under, *(assets or [(None, None)] * len(dags or []))[index])
                for index, (dag_id, under) in enumerate(dags or [])
            ],
            import_errors=errors,
            warnings=warnings,
        ),
        run_duration=0.5,
        stat=DagFileStat(),
        bundle_version=version,
        version_data={"sha": version} if version else None,
    )


def _snapshot(session) -> dict[str, list[tuple[str, ...]]]:
    """Everything a sweep writes that a reader can tell apart."""

    def rows(stmt):
        return sorted(tuple(str(column) for column in row) for row in session.execute(stmt).all())

    session.expire_all()
    return {
        "dag": rows(
            select(
                DagModel.dag_id,
                DagModel.bundle_name,
                DagModel.bundle_version,
                DagModel.relative_fileloc,
                DagModel.is_stale,
                DagModel.has_import_errors,
            )
        ),
        "dag_version": rows(select(DagVersion.dag_id, DagVersion.bundle_version, DagVersion.version_data)),
        "import_error": rows(
            select(ParseImportError.bundle_name, ParseImportError.filename, ParseImportError.stacktrace)
        ),
        "warning": rows(select(DagWarning.dag_id, DagWarning.warning_type, DagWarning.message)),
        # Assets are shared across files and bundles, so whichever file is written last decides
        # what they look like.
        "asset": rows(select(AssetModel.name, AssetModel.uri, AssetModel.group, AssetModel.extra)),
        "asset_alias": rows(select(AssetAliasModel.name, AssetAliasModel.group)),
        "asset_active": rows(select(AssetActive.name, AssetActive.uri)),
        "schedule_ref": rows(select(DagScheduleAssetReference.dag_id, DagScheduleAssetReference.asset_id)),
        "outlet_ref": rows(
            select(
                TaskOutletAssetReference.dag_id,
                TaskOutletAssetReference.task_id,
                TaskOutletAssetReference.asset_id,
            )
        ),
    }


def _reset(session) -> None:
    """Clear what a sweep writes, leaving the bundles it is written against."""
    session.execute(delete(DagWarning))
    session.commit()
    clear_db_serialized_dags()
    clear_db_import_errors()
    clear_db_dags()
    for name in (BUNDLE, OTHER_BUNDLE):
        session.merge(DagBundleModel(name=name))
    session.commit()


def _persist(sweep: list[FileParseResult], *, batched: bool) -> None:
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions.update({BUNDLE: "v1", OTHER_BUNDLE: "v1"})
    if batched:
        manager._persist_sweep(sweep)
    else:
        for item in sweep:
            manager._persist_sweep([item])


def _plain(tmp_path):
    return [_file(tmp_path, f"plain_{i}.py", dags=[(f"plain_{i}", f"plain_{i}.py")]) for i in range(3)]


def _duplicate_dag_id(tmp_path):
    return [
        _file(tmp_path, "first.py", dags=[("shared", "first.py")]),
        _file(tmp_path, "second.py", dags=[("shared", "second.py")]),
    ]


def _error_against_a_file_that_then_parses(tmp_path):
    return [
        _file(tmp_path, "blamer.py", errors={"blamed.py": "reported by another file"}),
        _file(tmp_path, "blamed.py", dags=[("blamed_dag", "blamed.py")]),
    ]


def _dag_filed_under_a_file_that_errored(tmp_path):
    return [
        _file(tmp_path, "broken.py", errors={"broken.py": "it broke"}),
        _file(tmp_path, "healthy.py", dags=[("filed_elsewhere", "broken.py")]),
    ]


def _file_that_now_defines_nothing(tmp_path):
    return [
        _file(tmp_path, "emptied.py", errors={"emptied.py": "was broken"}),
        _file(tmp_path, "healthy.py", dags=[("healthy_dag", "healthy.py")]),
        _file(tmp_path, "emptied.py"),
    ]


def _two_bundles_interleaved(tmp_path):
    return [
        _file(tmp_path, "a.py", dags=[("a_dag", "a.py")]),
        _file(tmp_path, "b.py", dags=[("b_dag", "b.py")], bundle=OTHER_BUNDLE),
        _file(tmp_path, "c.py", dags=[("c_dag", "c.py")]),
    ]


def _one_asset_defined_differently_by_two_files(tmp_path):
    """The case the whole ordering guarantee exists for: last write decides what the asset is."""
    return [
        _file(
            tmp_path,
            "first.py",
            dags=[("asset_first", "first.py")],
            assets=[(Asset(name="shared", uri="s3://shared", extra={"from": "first"}), None)],
        ),
        _file(
            tmp_path,
            "second.py",
            dags=[("asset_second", "second.py")],
            assets=[(Asset(name="shared", uri="s3://shared", extra={"from": "second"}), None)],
        ),
    ]


def _one_asset_across_two_bundles(tmp_path):
    """
    Assets are not scoped to a bundle, so an interleaved sweep can reorder who wins.

    The asset is shared by the middle and last files deliberately. Were the last allowed to join
    the first file's group to fill it, it would be written before the middle one and lose an asset
    it should win.
    """
    return [
        _file(tmp_path, "x.py", dags=[("asset_x", "x.py")]),
        _file(
            tmp_path,
            "y.py",
            dags=[("asset_y", "y.py")],
            bundle=OTHER_BUNDLE,
            assets=[(Asset(name="crossing", uri="s3://crossing", extra={"from": "y"}), None)],
        ),
        _file(
            tmp_path,
            "z.py",
            dags=[("asset_z", "z.py")],
            assets=[(Asset(name="crossing", uri="s3://crossing", extra={"from": "z"}), None)],
        ),
    ]


def _an_asset_produced_and_consumed_in_one_sweep(tmp_path):
    """One file's outlet is another's schedule, so the rows land in whichever order they are written."""
    produced = Asset(name="handoff", uri="s3://handoff")
    return [
        _file(tmp_path, "producer.py", dags=[("producer", "producer.py")], assets=[(None, produced)]),
        _file(tmp_path, "consumer.py", dags=[("consumer", "consumer.py")], assets=[(produced, None)]),
    ]


def _warning_then_a_clean_parse(tmp_path):
    return [
        _file(
            tmp_path,
            "warner.py",
            warnings=[{"dag_id": "warned_dag", "warning_type": "non-existent pool", "message": "gone"}],
        ),
        _file(tmp_path, "owner.py", dags=[("warned_dag", "owner.py")]),
    ]


@pytest.mark.parametrize(
    "build_sweep",
    [
        _plain,
        _duplicate_dag_id,
        _error_against_a_file_that_then_parses,
        _dag_filed_under_a_file_that_errored,
        _file_that_now_defines_nothing,
        _two_bundles_interleaved,
        _warning_then_a_clean_parse,
        _one_asset_defined_differently_by_two_files,
        _one_asset_across_two_bundles,
        _an_asset_produced_and_consumed_in_one_sweep,
    ],
    ids=lambda fn: fn.__name__.strip("_"),
)
def test_a_sweep_persisted_together_lands_where_one_file_at_a_time_would(build_sweep, session, tmp_path):
    _reset(session)
    _persist(build_sweep(tmp_path / "sequential"), batched=False)
    sequential = _snapshot(session)

    _reset(session)
    _persist(build_sweep(tmp_path / "batched"), batched=True)
    batched = _snapshot(session)

    assert batched == sequential


@pytest.fixture(autouse=True)
def clean_db():
    yield
    clear_db_serialized_dags()
    clear_db_import_errors()
    clear_db_dags()
    clear_db_dag_bundles()

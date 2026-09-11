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
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, select, update

from airflow import settings
from airflow.cli import cli_parser
from airflow.cli.commands.dag_command import dag_reserialize
from airflow.models import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

pytestmark = pytest.mark.db_test


@pytest.fixture
def bundle_files(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    configure_dag_bundles: Callable[[dict[str, Path]], AbstractContextManager[None]],
) -> Iterator[dict[str, Path]]:
    clear_db_dags()
    sources = {
        "first/repair.py": ["repair_a", "repair_b", "shared"],
        "first/healthy.py": ["healthy"],
        "second/other.py": ["other"],
        "second/stale.py": ["stale"],
    }
    sources.update(
        {f"first/healthy_{index}.py": [f"extra_{index}"] for index in range(getattr(request, "param", 0))}
    )
    for filename, dag_ids in sources.items():
        path = tmp_path / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "from airflow.sdk import DAG\n"
            "from airflow.providers.standard.operators.empty import EmptyOperator\n"
            f"with open({str(tmp_path / 'parsed.txt')!r}, 'a') as stream:\n"
            "    stream.write(__file__ + '\\n')\n"
            f"for dag_id in {dag_ids!r}:\n"
            "    with DAG(dag_id=dag_id, schedule=None) as dag:\n"
            "        EmptyOperator(task_id='task')\n"
        )
    bundles = {"first": tmp_path / "first", "second": tmp_path / "second"}
    with conf_vars({("core", "load_examples"): "False"}), configure_dag_bundles(bundles):
        dag_reserialize(cli_parser.get_parser().parse_args(["dags", "reserialize"]))
        (tmp_path / "parsed.txt").unlink()
        yield bundles
    clear_db_dags()


def test_reserialize_restores_missing_row_with_existing_version(bundle_files: dict[str, Path]) -> None:
    with create_session() as session:
        version_id = session.scalar(select(DagVersion.id).where(DagVersion.dag_id == "repair_a"))
        session.execute(delete(SerializedDagModel).where(SerializedDagModel.dag_id == "repair_a"))

    dag_reserialize(cli_parser.get_parser().parse_args(["dags", "reserialize"]))

    with create_session() as session:
        assert (
            session.scalar(
                select(SerializedDagModel.dag_version_id).where(SerializedDagModel.dag_id == "repair_a")
            )
            == version_id
        )


@pytest.mark.parametrize("bundle_names", [[], ["first"], ["first", "second"]])
def test_only_missing_scopes_bundles_and_preserves_rows(
    bundle_files: dict[str, Path], bundle_names: list[str]
) -> None:
    with create_session() as session:
        session.execute(
            delete(SerializedDagModel).where(
                SerializedDagModel.dag_id.in_(["repair_a", "repair_b", "other", "stale"])
            )
        )
        session.execute(update(DagModel).where(DagModel.dag_id == "stale").values(is_stale=True))
        retained_rows = set(
            session.execute(
                select(SerializedDagModel.id, SerializedDagModel.dag_hash, SerializedDagModel.dag_version_id)
            )
        )
    args = ["dags", "reserialize", "--only-missing"]
    for name in bundle_names:
        args.extend(["--bundle-name", name])
    dag_reserialize(cli_parser.get_parser().parse_args(args))

    expected_ids = {"repair_a", "repair_b", "shared", "healthy"}
    expected_files = [str(bundle_files["first"] / "repair.py")]
    if not bundle_names or "second" in bundle_names:
        expected_ids.add("other")
        expected_files.append(str(bundle_files["second"] / "other.py"))
    with create_session() as session:
        assert set(session.scalars(select(SerializedDagModel.dag_id))) == expected_ids
        assert retained_rows <= set(
            session.execute(
                select(SerializedDagModel.id, SerializedDagModel.dag_hash, SerializedDagModel.dag_version_id)
            )
        )
    trace = bundle_files["first"].parent / "parsed.txt"
    assert sorted(trace.read_text().splitlines()) == sorted(expected_files)
    trace.unlink()
    dag_reserialize(cli_parser.get_parser().parse_args(args))
    assert not trace.exists()


@pytest.mark.parametrize("source_state", ["missing", "import_error"])
def test_only_missing_preserves_progress_when_a_file_is_unreadable(
    bundle_files: dict[str, Path], source_state: str
) -> None:
    with create_session() as session:
        session.execute(
            delete(SerializedDagModel).where(SerializedDagModel.dag_id.in_(["repair_a", "other"]))
        )
    source = bundle_files["first"] / "repair.py"
    if source_state == "missing":
        source.unlink()
    else:
        source.write_text("from airflow.sdk import DAG\nraise RuntimeError('unavailable dependency')\n")
    with pytest.raises(SystemExit, match="Failed to reserialize 1 Dag file"):
        dag_reserialize(cli_parser.get_parser().parse_args(["dags", "reserialize", "--only-missing"]))
    with create_session() as session:
        assert set(session.scalars(select(SerializedDagModel.dag_id))) == {
            "repair_b",
            "shared",
            "healthy",
            "other",
            "stale",
        }


def test_only_missing_uses_current_bundle_path(
    bundle_files: dict[str, Path],
    configure_dag_bundles: Callable[[dict[str, Path]], AbstractContextManager[None]],
) -> None:
    with create_session() as session:
        session.execute(delete(SerializedDagModel).where(SerializedDagModel.dag_id == "repair_a"))
    relocated = bundle_files["first"].with_name("relocated")
    bundle_files["first"].rename(relocated)

    with configure_dag_bundles({**bundle_files, "first": relocated}):
        dag_reserialize(cli_parser.get_parser().parse_args(["dags", "reserialize", "--only-missing"]))

    with create_session() as session:
        assert "repair_a" in set(session.scalars(select(SerializedDagModel.dag_id)))
        assert session.scalar(select(DagModel.fileloc).where(DagModel.dag_id == "repair_a")) == str(
            relocated / "repair.py"
        )


@pytest.mark.parametrize("bundle_kind", ["file", "zip"])
def test_only_missing_supports_file_bundles(
    bundle_files: dict[str, Path],
    bundle_kind: str,
    test_zip_path: str,
    configure_dag_bundles: Callable[[dict[str, Path]], AbstractContextManager[None]],
) -> None:
    source = Path(test_zip_path) if bundle_kind == "zip" else bundle_files["first"] / "repair.py"
    with configure_dag_bundles({"selected": source}):
        dag_reserialize(
            cli_parser.get_parser().parse_args(["dags", "reserialize", "--bundle-name", "selected"])
        )
        with create_session() as session:
            selected_ids = set(
                session.scalars(select(DagModel.dag_id).where(DagModel.bundle_name == "selected"))
            )
            assert selected_ids
            session.execute(delete(SerializedDagModel).where(SerializedDagModel.dag_id.in_(selected_ids)))

        dag_reserialize(
            cli_parser.get_parser().parse_args(
                ["dags", "reserialize", "--only-missing", "--bundle-name", "selected"]
            )
        )

        with create_session() as session:
            assert selected_ids <= set(session.scalars(select(SerializedDagModel.dag_id)))


@pytest.mark.parametrize("bundle_files", [0, 64], indirect=True, ids=["small", "many-files"])
@pytest.mark.parametrize("only_missing", [False, True], ids=["full", "only-missing"])
def test_reserialize_native_cli(bundle_files: dict[str, Path], only_missing: bool) -> None:
    with create_session() as session:
        expected_ids = set(session.scalars(select(SerializedDagModel.dag_id)))
        session.execute(
            delete(SerializedDagModel).where(SerializedDagModel.dag_id.in_(["repair_a", "repair_b"]))
        )
        retained_rows = set(
            session.execute(
                select(SerializedDagModel.id, SerializedDagModel.dag_hash, SerializedDagModel.dag_version_id)
            )
        )
    command = [sys.executable, "-m", "airflow", "dags", "reserialize"]
    if only_missing:
        command.append("--only-missing")
    assert settings.SQL_ALCHEMY_CONN is not None
    result = subprocess.run(
        args=command,
        env={
            **os.environ,
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": settings.SQL_ALCHEMY_CONN,
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(
                [
                    {
                        "name": name,
                        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                        "kwargs": {"path": str(path)},
                    }
                    for name, path in bundle_files.items()
                ]
            ),
        },
        text=True,
        capture_output=True,
        check=True,
    )
    print(result.stdout, end="")
    print(result.stderr, end="")
    parsed_files = len((bundle_files["first"].parent / "parsed.txt").read_text().splitlines())
    with create_session() as session:
        dag_ids = set(session.scalars(select(SerializedDagModel.dag_id)))
        assert retained_rows <= set(
            session.execute(
                select(SerializedDagModel.id, SerializedDagModel.dag_hash, SerializedDagModel.dag_version_id)
            )
        )
    print(
        json.dumps(
            {
                "mode": "only-missing" if only_missing else "full",
                "parsed_files": parsed_files,
                "serialized_dags": len(dag_ids),
                "retained_rows_unchanged": True,
            }
        )
    )
    assert dag_ids == expected_ids
    assert parsed_files == (
        1 if only_missing else sum(len(list(path.glob("*.py"))) for path in bundle_files.values())
    )

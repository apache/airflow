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

import importlib.util
from pathlib import Path

from sqlalchemy.exc import OperationalError

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

MIGRATION_PATH = Path(
    AIRFLOW_CORE_SOURCES_PATH,
    "airflow/migrations/versions/0101_3_2_0_ui_improvements_for_deadlines.py",
)


def load_migration_module():
    spec = importlib.util.spec_from_file_location(
        "migration_0101_ui_improvements_for_deadlines", MIGRATION_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def configure_migration(mocker, migration_module, *, dialect_name: str, indexes: list[dict]):
    conn = mocker.MagicMock()
    conn.dialect.name = dialect_name

    inspector = mocker.MagicMock()
    inspector.get_indexes.return_value = indexes
    inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}
    inspector.get_unique_constraints.return_value = []
    mocker.patch.object(migration_module.sa, "inspect", return_value=inspector)

    fake_op = mocker.MagicMock()
    fake_op.get_bind.return_value = conn
    mocker.patch.object(migration_module, "op", fake_op)
    return fake_op


def test_temporary_index_skips_existing_matching_index(mocker):
    migration_module = load_migration_module()
    fake_op = configure_migration(
        mocker,
        migration_module,
        dialect_name="mysql",
        indexes=[{"name": "deadline_dagrun_id_fkey", "column_names": ["dagrun_id"]}],
    )
    wrapped = migration_module.temporary_index("tmp_deadline_dagrun_id_idx", "deadline", ["dagrun_id"])(
        mocker.Mock(return_value="ok")
    )

    assert wrapped() == "ok"
    fake_op.create_index.assert_not_called()
    fake_op.drop_index.assert_not_called()


def test_temporary_index_creates_and_drops_when_missing(mocker):
    migration_module = load_migration_module()
    fake_op = configure_migration(mocker, migration_module, dialect_name="postgresql", indexes=[])
    wrapped = migration_module.temporary_index("tmp_deadline_dagrun_id_idx", "deadline", ["dagrun_id"])(
        mocker.Mock(return_value="ok")
    )

    assert wrapped() == "ok"
    fake_op.create_index.assert_called_once_with("tmp_deadline_dagrun_id_idx", "deadline", ["dagrun_id"])
    fake_op.drop_index.assert_called_once_with("tmp_deadline_dagrun_id_idx", table_name="deadline")


def test_temporary_index_keeps_mysql_index_when_drop_is_blocked_by_foreign_key(mocker):
    migration_module = load_migration_module()
    fake_op = configure_migration(mocker, migration_module, dialect_name="mysql", indexes=[])
    log_warning = mocker.patch.object(migration_module.log, "warning")
    fake_op.drop_index.side_effect = OperationalError(
        "DROP INDEX tmp_deadline_dagrun_id_idx ON deadline",
        {},
        type(
            "MySQLError",
            (),
            {
                "args": (
                    1553,
                    "Cannot drop index 'tmp_deadline_dagrun_id_idx': needed in a foreign key constraint",
                )
            },
        )(),
    )
    wrapped = migration_module.temporary_index("tmp_deadline_dagrun_id_idx", "deadline", ["dagrun_id"])(
        mocker.Mock(return_value="ok")
    )

    assert wrapped() == "ok"
    fake_op.create_index.assert_called_once_with("tmp_deadline_dagrun_id_idx", "deadline", ["dagrun_id"])
    fake_op.drop_index.assert_called_once_with("tmp_deadline_dagrun_id_idx", table_name="deadline")
    log_warning.assert_called_once()

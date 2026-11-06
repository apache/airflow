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

import os
import subprocess
import sys

import pytest

from airflow.models.base import get_id_collation_args

from tests_common.test_utils.config import conf_vars


@pytest.mark.db_test
@pytest.mark.parametrize(
    ("dsn", "expected", "extra"),
    [
        pytest.param("postgresql+psycopg2://host/the_database", {}, {}, id="postgres"),
        pytest.param("mysql://host/the_database", {"collation": "utf8mb3_bin"}, {}, id="mysql"),
        pytest.param(
            "mysql+pymysql://host/the_database", {"collation": "utf8mb3_bin"}, {}, id="mysql+pymysql"
        ),
        pytest.param(
            "mysql://host/the_database",
            {"collation": "ascii"},
            {("database", "sql_engine_collation_for_ids"): "ascii"},
            id="mysql with explicit config",
        ),
        pytest.param(
            "postgresql+psycopg2://host/the_database",
            {"collation": "ascii"},
            {("database", "sql_engine_collation_for_ids"): "ascii"},
            id="postgres with explicit config",
        ),
    ],
)
def test_collation(dsn, expected, extra):
    with conf_vars({("database", "sql_alchemy_conn"): dsn, **extra}):
        assert expected == get_id_collation_args()


def test_models_configure_in_process_that_skips_orm_setup():
    # Models name each other in string relationships, which resolve only once every model is imported.
    # This process never calls configure_orm(), so building the first model must import the rest.
    code = "from airflow.models.connection import Connection; Connection(conn_id='test', conn_type='http')"

    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "PYTHON_OPERATORS_VIRTUAL_ENV_MODE": "1"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_local_settings_can_build_models_while_airflow_is_imported(tmp_path):
    # airflow_local_settings is imported before configure_orm() runs.
    (tmp_path / "airflow_local_settings.py").write_text(
        "from sqlalchemy.orm import configure_mappers\n"
        "from airflow.models.team import Team\n"
        "configure_mappers()\n"
    )
    python_path = os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH")]))

    result = subprocess.run(
        [sys.executable, "-c", "import airflow"],
        env={**os.environ, "PYTHONPATH": python_path},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr

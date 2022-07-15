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
#
from unittest import mock

import pytest
import requests

from airflow.providers.openmldb.operators.openmldb import (
    Mode,
    OpenMLDBDeployOperator,
    OpenMLDBLoadDataOperator,
    OpenMLDBSelectIntoOperator,
    OpenMLDBSQLOperator,
)

MOCK_TASK_ID = "test-openmldb-operator"
MOCK_DB = "mock_db"
MOCK_TABLE = "mock_table"
MOCK_FILE = "mock_file_name"
MOCK_OPENMLDB_CONN_ID = "mock_openmldb_conn_default"

MOCK_KEYS = ["mock_key1", "mock_key_2", "mock_key3"]
MOCK_CONTENT = "mock_content"


class TestOpenMLDBLoadDataOperator:
    @mock.patch("airflow.providers.openmldb.operators.openmldb.OpenMLDBHook")
    def test_execute(self, mock_hook):
        operator = OpenMLDBLoadDataOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            table=MOCK_TABLE,
            file=MOCK_FILE,
            disable_response_check=True,
        )
        operator.execute({})
        mock_hook.assert_called_once_with(openmldb_conn_id=MOCK_OPENMLDB_CONN_ID)
        mock_hook.return_value.submit_job.assert_called_once_with(
            db=MOCK_DB,
            mode=Mode.OFFSYNC.value,
            sql=f"LOAD DATA INFILE '{MOCK_FILE}' INTO " f"TABLE {MOCK_TABLE}",
        )

    @mock.patch("airflow.providers.openmldb.operators.openmldb.OpenMLDBHook")
    def test_execute_with_options(self, mock_hook):
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"code": 0, "msg": "ok"}'
        mock_hook.return_value.submit_job.return_value = response

        options = "mode='overwrite'"
        operator = OpenMLDBLoadDataOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            table=MOCK_TABLE,
            file=MOCK_FILE,
            options=options,
        )
        operator.execute({})
        mock_hook.assert_called_once_with(openmldb_conn_id=MOCK_OPENMLDB_CONN_ID)
        mock_hook.return_value.submit_job.assert_called_once_with(
            db=MOCK_DB,
            mode=Mode.OFFSYNC.value,
            sql=f"LOAD DATA INFILE '{MOCK_FILE}' INTO " f"TABLE {MOCK_TABLE} OPTIONS" f"({options})",
        )


class TestOpenMLDBSelectOutOperator:
    @mock.patch("airflow.providers.openmldb.operators.openmldb.OpenMLDBHook")
    def test_execute(self, mock_hook):
        fe_sql = (
            "SELECT id, ts, sum(c1) over w1 FROM t1 WINDOW w1 as "
            "(PARTITION BY id ORDER BY ts BETWEEN 20s PRECEDING AND CURRENT ROW)"
        )
        operator = OpenMLDBSelectIntoOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            sql=fe_sql,
            file=MOCK_FILE,
            disable_response_check=True,
        )
        operator.execute({})
        mock_hook.assert_called_once_with(openmldb_conn_id=MOCK_OPENMLDB_CONN_ID)
        mock_hook.return_value.submit_job.assert_called_once_with(
            db=MOCK_DB, mode=Mode.OFFSYNC.value, sql=f"{fe_sql} INTO OUTFILE '{MOCK_FILE}'"
        )

    @mock.patch("airflow.providers.openmldb.operators.openmldb.OpenMLDBHook")
    def test_execute_with_options(self, mock_hook):
        fe_sql = (
            "SELECT id, ts, sum(c1) over w1 FROM t1 WINDOW w1 as "
            "(PARTITION BY id ORDER BY ts BETWEEN 20s PRECEDING AND CURRENT ROW)"
        )
        options = "mode='errorifexists, delimiter='-'"
        operator = OpenMLDBSelectIntoOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=Mode.OFFSYNC,
            sql=fe_sql,
            file=MOCK_FILE,
            options=options,
            disable_response_check=True,
        )
        operator.execute({})
        mock_hook.assert_called_once_with(openmldb_conn_id=MOCK_OPENMLDB_CONN_ID)
        mock_hook.return_value.submit_job.assert_called_once_with(
            db=MOCK_DB,
            mode=Mode.OFFSYNC.value,
            sql=f"{fe_sql} INTO OUTFILE '{MOCK_FILE}' " f"OPTIONS({options})",
        )


class TestOpenMLDBDeployOperator:
    @mock.patch("airflow.providers.openmldb.operators.openmldb.OpenMLDBHook")
    def test_execute(self, mock_hook):
        fe_sql = (
            "SELECT id, ts, sum(c1) over w1 FROM t1 WINDOW w1 as "
            "(PARTITION BY id ORDER BY ts BETWEEN 20s PRECEDING AND CURRENT ROW)"
        )
        deploy_name = "demo"
        operator = OpenMLDBDeployOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            deploy_name=deploy_name,
            sql=fe_sql,
            disable_response_check=True,
        )
        operator.execute({})
        mock_hook.assert_called_once_with(openmldb_conn_id=MOCK_OPENMLDB_CONN_ID)
        mock_hook.return_value.submit_job.assert_called_once_with(
            db=MOCK_DB, mode=Mode.ONLINE.value, sql=f"DEPLOY {deploy_name} {fe_sql}"
        )


class TestOpenMLDBSQLOperator:
    @mock.patch("airflow.providers.openmldb.operators.openmldb.OpenMLDBHook")
    @pytest.mark.parametrize(
        "sql, mode",
        [
            ("create database if not exists test_db", Mode.OFFSYNC),
            ("SHOW JOBS", Mode.ONLINE),
            ("SELECT 1", Mode.OFFSYNC),
            ("SELECT 1", Mode.ONLINE),
        ],
    )
    def test_execute(self, mock_hook, sql, mode):
        operator = OpenMLDBSQLOperator(
            task_id=MOCK_TASK_ID,
            openmldb_conn_id=MOCK_OPENMLDB_CONN_ID,
            db=MOCK_DB,
            mode=mode,
            sql=sql,
            disable_response_check=True,
        )
        operator.execute({})
        mock_hook.assert_called_once_with(openmldb_conn_id=MOCK_OPENMLDB_CONN_ID)
        mock_hook.return_value.submit_job.assert_called_once_with(db=MOCK_DB, mode=mode.value, sql=sql)

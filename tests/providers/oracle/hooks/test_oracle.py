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
from datetime import datetime
from unittest import mock

import numpy as np
import oracledb
import pytest

from airflow.models import Connection
from airflow.providers.oracle.hooks.oracle import OracleHook


class TestOracleHookConn:
    def setup_method(self):
        self.connection = Connection(
            login="login", password="password", host="host", port=1521, extra='{"service_name": "schema"}'
        )

        self.db_hook = OracleHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_get_conn_host(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["dsn"] == oracledb.makedsn("host", 1521, service_name="schema")

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_get_conn_host_alternative_port(self, mock_connect):
        self.connection.port = 1522
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["dsn"] == oracledb.makedsn("host", self.connection.port, service_name="schema")

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_get_conn_sid(self, mock_connect):
        dsn_sid = {"dsn": "ignored", "sid": "sid"}
        self.connection.extra = json.dumps(dsn_sid)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["dsn"] == oracledb.makedsn("host", self.connection.port, dsn_sid["sid"])

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_get_conn_service_name(self, mock_connect):
        dsn_service_name = {"dsn": "ignored", "service_name": "service_name"}
        self.connection.extra = json.dumps(dsn_service_name)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["dsn"] == oracledb.makedsn(
            "host", self.connection.port, service_name=dsn_service_name["service_name"]
        )

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_get_conn_mode(self, mock_connect):
        mode = {
            "sysdba": oracledb.AUTH_MODE_SYSDBA,
            "sysasm": oracledb.AUTH_MODE_SYSASM,
            "sysoper": oracledb.AUTH_MODE_SYSOPER,
            "sysbkp": oracledb.AUTH_MODE_SYSBKP,
            "sysdgd": oracledb.AUTH_MODE_SYSDGD,
            "syskmt": oracledb.AUTH_MODE_SYSKMT,
        }
        first = True
        for mod in mode:
            self.connection.extra = json.dumps({"mode": mod})
            self.db_hook.get_conn()
            if first:
                assert mock_connect.call_count == 1
                first = False
            args, kwargs = mock_connect.call_args
            assert args == ()
            assert kwargs["mode"] == mode.get(mod)

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_get_conn_events(self, mock_connect):
        self.connection.extra = json.dumps({"events": True})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["events"] is True

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_get_conn_purity(self, mock_connect):
        purity = {
            "new": oracledb.PURITY_NEW,
            "self": oracledb.PURITY_SELF,
            "default": oracledb.PURITY_DEFAULT,
        }
        first = True
        for pur in purity:
            self.connection.extra = json.dumps({"purity": pur})
            self.db_hook.get_conn()
            if first:
                assert mock_connect.call_count == 1
                first = False
            args, kwargs = mock_connect.call_args
            assert args == ()
            assert kwargs["purity"] == purity.get(pur)

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_set_current_schema(self, mock_connect):
        self.connection.schema = "schema_name"
        self.connection.extra = json.dumps({"service_name": "service_name"})
        assert self.db_hook.get_conn().current_schema == self.connection.schema

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.init_oracle_client")
    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_set_thick_mode_extra(self, mock_connect, mock_init_client):
        thick_mode_test = {
            "thick_mode": True,
            "thick_mode_lib_dir": "/opt/oracle/instantclient",
            "thick_mode_config_dir": "/opt/oracle/config",
        }
        self.connection.extra = json.dumps(thick_mode_test)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert mock_init_client.call_count == 1
        args, kwargs = mock_init_client.call_args
        assert args == ()
        assert kwargs["lib_dir"] == thick_mode_test["thick_mode_lib_dir"]
        assert kwargs["config_dir"] == thick_mode_test["thick_mode_config_dir"]

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.init_oracle_client")
    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_set_thick_mode_extra_str(self, mock_connect, mock_init_client):
        thick_mode_test = {"thick_mode": "True"}
        self.connection.extra = json.dumps(thick_mode_test)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert mock_init_client.call_count == 1

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.init_oracle_client")
    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_set_thick_mode_params(self, mock_connect, mock_init_client):
        # Verify params overrides connection config extra
        thick_mode_test = {
            "thick_mode": False,
            "thick_mode_lib_dir": "/opt/oracle/instantclient",
            "thick_mode_config_dir": "/opt/oracle/config",
        }
        self.connection.extra = json.dumps(thick_mode_test)
        db_hook = OracleHook(thick_mode=True, thick_mode_lib_dir="/test", thick_mode_config_dir="/test_conf")
        db_hook.get_connection = mock.Mock()
        db_hook.get_connection.return_value = self.connection
        db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert mock_init_client.call_count == 1
        args, kwargs = mock_init_client.call_args
        assert args == ()
        assert kwargs["lib_dir"] == "/test"
        assert kwargs["config_dir"] == "/test_conf"

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.init_oracle_client")
    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_thick_mode_defaults_to_false(self, mock_connect, mock_init_client):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert mock_init_client.call_count == 0

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.init_oracle_client")
    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_thick_mode_dirs_defaults(self, mock_connect, mock_init_client):
        thick_mode_test = {"thick_mode": True}
        self.connection.extra = json.dumps(thick_mode_test)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert mock_init_client.call_count == 1
        args, kwargs = mock_init_client.call_args
        assert args == ()
        assert kwargs["lib_dir"] is None
        assert kwargs["config_dir"] is None

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_oracledb_defaults_attributes_default_values(self, mock_connect):
        default_fetch_decimals = oracledb.defaults.fetch_decimals
        default_fetch_lobs = oracledb.defaults.fetch_lobs
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        # Check that OracleHook.get_conn() doesn't try to set defaults if not provided
        assert oracledb.defaults.fetch_decimals == default_fetch_decimals
        assert oracledb.defaults.fetch_lobs == default_fetch_lobs

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_set_oracledb_defaults_attributes_extra(self, mock_connect):
        defaults_test = {"fetch_decimals": True, "fetch_lobs": False}
        self.connection.extra = json.dumps(defaults_test)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert oracledb.defaults.fetch_decimals == defaults_test["fetch_decimals"]
        assert oracledb.defaults.fetch_lobs == defaults_test["fetch_lobs"]

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_set_oracledb_defaults_attributes_extra_str(self, mock_connect):
        defaults_test = {"fetch_decimals": "True", "fetch_lobs": "False"}
        self.connection.extra = json.dumps(defaults_test)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert oracledb.defaults.fetch_decimals is True
        assert oracledb.defaults.fetch_lobs is False

    @mock.patch("airflow.providers.oracle.hooks.oracle.oracledb.connect")
    def test_set_oracledb_defaults_attributes_params(self, mock_connect):
        # Verify params overrides connection config extra
        defaults_test = {"fetch_decimals": False, "fetch_lobs": True}
        self.connection.extra = json.dumps(defaults_test)
        db_hook = OracleHook(fetch_decimals=True, fetch_lobs=False)
        db_hook.get_connection = mock.Mock()
        db_hook.get_connection.return_value = self.connection
        db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert oracledb.defaults.fetch_decimals is True
        assert oracledb.defaults.fetch_lobs is False

    def test_type_checking_thick_mode_lib_dir(self):
        thick_mode_lib_dir_test = {"thick_mode": True, "thick_mode_lib_dir": 1}
        self.connection.extra = json.dumps(thick_mode_lib_dir_test)
        with pytest.raises(TypeError, match=r"thick_mode_lib_dir expected str or None, got.*"):
            self.db_hook.get_conn()

    def test_type_checking_thick_mode_config_dir(self):
        thick_mode_config_dir_test = {"thick_mode": True, "thick_mode_config_dir": 1}
        self.connection.extra = json.dumps(thick_mode_config_dir_test)
        with pytest.raises(TypeError, match=r"thick_mode_config_dir expected str or None, got.*"):
            self.db_hook.get_conn()


class TestOracleHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestOracleHook(OracleHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestOracleHook()

    def test_run_without_parameters(self):
        sql = "SQL"
        self.db_hook.run(sql)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.called

    def test_run_with_parameters(self):
        sql = "SQL"
        param = ("p1", "p2")
        self.db_hook.run(sql, parameters=param)
        self.cur.execute.assert_called_once_with(sql, param)
        assert self.conn.commit.called

    def test_insert_rows_with_fields(self):
        rows = [
            (
                "'basestr_with_quote",
                None,
                np.NAN,
                np.datetime64("2019-01-24T01:02:03"),
                datetime(2019, 1, 24),
                1,
                10.24,
                "str",
            )
        ]
        target_fields = [
            "basestring",
            "none",
            "numpy_nan",
            "numpy_datetime64",
            "datetime",
            "int",
            "float",
            "str",
        ]
        self.db_hook.insert_rows("table", rows, target_fields)
        self.cur.execute.assert_called_once_with(
            "INSERT /*+ APPEND */ INTO table "
            "(basestring, none, numpy_nan, numpy_datetime64, datetime, int, float, str) "
            "VALUES ('''basestr_with_quote',NULL,NULL,'2019-01-24T01:02:03',"
            "to_date('2019-01-24 00:00:00','YYYY-MM-DD HH24:MI:SS'),1,10.24,'str')"
        )

    def test_insert_rows_without_fields(self):
        rows = [
            (
                "'basestr_with_quote",
                None,
                np.NAN,
                np.datetime64("2019-01-24T01:02:03"),
                datetime(2019, 1, 24),
                1,
                10.24,
                "str",
            )
        ]
        self.db_hook.insert_rows("table", rows)
        self.cur.execute.assert_called_once_with(
            "INSERT /*+ APPEND */ INTO table "
            " VALUES ('''basestr_with_quote',NULL,NULL,'2019-01-24T01:02:03',"
            "to_date('2019-01-24 00:00:00','YYYY-MM-DD HH24:MI:SS'),1,10.24,'str')"
        )

    def test_bulk_insert_rows_with_fields(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        target_fields = ["col1", "col2", "col3"]
        self.db_hook.bulk_insert_rows("table", rows, target_fields)
        self.cur.prepare.assert_called_once_with("insert into table (col1, col2, col3) values (:1, :2, :3)")
        self.cur.executemany.assert_called_once_with(None, rows)

    def test_bulk_insert_rows_with_commit_every(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        target_fields = ["col1", "col2", "col3"]
        self.db_hook.bulk_insert_rows("table", rows, target_fields, commit_every=2)
        calls = [
            mock.call("insert into table (col1, col2, col3) values (:1, :2, :3)"),
            mock.call("insert into table (col1, col2, col3) values (:1, :2, :3)"),
        ]
        self.cur.prepare.assert_has_calls(calls)
        calls = [
            mock.call(None, rows[:2]),
            mock.call(None, rows[2:]),
        ]
        self.cur.executemany.assert_has_calls(calls, any_order=True)

    def test_bulk_insert_rows_without_fields(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        self.db_hook.bulk_insert_rows("table", rows)
        self.cur.prepare.assert_called_once_with("insert into table  values (:1, :2, :3)")
        self.cur.executemany.assert_called_once_with(None, rows)

    def test_bulk_insert_rows_no_rows(self):
        rows = []
        with pytest.raises(ValueError):
            self.db_hook.bulk_insert_rows("table", rows)


    def test_bulk_insert_sequence_field(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        target_fields = ["col1", "col2", "col3"]
        sequence_column = "id"
        sequence_name = "my_sequence"
        self.db_hook.bulk_insert_rows("table", rows, target_fields, sequence_column=sequence_column, sequence_name=sequence_name)
        self.cur.prepare.assert_called_once_with("insert into table (id, col1, col2, col3) values (my_sequence.NEXTVAL, :1, :2, :3)")
        self.cur.executemany.assert_called_once_with(None, rows)

    def test_callproc_none(self):
        parameters = None

        class bindvar(int):
            def getvalue(self):
                return self

        self.cur.bindvars = None
        result = self.db_hook.callproc("proc", True, parameters)
        assert self.cur.execute.mock_calls == [mock.call("BEGIN proc(); END;")]
        assert result == parameters

    def test_callproc_dict(self):
        parameters = {"a": 1, "b": 2, "c": 3}

        class bindvar(int):
            def getvalue(self):
                return self

        self.cur.bindvars = {k: bindvar(v) for k, v in parameters.items()}
        result = self.db_hook.callproc("proc", True, parameters)
        assert self.cur.execute.mock_calls == [mock.call("BEGIN proc(:a,:b,:c); END;", parameters)]
        assert result == parameters

    def test_callproc_list(self):
        parameters = [1, 2, 3]

        class bindvar(int):
            def getvalue(self):
                return self

        self.cur.bindvars = list(map(bindvar, parameters))
        result = self.db_hook.callproc("proc", True, parameters)
        assert self.cur.execute.mock_calls == [mock.call("BEGIN proc(:1,:2,:3); END;", parameters)]
        assert result == parameters

    def test_callproc_out_param(self):
        parameters = [1, int, float, bool, str]

        def bindvar(value):
            m = mock.Mock()
            m.getvalue.return_value = value
            return m

        self.cur.bindvars = [bindvar(p() if type(p) is type else p) for p in parameters]
        result = self.db_hook.callproc("proc", True, parameters)
        expected = [1, 0, 0.0, False, ""]
        assert self.cur.execute.mock_calls == [mock.call("BEGIN proc(:1,:2,:3,:4,:5); END;", expected)]
        assert result == expected

    def test_test_connection_use_dual_table(self):
        status, message = self.db_hook.test_connection()
        self.cur.execute.assert_called_once_with("select 1 from dual")
        assert status is True
        assert message == "Connection successfully tested"

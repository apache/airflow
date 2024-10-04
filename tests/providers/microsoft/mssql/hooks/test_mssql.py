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
from urllib.parse import quote_plus

import pytest

from airflow.models import Connection
from tests.providers.microsoft.conftest import load_file

try:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
except ImportError:
    pytest.skip("MSSQL not available", allow_module_level=True)

PYMSSQL_CONN = Connection(
    conn_type="mssql", host="ip", schema="share", login="username", password="password", port=8081
)
PYMSSQL_CONN_ALT = Connection(
    conn_type="mssql", host="ip", schema="", login="username", password="password", port=8081
)
PYMSSQL_CONN_ALT_1 = Connection(
    conn_type="mssql",
    host="ip",
    schema="",
    login="username",
    password="password",
    port=8081,
    extra={"SQlalchemy_Scheme": "mssql+testdriver"},
)
PYMSSQL_CONN_ALT_2 = Connection(
    conn_type="mssql",
    host="ip",
    schema="",
    login="username",
    password="password",
    port=8081,
    extra={"SQlalchemy_Scheme": "mssql+testdriver", "myparam": "5@-//*"},
)


def get_column_names(self, table: str) -> list[str] | None:
    return [
        "ReportRefreshDate",
        "UserId",
        "UserPrincipalName",
        "LastActivityDate",
        "IsDeleted",
        "DeletedDate",
        "AssignedProducts",
        "TeamChatMessageCount",
        "PrivateChatMessageCount",
        "CallCount",
        "MeetingCount",
        "MeetingsOrganizedCount",
        "MeetingsAttendedCount",
        "AdHocMeetingsOrganizedCount",
        "AdHocMeetingsAttendedCount",
        "ScheduledOne-timeMeetingsOrganizedCount",
        "ScheduledOne-timeMeetingsAttendedCount",
        "ScheduledRecurringMeetingsOrganizedCount",
        "ScheduledRecurringMeetingsAttendedCount",
        "AudioDuration",
        "VideoDuration",
        "ScreenShareDuration",
        "AudioDurationInSeconds",
        "VideoDurationInSeconds",
        "ScreenShareDurationInSeconds",
        "HasOtherAction",
        "UrgentMessages",
        "PostMessages",
        "TenantDisplayName",
        "SharedChannelTenantDisplayNames",
        "ReplyMessages",
        "IsLicensed",
        "ReportPeriod",
        "LoadDate",
    ]


def get_primary_keys(self, table: str) -> list[str] | None:
    return [
        "GroupDisplayName",
        "OwnerPrincipalName",
        "ReportPeriod",
        "ReportRefreshDate",
    ]


class TestMsSqlHook:
    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_connection")
    def test_get_conn_should_return_connection(self, get_connection, mssql_get_conn):
        get_connection.return_value = PYMSSQL_CONN
        mssql_get_conn.return_value = mock.Mock()

        hook = MsSqlHook()
        conn = hook.get_conn()

        assert mssql_get_conn.return_value == conn
        mssql_get_conn.assert_called_once()

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_connection")
    def test_set_autocommit_should_invoke_autocommit(self, get_connection, mssql_get_conn):
        get_connection.return_value = PYMSSQL_CONN
        mssql_get_conn.return_value = mock.Mock()
        autocommit_value = mock.Mock()

        hook = MsSqlHook()
        conn = hook.get_conn()

        hook.set_autocommit(conn, autocommit_value)
        mssql_get_conn.assert_called_once()
        mssql_get_conn.return_value.autocommit.assert_called_once_with(autocommit_value)

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_connection")
    def test_get_autocommit_should_return_autocommit_state(self, get_connection, mssql_get_conn):
        get_connection.return_value = PYMSSQL_CONN
        mssql_get_conn.return_value = mock.Mock()
        mssql_get_conn.return_value.autocommit_state = "autocommit_state"

        hook = MsSqlHook()
        conn = hook.get_conn()

        mssql_get_conn.assert_called_once()
        assert hook.get_autocommit(conn) == "autocommit_state"

    @pytest.mark.parametrize(
        "conn, exp_uri",
        [
            (
                PYMSSQL_CONN,
                (
                    "mssql+pymssql://"
                    f"{quote_plus(PYMSSQL_CONN.login)}:{quote_plus(PYMSSQL_CONN.password)}"
                    f"@{PYMSSQL_CONN.host}:{PYMSSQL_CONN.port}/{PYMSSQL_CONN.schema}"
                ),
            ),
            (
                PYMSSQL_CONN_ALT,
                (
                    "mssql+pymssql://"
                    f"{quote_plus(PYMSSQL_CONN_ALT.login)}:{quote_plus(PYMSSQL_CONN_ALT.password)}"
                    f"@{PYMSSQL_CONN_ALT.host}:{PYMSSQL_CONN_ALT.port}"
                ),
            ),
            (
                PYMSSQL_CONN_ALT_1,
                (
                    f"{PYMSSQL_CONN_ALT_1.extra_dejson['SQlalchemy_Scheme']}://"
                    f"{quote_plus(PYMSSQL_CONN_ALT.login)}:{quote_plus(PYMSSQL_CONN_ALT.password)}"
                    f"@{PYMSSQL_CONN_ALT.host}:{PYMSSQL_CONN_ALT.port}/"
                ),
            ),
            (
                PYMSSQL_CONN_ALT_2,
                (
                    f"{PYMSSQL_CONN_ALT_2.extra_dejson['SQlalchemy_Scheme']}://"
                    f"{quote_plus(PYMSSQL_CONN_ALT_2.login)}:{quote_plus(PYMSSQL_CONN_ALT_2.password)}"
                    f"@{PYMSSQL_CONN_ALT_2.host}:{PYMSSQL_CONN_ALT_2.port}/"
                    f"?myparam={quote_plus(PYMSSQL_CONN_ALT_2.extra_dejson['myparam'])}"
                ),
            ),
        ],
    )
    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_get_uri_driver_rewrite(self, get_connection, conn, exp_uri):
        get_connection.return_value = conn

        hook = MsSqlHook()
        res_uri = hook.get_uri()

        get_connection.assert_called()
        assert res_uri == exp_uri

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_sqlalchemy_scheme_is_default(self, get_connection):
        get_connection.return_value = PYMSSQL_CONN

        hook = MsSqlHook()
        assert hook.sqlalchemy_scheme == hook.DEFAULT_SQLALCHEMY_SCHEME

    @pytest.mark.db_test
    def test_sqlalchemy_scheme_is_from_hook(self):
        hook = MsSqlHook(sqlalchemy_scheme="mssql+mytestdriver")
        assert hook.sqlalchemy_scheme == "mssql+mytestdriver"

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_sqlalchemy_scheme_is_from_conn_extra(self, get_connection):
        get_connection.return_value = PYMSSQL_CONN_ALT_1

        hook = MsSqlHook()
        scheme = hook.sqlalchemy_scheme
        get_connection.assert_called()
        assert scheme == PYMSSQL_CONN_ALT_1.extra_dejson["SQlalchemy_Scheme"]

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_get_sqlalchemy_engine(self, get_connection):
        get_connection.return_value = PYMSSQL_CONN

        hook = MsSqlHook()
        hook.get_sqlalchemy_engine()

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    @mock.patch(
        "airflow.providers.microsoft.mssql.dialects.mssql.MsSqlDialect.get_column_names",
        get_column_names,
    )
    @mock.patch(
        "airflow.providers.microsoft.mssql.dialects.mssql.MsSqlDialect.get_primary_keys",
        get_primary_keys,
    )
    def test_generate_insert_sql(self, get_connection):
        get_connection.return_value = PYMSSQL_CONN

        hook = MsSqlHook()
        sql = hook._generate_insert_sql(
            table="YAMMER_GROUPS_ACTIVITY_DETAIL",
            values=[
                "2024-07-17",
                "daa5b44c-80d6-4e22-85b5-a94e04cf7206",
                "no-reply@microsoft.com",
                "2024-07-17",
                0,
                0.0,
                "MICROSOFT FABRIC (FREE)+MICROSOFT 365 E5",
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                "PT0S",
                "PT0S",
                "PT0S",
                0,
                0,
                0,
                "Yes",
                0,
                0,
                "APACHE",
                0.0,
                0,
                "Yes",
                1,
                "2024-07-17T00:00:00+00:00",
            ],
            replace=True,
        )
        assert sql == load_file("resources", "replace.sql")

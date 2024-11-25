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

from airflow.models import Connection

from providers.tests.microsoft.conftest import load_file

try:
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
except ImportError:
    pytest.skip("MSSQL not available", allow_module_level=True)


@pytest.fixture
def get_primary_keys():
    return [
        "GroupDisplayName",
        "OwnerPrincipalName",
        "ReportPeriod",
        "ReportRefreshDate",
    ]


@pytest.fixture
def mssql_connections():
    return {
        "default": Connection(
            conn_type="mssql", host="ip", schema="share", login="username", password="password", port=8081
        ),
        "alt": Connection(
            conn_type="mssql", host="ip", schema="", login="username", password="password", port=8081
        ),
        "alt_1": Connection(
            conn_type="mssql",
            host="ip",
            schema="",
            login="username",
            password="password",
            port=8081,
            extra={"SQlalchemy_Scheme": "mssql+testdriver"},
        ),
        "alt_2": Connection(
            conn_type="mssql",
            host="ip",
            schema="",
            login="username",
            password="password",
            port=8081,
            extra={"SQlalchemy_Scheme": "mssql+testdriver", "myparam": "5@-//*"},
        ),
    }


URI_TEST_CASES = [
    ("default", "mssql+pymssql://username:password@ip:8081/share"),
    ("alt", "mssql+pymssql://username:password@ip:8081"),
    ("alt_1", "mssql+testdriver://username:password@ip:8081/"),
    ("alt_2", "mssql+testdriver://username:password@ip:8081/?myparam=5%40-%2F%2F%2A"),
]


class TestMsSqlHook:
    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_connection")
    def test_get_conn_should_return_connection(self, get_connection, mssql_get_conn, mssql_connections):
        get_connection.return_value = mssql_connections["default"]
        mssql_get_conn.return_value = mock.Mock()

        hook = MsSqlHook()
        conn = hook.get_conn()

        assert mssql_get_conn.return_value == conn
        mssql_get_conn.assert_called_once()

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_connection")
    def test_set_autocommit_should_invoke_autocommit(self, get_connection, mssql_get_conn, mssql_connections):
        get_connection.return_value = mssql_connections["default"]
        mssql_get_conn.return_value = mock.Mock()
        autocommit_value = mock.Mock()

        hook = MsSqlHook()
        conn = hook.get_conn()

        hook.set_autocommit(conn, autocommit_value)
        mssql_get_conn.assert_called_once()
        mssql_get_conn.return_value.autocommit.assert_called_once_with(autocommit_value)

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_conn")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_connection")
    def test_get_autocommit_should_return_autocommit_state(
        self, get_connection, mssql_get_conn, mssql_connections
    ):
        get_connection.return_value = mssql_connections["default"]
        mssql_get_conn.return_value = mock.Mock()
        mssql_get_conn.return_value.autocommit_state = "autocommit_state"

        hook = MsSqlHook()
        conn = hook.get_conn()

        mssql_get_conn.assert_called_once()
        assert hook.get_autocommit(conn) == "autocommit_state"

    @pytest.mark.parametrize("conn_id,exp_uri", URI_TEST_CASES)
    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_get_uri_driver_rewrite(self, get_connection, mssql_connections, conn_id, exp_uri):
        get_connection.return_value = mssql_connections[conn_id]

        hook = MsSqlHook()
        res_uri = hook.get_uri()

        get_connection.assert_called()
        assert res_uri == exp_uri

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_sqlalchemy_scheme_is_default(self, get_connection, mssql_connections):
        get_connection.return_value = mssql_connections["default"]

        hook = MsSqlHook()
        assert hook.sqlalchemy_scheme == hook.DEFAULT_SQLALCHEMY_SCHEME

    @pytest.mark.db_test
    def test_sqlalchemy_scheme_is_from_hook(self):
        hook = MsSqlHook(sqlalchemy_scheme="mssql+mytestdriver")
        assert hook.sqlalchemy_scheme == "mssql+mytestdriver"

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_sqlalchemy_scheme_is_from_conn_extra(self, get_connection, mssql_connections):
        get_connection.return_value = mssql_connections["alt_1"]

        hook = MsSqlHook()
        scheme = hook.sqlalchemy_scheme
        get_connection.assert_called()
        assert scheme == mssql_connections["alt_1"].extra_dejson["SQlalchemy_Scheme"]

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_get_sqlalchemy_engine(self, get_connection, mssql_connections):
        get_connection.return_value = mssql_connections["default"]

        hook = MsSqlHook()
        hook.get_sqlalchemy_engine()

    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_generate_insert_sql(self, get_connection, mssql_connections, get_primary_keys):
        get_connection.return_value = mssql_connections["default"]

        hook = MsSqlHook()
        with mock.patch.object(hook, "get_primary_keys", return_value=get_primary_keys):
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
                target_fields=[
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
                ],
                replace=True,
            )
            assert sql == load_file("resources", "replace.sql")

    @pytest.mark.db_test
    @mock.patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.get_connection")
    def test_get_extra(self, get_connection, mssql_connections):
        get_connection.return_value = mssql_connections["alt_2"]

        hook = MsSqlHook()
        assert hook.get_connection().extra

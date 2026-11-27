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
from botocore.credentials import ReadOnlyCredentials
from sqlalchemy.engine import URL

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.athena_sql import AthenaSQLHook
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper

REGION_NAME = "us-east-1"
WORK_GROUP = "test-work-group"
SCHEMA_NAME = "athena_sql_schema"
AWS_ACCESS_KEY_ID = "aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "aws_secret_access_key"
AWS_SESSION_TOKEN = "aws_session_token"

AWS_CONN_ID = "aws_not_default"
AWS_ATHENA_CONN_ID = "aws_athena_not_default"


class TestAthenaSQLHookConn:
    def setup_method(self):
        conn = Connection(
            conn_type="athena",
            schema=SCHEMA_NAME,
            extra={"work_group": WORK_GROUP, "region_name": REGION_NAME},
        )
        self.conn_athena = AwsConnectionWrapper(conn)

        self.db_hook = AthenaSQLHook()

        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = conn

    @pytest.mark.parametrize(
        ("token", "expected_query"),
        [
            pytest.param(
                AWS_SESSION_TOKEN,
                "aws_session_token=aws_session_token&region_name=us-east-1&work_group=test-work-group",
                id="session-token",
            ),
            pytest.param(None, "region_name=us-east-1&work_group=test-work-group", id="no-session-token"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_credentials")
    def test_get_uri(self, mock_get_credentials, token, expected_query):
        mock_get_credentials.return_value = mock.Mock(
            access_key=AWS_ACCESS_KEY_ID, secret_key=AWS_SECRET_ACCESS_KEY, token=token
        )

        expected_athena_uri = f"awsathena+rest://aws_access_key_id:aws_secret_access_key@athena.us-east-1.amazonaws.com:443/athena_sql_schema?{expected_query}"

        athena_uri = self.db_hook.get_uri()

        mock_get_credentials.assert_called_once_with(region_name=REGION_NAME)

        assert athena_uri == expected_athena_uri

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook._get_conn_params")
    def test_get_uri_change_driver(self, mock_get_conn_params):
        mock_get_conn_params.return_value = dict(
            driver="arrow", schema_name=SCHEMA_NAME, region_name=REGION_NAME, aws_domain="amazonaws.com"
        )

        athena_uri = self.db_hook.get_uri()

        assert athena_uri.startswith("awsathena+arrow://")

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.pyathena.connect")
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_session")
    def test_get_conn(self, mock_get_session, mock_connect):
        self.db_hook.get_conn()

        mock_get_session.assert_called_once_with(region_name=REGION_NAME)

        mock_connect.assert_called_once_with(
            schema_name=SCHEMA_NAME,
            region_name=REGION_NAME,
            session=mock_get_session.return_value,
            work_group=WORK_GROUP,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.pyathena.connect")
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_session")
    def test_get_conn_with_aws_conn(self, mock_get_session, mock_connect):
        self.db_hook.get_conn()

        mock_get_session.assert_called_once_with(region_name=REGION_NAME)

        mock_connect.assert_called_once_with(
            schema_name=SCHEMA_NAME,
            region_name=REGION_NAME,
            session=mock_get_session.return_value,
            work_group=WORK_GROUP,
        )

    @pytest.mark.parametrize(
        ("conn_params", "conn_extra", "expected_call_args"),
        [
            (
                {"schema": "athena_sql_schema1"},
                {"region_name": "us-east-2"},
                {"region_name": "us-east-2", "schema_name": "athena_sql_schema1", "session": mock.ANY},
            ),
            (
                {"schema": "athena_sql_schema2"},
                {"work_group": "test-work-group", "region_name": "us-east-2"},
                {
                    "region_name": "us-east-2",
                    "schema_name": "athena_sql_schema2",
                    "work_group": "test-work-group",
                    "session": mock.ANY,
                },
            ),
            (
                {"schema": "athena_sql_schema3"},
                {"s3_staging_dir": "s3://test-bucket/", "region_name": "us-east-3"},
                {
                    "region_name": "us-east-3",
                    "schema_name": "athena_sql_schema3",
                    "s3_staging_dir": "s3://test-bucket/",
                    "session": mock.ANY,
                },
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.pyathena.connect")
    def test_get_conn_passing_args(self, mock_connect, conn_params, conn_extra, expected_call_args):
        with mock.patch(
            "airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.conn",
            AwsConnectionWrapper(Connection(conn_type="athena", extra=conn_extra, **conn_params)),
        ):
            self.db_hook.get_conn()
            mock_connect.assert_called_once_with(**expected_call_args)

    # Guards the PyAthena>=3.28.0 floor: older releases fall back to AWS_ATHENA_S3_STAGING_DIR
    # when s3_staging_dir is empty, which workgroups that use managed query results reject.
    @mock.patch.dict("os.environ", {"AWS_ATHENA_S3_STAGING_DIR": "s3://env-bucket/staging/"})
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_session", autospec=True)
    def test_get_conn_empty_s3_staging_dir_skips_env_fallback(self, mock_get_session):
        self.db_hook.get_connection.return_value = Connection(
            conn_type="athena",
            schema=SCHEMA_NAME,
            extra={"work_group": WORK_GROUP, "region_name": REGION_NAME, "s3_staging_dir": ""},
        )

        assert self.db_hook.get_conn().s3_staging_dir is None

    @pytest.mark.parametrize("schema", [SCHEMA_NAME, "sales#archive"])
    @mock.patch("airflow.providers.common.sql.hooks.sql.create_engine", autospec=True)
    @mock.patch.object(
        AthenaSQLHook,
        "get_credentials",
        autospec=True,
        return_value=ReadOnlyCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN),
    )
    def test_get_sqlalchemy_engine_keeps_empty_s3_staging_dir(
        self, mock_get_credentials, mock_create_engine, schema
    ):
        self.db_hook.get_connection.return_value = Connection(
            conn_type="athena",
            schema=schema,
            extra={"work_group": WORK_GROUP, "region_name": REGION_NAME, "s3_staging_dir": ""},
        )

        self.db_hook.get_sqlalchemy_engine()

        mock_create_engine.assert_called_once()
        url = mock_create_engine.call_args.kwargs["url"]
        assert isinstance(url, URL)
        assert url.database == schema
        assert url.query["s3_staging_dir"] == ""

    @pytest.mark.parametrize(
        ("change_uri", "expected_query"),
        [
            pytest.param(
                lambda uri: uri.replace(f"work_group={WORK_GROUP}", "work_group=other-work-group"),
                {"work_group": "other-work-group", "s3_staging_dir": ""},
                id="other-work-group",
            ),
            pytest.param(
                lambda uri: f"{uri}&s3_staging_dir=s3%3A%2F%2Fexplicit-bucket%2F",
                {"work_group": WORK_GROUP, "s3_staging_dir": "s3://explicit-bucket/"},
                id="explicit-s3-staging-dir",
            ),
        ],
    )
    @mock.patch("airflow.providers.common.sql.hooks.sql.create_engine", autospec=True)
    @mock.patch.object(
        AthenaSQLHook,
        "get_credentials",
        autospec=True,
        return_value=ReadOnlyCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN),
    )
    def test_get_sqlalchemy_engine_uses_overridden_get_uri(
        self, mock_get_credentials, mock_create_engine, change_uri, expected_query
    ):
        class AthenaSQLHookWithCustomUri(AthenaSQLHook):
            def get_uri(self) -> str:
                return change_uri(super().get_uri())

        hook = AthenaSQLHookWithCustomUri()
        hook.get_connection = mock.Mock(  # type: ignore[method-assign]
            return_value=Connection(
                conn_type="athena",
                schema=SCHEMA_NAME,
                extra={"work_group": WORK_GROUP, "region_name": REGION_NAME, "s3_staging_dir": ""},
            )
        )

        hook.get_sqlalchemy_engine()

        mock_create_engine.assert_called_once()
        url = mock_create_engine.call_args.kwargs["url"]
        assert isinstance(url, URL)
        assert {key: url.query.get(key) for key in expected_query} == expected_query

    def test_conn_id_default_setter(self):
        assert self.db_hook.athena_conn_id == "athena_default"
        assert self.db_hook.aws_conn_id == "aws_default"

    def test_conn_id_override_setter(self):
        hook = AthenaSQLHook(athena_conn_id=AWS_ATHENA_CONN_ID, aws_conn_id=AWS_CONN_ID)
        assert hook.athena_conn_id == AWS_ATHENA_CONN_ID
        assert hook.aws_conn_id == AWS_CONN_ID

    def test_init_ignores_unexpected_kwargs(self):
        """Verify that connection extras passed as kwargs don't crash the constructor.

        BaseSQLOperator.get_hook() passes all connection extras as hook_params which
        end up as constructor kwargs. Extras like s3_staging_dir and work_group are
        not valid params for AwsGenericHook.__init__ and must be filtered out.
        """
        hook = AthenaSQLHook(
            athena_conn_id="athena_conn",
            s3_staging_dir="s3://mybucket/athena/",
            work_group="primary",
            region_name="eu-west-1",
            driver="rest",
        )
        assert hook.athena_conn_id == "athena_conn"
        # region_name is a valid AwsGenericHook param and should be passed through
        assert hook._region_name == "eu-west-1"

    def test_init_passes_valid_aws_kwargs(self):
        """Verify that valid AwsGenericHook kwargs are still forwarded correctly."""
        hook = AthenaSQLHook(
            athena_conn_id="athena_conn",
            aws_conn_id="custom_aws",
            verify=False,
            region_name="us-west-2",
            config={"retries": {"max_attempts": 5}},
        )
        assert hook.athena_conn_id == "athena_conn"
        assert hook.aws_conn_id == "custom_aws"
        assert hook._verify is False
        assert hook._region_name == "us-west-2"
        assert hook._config is not None


class TestAthenaSQLHookOpenLineage:
    """Static tests for the OpenLineage methods on AthenaSQLHook."""

    EXPECTED_INFORMATION_SCHEMA_COLUMNS = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
        "table_catalog",
    ]

    @staticmethod
    def _make_hook(connection: Connection, hook_region: str | None = None) -> AthenaSQLHook:
        hook = AthenaSQLHook(region_name=hook_region) if hook_region else AthenaSQLHook()
        hook.get_connection = mock.Mock(return_value=connection)  # type: ignore[method-assign]
        return hook

    @pytest.mark.parametrize(
        ("extras", "hook_region", "expected_authority"),
        [
            # region from connection extras when hook-constructor region not set
            ({"region_name": "us-east-1"}, None, "athena.us-east-1.amazonaws.com"),
            # hook-constructor region (explicit user override) wins over extras region
            ({"region_name": "eu-west-1"}, "us-east-2", "athena.us-east-2.amazonaws.com"),
            # hook-constructor region used when extras have none
            ({}, "ap-south-1", "athena.ap-south-1.amazonaws.com"),
            # graceful fallback when neither is set
            ({}, None, "athena.amazonaws.com"),
            # aws_domain extra changes the domain (AWS GovCloud / China / ISO partitions)
            (
                {"region_name": "cn-north-1", "aws_domain": "amazonaws.com.cn"},
                None,
                "athena.cn-north-1.amazonaws.com.cn",
            ),
            # aws_domain still applied when region falls back
            ({"aws_domain": "amazonaws.com.cn"}, None, "athena.amazonaws.com.cn"),
        ],
    )
    def test_get_openlineage_database_info_region_extraction(self, extras, hook_region, expected_authority):
        conn = Connection(conn_type="athena", schema="default", extra=extras)
        hook = self._make_hook(conn, hook_region)
        info = hook.get_openlineage_database_info(conn)
        assert info.authority == expected_authority

    def test_get_openlineage_database_info_returns_expected_fields(self):
        """Snapshot of the DatabaseInfo shape so accidental changes are caught."""
        conn = Connection(
            conn_type="athena",
            schema="default",
            extra={"region_name": "us-east-1"},
        )
        hook = self._make_hook(conn)
        info = hook.get_openlineage_database_info(conn)
        assert info.scheme == "awsathena"
        assert info.authority == "athena.us-east-1.amazonaws.com"
        assert info.database == "AwsDataCatalog"
        assert info.is_information_schema_cross_db is True
        assert info.information_schema_columns == self.EXPECTED_INFORMATION_SCHEMA_COLUMNS

    def test_get_openlineage_database_info_custom_catalog(self):
        conn = Connection(
            conn_type="athena",
            schema="default",
            extra={"region_name": "us-east-1", "catalog": "MyCatalog"},
        )
        hook = self._make_hook(conn)
        info = hook.get_openlineage_database_info(conn)
        assert info.database == "MyCatalog"

    def test_get_openlineage_database_dialect_returns_trino(self):
        conn = Connection(conn_type="athena", extra={"region_name": "us-east-1"})
        hook = self._make_hook(conn)
        assert hook.get_openlineage_database_dialect(conn) == "trino"

    @pytest.mark.parametrize(
        ("connection_schema", "expected_schema"),
        [
            ("mydb", "mydb"),
            (None, "default"),
            ("", "default"),
        ],
    )
    def test_get_openlineage_default_schema(self, connection_schema, expected_schema):
        conn = Connection(
            conn_type="athena",
            schema=connection_schema,
            extra={"region_name": "us-east-1"},
        )
        hook = self._make_hook(conn)
        assert hook.get_openlineage_default_schema() == expected_schema

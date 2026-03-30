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

from airflow.providers.common.compat.sdk import AirflowSkipException
from airflow.providers.slack.transfers.sql_to_slack import SqlToSlackApiFileOperator

from tests_common.test_utils.compat import timezone

TEST_DAG_ID = "sql_to_slack_unit_test"
TEST_TASK_ID = "sql_to_slack_unit_test_task"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSqlToSlackApiFileOperator:
    def setup_method(self):
        self.default_op_kwargs = {
            "sql": "SELECT 1",
            "sql_conn_id": "test-sql-conn-id",
            "slack_conn_id": "test-slack-conn-id",
            "sql_hook_params": None,
            "parameters": None,
        }

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.BaseSqlToSlackOperator._get_query_results")
    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackHook")
    @pytest.mark.parametrize(
        ("filename", "df_method"),
        [
            ("awesome.json", "to_json"),
            ("awesome.json.zip", "to_json"),
            ("awesome.csv", "to_csv"),
            ("awesome.csv.xz", "to_csv"),
            ("awesome.html", "to_html"),
        ],
    )
    @pytest.mark.parametrize("df_kwargs", [None, {}, {"foo": "bar"}])
    @pytest.mark.parametrize("channels", ["#random", "#random,#general", None])
    @pytest.mark.parametrize("initial_comment", [None, "Test Comment"])
    @pytest.mark.parametrize("title", [None, "Test File Title"])
    @pytest.mark.parametrize(
        ("slack_op_kwargs", "hook_extra_kwargs"),
        [
            pytest.param(
                {},
                {"base_url": None, "timeout": None, "proxy": None, "retry_handlers": None},
                id="default-hook-parameters",
            ),
            pytest.param(
                {
                    "slack_base_url": "https://foo.bar",
                    "slack_timeout": 42,
                    "slack_proxy": "http://spam.egg",
                    "slack_retry_handlers": [],
                },
                {
                    "base_url": "https://foo.bar",
                    "timeout": 42,
                    "proxy": "http://spam.egg",
                    "retry_handlers": [],
                },
                id="with-extra-hook-parameters",
            ),
        ],
    )
    def test_send_file(
        self,
        mock_slack_hook_cls,
        mock_get_query_results,
        filename,
        df_method,
        df_kwargs,
        channels,
        initial_comment,
        title,
        slack_op_kwargs: dict,
        hook_extra_kwargs: dict,
    ):
        # Mock Hook
        mock_send_file = mock.MagicMock()
        setattr(mock_slack_hook_cls.return_value, "send_file_v1_to_v2", mock_send_file)

        # Mock returns pandas.DataFrame and expected method
        mock_df = mock.MagicMock()
        mock_df_output_method = mock.MagicMock()
        setattr(mock_df, df_method, mock_df_output_method)
        mock_get_query_results.return_value = mock_df

        op_kwargs = {
            **self.default_op_kwargs,
            "slack_conn_id": "expected-test-slack-conn-id",
            "slack_filename": filename,
            "slack_channels": channels,
            "slack_initial_comment": initial_comment,
            "slack_title": title,
            "df_kwargs": df_kwargs,
            **slack_op_kwargs,
        }
        op = SqlToSlackApiFileOperator(task_id="test_send_file", **op_kwargs)

        mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackHook")
        op.execute(mock.MagicMock())

        mock_slack_hook_cls.assert_called_once_with(
            slack_conn_id="expected-test-slack-conn-id", **hook_extra_kwargs
        )
        mock_get_query_results.assert_called_once_with()
        mock_df_output_method.assert_called_once_with(mock.ANY, **(df_kwargs or {}))
        mock_send_file.assert_called_once_with(
            channels=channels,
            filename=filename,
            initial_comment=initial_comment,
            title=title,
            file=mock.ANY,
        )

    @pytest.mark.parametrize(
        "filename",
        [
            "foo.parquet",
            "bat.parquet.snappy",
            "spam.xml",
            "egg.xlsx",
        ],
    )
    def test_unsupported_format(self, filename):
        op = SqlToSlackApiFileOperator(
            task_id="test_send_file", slack_filename=filename, **self.default_op_kwargs
        )
        with pytest.raises(ValueError, match="Unsupported file format"):
            op.execute(mock.MagicMock())

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackHook")
    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.BaseSqlToSlackOperator._get_query_results")
    def test_null_output_sending_empty_file_by_default(self, mock_get_query_results, mock_slack_hook_cls):
        op_kwargs = {
            **self.default_op_kwargs,
            "slack_conn_id": "expected-test-slack-conn-id",
            "slack_filename": "test_filename.csv",
            "slack_channels": ["#random"],
            "slack_initial_comment": "test_comment",
            "slack_title": "test_title",
        }
        op = SqlToSlackApiFileOperator(task_id="test_send_file", **op_kwargs)

        # Mock empty query results
        mock_df = mock.MagicMock()
        mock_df.configure_mock(**{"empty.return_value": True})
        mock_get_query_results.return_value = mock_df

        op.execute(mock.MagicMock)
        mock_slack_hook_cls.assert_called_once()

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackHook")
    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.BaseSqlToSlackOperator._get_query_results")
    def test_null_output_skip_sending_file(self, mock_get_query_results, mock_slack_hook_cls):
        op_kwargs = {
            **self.default_op_kwargs,
            "slack_conn_id": "expected-test-slack-conn-id",
            "slack_filename": "test_filename.csv",
            "slack_channels": ["#random"],
            "slack_initial_comment": "test_comment",
            "slack_title": "test_title",
            "action_on_empty_df": "skip",
        }
        op = SqlToSlackApiFileOperator(task_id="test_send_file", **op_kwargs)

        # Mock empty query results
        mock_df = mock.MagicMock()
        mock_df.configure_mock(**{"empty.return_value": True})
        mock_get_query_results.return_value = mock_df

        with pytest.raises(AirflowSkipException):
            op.execute(mock.MagicMock())
        mock_slack_hook_cls.assert_not_called()

    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.SlackHook")
    @mock.patch("airflow.providers.slack.transfers.sql_to_slack.BaseSqlToSlackOperator._get_query_results")
    def test_null_output_raise_error(self, mock_get_query_results, mock_slack_hook_cls):
        op_kwargs = {
            **self.default_op_kwargs,
            "slack_conn_id": "expected-test-slack-conn-id",
            "slack_filename": "test_filename.csv",
            "slack_channels": ["#random"],
            "slack_initial_comment": "test_comment",
            "slack_title": "test_title",
            "action_on_empty_df": "error",
        }
        op = SqlToSlackApiFileOperator(task_id="test_send_file", **op_kwargs)

        # Mock empty query results
        mock_df = mock.MagicMock()
        mock_df.configure_mock(**{"empty.return_value": True})
        mock_get_query_results.return_value = mock_df

        with pytest.raises(ValueError, match=r"output df must be non-empty\. Failing"):
            op.execute(mock.MagicMock())
        mock_slack_hook_cls.assert_not_called()

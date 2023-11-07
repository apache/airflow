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

import warnings
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.slack.transfers.base_sql_to_slack import BaseSqlToSlackOperator
from airflow.providers.slack.transfers.sql_to_slack_webhook import SqlToSlackWebhookOperator
from airflow.providers.slack.utils import parse_filename

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SqlToSlackApiFileOperator(BaseSqlToSlackOperator):
    """
    Executes an SQL statement in a given SQL connection and sends the results to Slack API as file.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SqlToSlackApiFileOperator`

    :param sql: The SQL query to be executed
    :param sql_conn_id: reference to a specific DB-API Connection.
    :param slack_conn_id: :ref:`Slack API Connection <howto/connection:slack>`.
    :param slack_filename: Filename for display in slack.
        Should contain supported extension which referenced to ``SUPPORTED_FILE_FORMATS``.
        It is also possible to set compression in extension:
        ``filename.csv.gzip``, ``filename.json.zip``, etc.
    :param sql_hook_params: Extra config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    :param parameters: The parameters to pass to the SQL query.
    :param slack_channels: Comma-separated list of channel names or IDs where the file will be shared.
         If omitting this parameter, then file will send to workspace.
    :param slack_initial_comment: The message text introducing the file in specified ``slack_channels``.
    :param slack_title: Title of file.
    :param slack_base_url: A string representing the Slack API base URL. Optional
    :param df_kwargs: Keyword arguments forwarded to ``pandas.DataFrame.to_{format}()`` method.
    """

    template_fields: Sequence[str] = (
        "sql",
        "slack_channels",
        "slack_filename",
        "slack_initial_comment",
        "slack_title",
    )
    template_ext: Sequence[str] = (".sql", ".jinja", ".j2")
    template_fields_renderers = {"sql": "sql", "slack_message": "jinja"}

    SUPPORTED_FILE_FORMATS: Sequence[str] = ("csv", "json", "html")

    def __init__(
        self,
        *,
        sql: str,
        sql_conn_id: str,
        sql_hook_params: dict | None = None,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        slack_conn_id: str = SlackHook.default_conn_name,
        slack_filename: str,
        slack_channels: str | Sequence[str] | None = None,
        slack_initial_comment: str | None = None,
        slack_title: str | None = None,
        slack_base_url: str | None = None,
        df_kwargs: dict | None = None,
        **kwargs,
    ):
        super().__init__(
            sql=sql, sql_conn_id=sql_conn_id, sql_hook_params=sql_hook_params, parameters=parameters, **kwargs
        )
        self.slack_conn_id = slack_conn_id
        self.slack_filename = slack_filename
        self.slack_channels = slack_channels
        self.slack_initial_comment = slack_initial_comment
        self.slack_title = slack_title
        self.slack_base_url = slack_base_url
        self.df_kwargs = df_kwargs or {}

    def execute(self, context: Context) -> None:
        # Parse file format from filename
        output_file_format, _ = parse_filename(
            filename=self.slack_filename,
            supported_file_formats=self.SUPPORTED_FILE_FORMATS,
        )

        slack_hook = SlackHook(
            slack_conn_id=self.slack_conn_id,
            base_url=self.slack_base_url,
            timeout=self.slack_timeout,
            proxy=self.slack_proxy,
            retry_handlers=self.slack_retry_handlers,
        )
        with NamedTemporaryFile(mode="w+", suffix=f"_{self.slack_filename}") as fp:
            # tempfile.NamedTemporaryFile used only for create and remove temporary file,
            # pandas will open file in correct mode itself depend on file type.
            # So we close file descriptor here for avoid incidentally write anything.
            fp.close()

            output_file_name = fp.name
            output_file_format = output_file_format.upper()
            df_result = self._get_query_results()
            if output_file_format == "CSV":
                df_result.to_csv(output_file_name, **self.df_kwargs)
            elif output_file_format == "JSON":
                df_result.to_json(output_file_name, **self.df_kwargs)
            elif output_file_format == "HTML":
                df_result.to_html(output_file_name, **self.df_kwargs)
            else:
                # Not expected that this error happen. This only possible
                # if SUPPORTED_FILE_FORMATS extended and no actual implementation for specific format.
                raise AirflowException(f"Unexpected output file format: {output_file_format}")

            slack_hook.send_file(
                channels=self.slack_channels,
                file=output_file_name,
                filename=self.slack_filename,
                initial_comment=self.slack_initial_comment,
                title=self.slack_title,
            )


class SqlToSlackOperator(SqlToSlackWebhookOperator):
    """
    Executes an SQL statement in a given SQL connection and sends the results to Slack Incoming Webhook.

    Deprecated, use :class:`airflow.providers.slack.transfers.sql_to_slack_webhook.SqlToSlackWebhookOperator`
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "`airflow.providers.slack.transfers.sql_to_slack.SqlToSlackOperator` has been renamed "
            "and moved `airflow.providers.slack.transfers.sql_to_slack_webhook.SqlToSlackWebhookOperator` "
            "this operator deprecated and will be removed in future",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)

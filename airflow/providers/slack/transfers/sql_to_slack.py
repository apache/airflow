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

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from pandas import DataFrame
from tabulate import tabulate

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.utils import parse_filename

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BaseSqlToSlackOperator(BaseOperator):
    """
    Operator implements base sql methods for SQL to Slack Transfer operators.

    :param sql: The SQL query to be executed
    :param sql_conn_id: reference to a specific DB-API Connection.
    :param sql_hook_params: Extra config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    :param parameters: The parameters to pass to the SQL query.
    """

    def __init__(
        self,
        *,
        sql: str,
        sql_conn_id: str,
        sql_hook_params: dict | None = None,
        parameters: Iterable | Mapping | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_conn_id = sql_conn_id
        self.sql_hook_params = sql_hook_params
        self.sql = sql
        self.parameters = parameters

    def _get_hook(self) -> DbApiHook:
        self.log.debug("Get connection for %s", self.sql_conn_id)
        conn = BaseHook.get_connection(self.sql_conn_id)
        hook = conn.get_hook(hook_params=self.sql_hook_params)
        if not callable(getattr(hook, "get_pandas_df", None)):
            raise AirflowException(
                "This hook is not supported. The hook class must have get_pandas_df method."
            )
        return hook

    def _get_query_results(self) -> DataFrame:
        sql_hook = self._get_hook()

        self.log.info("Running SQL query: %s", self.sql)
        df = sql_hook.get_pandas_df(self.sql, parameters=self.parameters)
        return df


class SqlToSlackOperator(BaseSqlToSlackOperator):
    """
    Executes an SQL statement in a given SQL connection and sends the results to Slack. The results of the
    query are rendered into the 'slack_message' parameter as a Pandas dataframe using a JINJA variable called
    '{{ results_df }}'. The 'results_df' variable name can be changed by specifying a different
    'results_df_name' parameter. The Tabulate library is added to the JINJA environment as a filter to
    allow the dataframe to be rendered nicely. For example, set 'slack_message' to {{ results_df |
    tabulate(tablefmt="pretty", headers="keys") }} to send the results to Slack as an ascii rendered table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SqlToSlackOperator`

    :param sql: The SQL query to be executed (templated)
    :param slack_message: The templated Slack message to send with the data returned from the SQL connection.
        You can use the default JINJA variable {{ results_df }} to access the pandas dataframe containing the
        SQL results
    :param sql_conn_id: reference to a specific database.
    :param sql_hook_params: Extra config params to be passed to the underlying hook.
           Should match the desired hook constructor params.
    :param slack_conn_id: The connection id for Slack.
    :param slack_webhook_token: The token to use to authenticate to Slack. If this is not provided, the
        'slack_conn_id' attribute needs to be specified in the 'password' field.
    :param slack_channel: The channel to send message. Override default from Slack connection.
    :param results_df_name: The name of the JINJA template's dataframe variable, default is 'results_df'
    :param parameters: The parameters to pass to the SQL query
    """

    template_fields: Sequence[str] = ("sql", "slack_message")
    template_ext: Sequence[str] = (".sql", ".jinja", ".j2")
    template_fields_renderers = {"sql": "sql", "slack_message": "jinja"}
    times_rendered = 0

    def __init__(
        self,
        *,
        sql: str,
        sql_conn_id: str,
        sql_hook_params: dict | None = None,
        slack_conn_id: str | None = None,
        slack_webhook_token: str | None = None,
        slack_channel: str | None = None,
        slack_message: str,
        results_df_name: str = "results_df",
        parameters: Iterable | Mapping | None = None,
        **kwargs,
    ) -> None:

        super().__init__(
            sql=sql, sql_conn_id=sql_conn_id, sql_hook_params=sql_hook_params, parameters=parameters, **kwargs
        )

        self.slack_conn_id = slack_conn_id
        self.slack_webhook_token = slack_webhook_token
        self.slack_channel = slack_channel
        self.slack_message = slack_message
        self.results_df_name = results_df_name
        self.kwargs = kwargs

        if not self.slack_conn_id and not self.slack_webhook_token:
            raise AirflowException(
                "SqlToSlackOperator requires either a `slack_conn_id` or a `slack_webhook_token` argument"
            )

    def _render_and_send_slack_message(self, context, df) -> None:
        # Put the dataframe into the context and render the JINJA template fields
        context[self.results_df_name] = df
        self.render_template_fields(context)

        slack_hook = self._get_slack_hook()
        self.log.info("Sending slack message: %s", self.slack_message)
        slack_hook.send(text=self.slack_message, channel=self.slack_channel)

    def _get_slack_hook(self) -> SlackWebhookHook:
        return SlackWebhookHook(
            slack_webhook_conn_id=self.slack_conn_id, webhook_token=self.slack_webhook_token
        )

    def render_template_fields(self, context, jinja_env=None) -> None:
        # If this is the first render of the template fields, exclude slack_message from rendering since
        # the SQL results haven't been retrieved yet.
        if self.times_rendered == 0:
            fields_to_render: Iterable[str] = filter(lambda x: x != "slack_message", self.template_fields)
        else:
            fields_to_render = self.template_fields

        if not jinja_env:
            jinja_env = self.get_template_env()

        # Add the tabulate library into the JINJA environment
        jinja_env.filters["tabulate"] = tabulate

        self._do_render_template_fields(self, fields_to_render, context, jinja_env, set())
        self.times_rendered += 1

    def execute(self, context: Context) -> None:
        if not isinstance(self.sql, str):
            raise AirflowException("Expected 'sql' parameter should be a string.")
        if self.sql is None or self.sql.strip() == "":
            raise AirflowException("Expected 'sql' parameter is missing.")
        if self.slack_message is None or self.slack_message.strip() == "":
            raise AirflowException("Expected 'slack_message' parameter is missing.")

        df = self._get_query_results()
        self._render_and_send_slack_message(context, df)

        self.log.debug("Finished sending SQL data to Slack")


class SqlToSlackApiFileOperator(BaseSqlToSlackOperator):
    """
    Executes an SQL statement in a given SQL connection and sends the results to Slack API as file.

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
    :param df_kwargs: Keyword arguments forwarded to ``pandas.DataFrame.to_{format}()`` method.

    Example:
     .. code-block:: python

        SqlToSlackApiFileOperator(
            task_id="sql_to_slack",
            sql="SELECT 1 a, 2 b, 3 c",
            sql_conn_id="sql-connection",
            slack_conn_id="slack-api-connection",
            slack_filename="awesome.json.gz",
            slack_channels="#random,#general",
            slack_initial_comment="Awesome load to compressed multiline JSON.",
            df_kwargs={
                "orient": "records",
                "lines": True,
            },
        )
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
        parameters: Iterable | Mapping | None = None,
        slack_conn_id: str,
        slack_filename: str,
        slack_channels: str | Sequence[str] | None = None,
        slack_initial_comment: str | None = None,
        slack_title: str | None = None,
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
        self.df_kwargs = df_kwargs or {}

    def execute(self, context: Context) -> None:
        # Parse file format from filename
        output_file_format, _ = parse_filename(
            filename=self.slack_filename,
            supported_file_formats=self.SUPPORTED_FILE_FORMATS,
        )

        slack_hook = SlackHook(slack_conn_id=self.slack_conn_id)
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

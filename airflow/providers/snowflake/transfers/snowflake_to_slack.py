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
import json
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, Optional, Sequence, Union

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.common import parse_filename

if TYPE_CHECKING:
    from pandas import DataFrame

    from airflow.utils.context import Context


class SnowflakeToSlackOperator(BaseOperator):
    """
    Executes an SQL statement in Snowflake and sends the results to Slack.

    Operator supports two modes.

    **First mode** send as a text if ``slack_message`` parameter specified.
    Render ``pandas.Dataframe`` using a Jinja variable called '{{ results_df }}'.
    The 'results_df' variable name can be changed by specifying a different ``results_df_name`` parameter.
    Also, ``tabulate`` library will add in this mode as Jinja filter to allow
    the dataframe to be rendered nicely.

    Example ``slack_message`` set for Slack Incoming Webhook (SlackWebhookHook):
     .. code-block:: jinja

        {{ results_df | tabulate(tablefmt="pretty", headers="keys") }}

    Example ``slack_message`` set for Slack API (SlackHook):
     .. code-block:: js+jinja

        {
            "channel": "#random",
            "text": "```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
        }

    **Second mode** send as a file if ``filename`` parameter specified.
    Note: this mode only works with Slack API (SlackHook).

    Example:
     .. code-block:: python

        SnowflakeToSlackOperator(
            task_id="snow_to_slack",
            sql="SELECT 1 a, 2 b, 3 c",
            use_slack_webhook=False,
            filename="awesome.parquet.snappy",
            channels="#random,#general",
            initial_comment="Awesome load to parquet.",
        )

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnowflakeToSlackOperator`

    :param sql: The SQL statement to execute on Snowflake (templated)
    :param use_slack_webhook: Use legacy Slack WebHook connection.
    :param filename: If specified than result send to slack as file, rather than as text.
        Only support SlackHook connection. Filename should contain valid extension which referenced
        to format: csv, parquet, json, etc. It is also possible to set
        compression in extension: csv.gzip, parquet.snappy, etc. If filename contains not supported
        file formats than operator fall back to uncompressed csv. (templated)
    :param slack_message: Use the templated Slack message to send with the data returned from Snowflake if
        ``filename`` not specified. You can use the default Jinja variable {{ results_df }} to access the
        pandas dataframe containing the SQL results. (templated)
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param slack_conn_id: The connection id for Slack. Either Slack API or Slack Incoming Webhooks
        could use, control by `use_slack_webhook`.
    :param parameters: The parameters to pass to the SQL query
    :param warehouse: The Snowflake virtual warehouse to use to run the SQL query
    :param database: The Snowflake database to use for the SQL query
    :param schema: The schema to run the SQL against in Snowflake
    :param role: The role to use when connecting to Snowflake
    :param results_df_name: The name of the JINJA template's dataframe variable, default is 'results_df'
    :param df_to_params: The parameters to pass to ``pandas.Dataframe.to_<file_format>`` method.
        See: https://pandas.pydata.org/docs/reference/io.html
    :param channels: Comma-separated list of channel names or IDs where the file will be shared.
        If not specified than file would upload to Slack Workspace files only.
    :param initial_comment: The message text introducing the file in specified ``channels``.
    :param slack_token: The token to use to authenticate to Slack. If this is not provided,
        than use token from connection.
    """

    SUPPORTED_FILE_FORMATS = (
        'csv',
        'parquet',
        'json',
    )

    template_fields: Sequence[str] = ('sql', 'slack_message', 'filename', 'channels', 'initial_comment')
    template_fields_renderers = {"sql": "sql", "slack_message": "jinja"}
    times_rendered = 0

    @property
    def template_ext(self):
        if not self.filename:
            return '.sql', '.jinja', '.j2'
        return ('.sql',)

    def __init__(
        self,
        *,
        sql: str,
        filename: Optional[str] = None,
        slack_message: Optional[Union[str, Dict]] = None,
        snowflake_conn_id: str = 'snowflake_default',
        slack_conn_id: str = 'slack_default',
        use_slack_webhook: bool = True,
        parameters: Optional[Union[Iterable, Mapping]] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        slack_token: Optional[str] = None,
        results_df_name: str = 'results_df',
        df_to_params: Optional[Dict[str, Any]] = None,
        channels: Optional[str] = None,
        initial_comment: Optional[str] = None,
        **kwargs,
    ) -> None:
        if not sql:
            raise ValueError("Expected 'sql' parameter is missing.")
        if not ((not slack_message) ^ (not filename)):
            raise ValueError(
                f"Either slack_message={slack_message} or filename={filename} should provided, not both."
            )
        if filename and use_slack_webhook:
            raise AirflowException("Not possible to send result as file if use legacy SlackWebhookHook.")

        self.filename = filename or None
        super().__init__(**kwargs)

        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.slack_conn_id = slack_conn_id
        self.slack_token = slack_token
        self.slack_message = slack_message or None
        self.results_df_name = results_df_name
        self.use_slack_webhook = use_slack_webhook
        self.df_to_params = df_to_params or {}
        self.channels = channels
        self.initial_comment = initial_comment

    @cached_property
    def slack_hook(self):
        if self.use_slack_webhook:
            self.log.warning('Use legacy SlackWebhookHook (conn_id=%r).', self.slack_conn_id)
            return SlackWebhookHook(
                http_conn_id=self.slack_conn_id, message=self.slack_message, webhook_token=self.slack_token
            )
        return SlackHook(slack_conn_id=self.slack_conn_id, token=self.slack_token)

    @cached_property
    def snowflake_hook(self) -> SnowflakeHook:
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
        )

    def render_template_fields(self, context, jinja_env=None) -> None:
        """Template all attributes listed in template_fields.

        If ``slack_message`` defined than if this is the first render of the template fields,
        exclude ``slack_message`` from rendering scope since the snowflake results haven't been retrieved yet.

        If ``slack_message`` not defined than use default behaviour from ``BaseOperator``.

        :param context: Dict with values to apply on content
        :param jinja_env: Jinja environment
        """
        if not jinja_env:
            jinja_env = self.get_template_env()

        if not self.slack_message:
            super().render_template_fields(context, jinja_env=jinja_env)
            return

        # Add the ``tabulate`` library into the Jinja Environment filters if not there.
        if "tabulate" not in jinja_env.filters:

            from tabulate import tabulate

            jinja_env.filters['tabulate'] = tabulate

        if self.times_rendered == 0:
            fields_to_render: Iterable[str] = filter(lambda x: x != 'slack_message', self.template_fields)
        else:
            fields_to_render = self.template_fields

        self._do_render_template_fields(self, fields_to_render, context, jinja_env, set())
        self.times_rendered += 1

    def _validate_slack_message(self):
        if not self.use_slack_webhook:
            # Slack SDK expected 'slack_message' as dictionary.
            if isinstance(self.slack_message, str):
                self.log.info("Try decode 'slack_message' as JSON.")
                self.slack_message = json.loads(self.slack_message)

            if not isinstance(self.slack_message, dict):
                raise AirflowException(
                    "Expected 'slack_message' parameter should be a dictionary "
                    f"if SlackHook connection use, got {type(self.sql)}."
                )
        else:
            # Slack Incoming Webhook expected 'slack_message' as string.
            if not isinstance(self.slack_message, str):
                raise AirflowException(
                    "Expected 'slack_message' parameter should be a string"
                    f"if SlackWebhookHook connection use, got {type(self.sql)}."
                )
            elif self.slack_message.strip() == "":
                raise AirflowException("Expected 'slack_message' parameter is missing.")

    def _validate_sql(self):
        if not isinstance(self.sql, str):
            raise AirflowException(f"Expected 'sql' parameter should be a string, got {type(self.sql)}.")
        elif self.sql.strip() == "":
            raise AirflowException("Expected 'sql' parameter is empty.")

    def _validate_filename(self):
        if not self.filename:
            return
        self.filename = self.filename.strip()

        if not isinstance(self.filename, str):
            raise TypeError(f"Expected 'filename' parameter should be a string, got {type(self.filename)}.")
        elif self.filename == "":
            raise ValueError("Expected 'filename' parameter is empty.")

    def send_text(self, context, df: 'DataFrame'):
        """Send pandas.DataFrame as a text."""
        self._validate_slack_message()

        rows = len(df.index)
        if rows >= 50:
            self.log.warning('Send output as text not suitable for large data, total rows: %s', rows)

        context[self.results_df_name] = df
        self.render_template_fields(context, self.get_template_env())

        if not self.use_slack_webhook:
            self.slack_hook.call(api_method="chat.postMessage", json=self.slack_message)
        else:
            self.slack_hook.message = self.slack_message  # Update hook message by rendered message
            return self.slack_hook.execute()

    def send_file(self, df: 'DataFrame'):
        """Send pandas.DataFrame as a file."""
        if not isinstance(self.slack_hook, SlackHook):
            raise AirflowException("Only possible to send result as file if use SlackHook.")
        self._validate_filename()

        file_format, compression = parse_filename(
            filename=self.filename, supported_file_formats=self.SUPPORTED_FILE_FORMATS, fallback="csv"
        )

        try:
            method = getattr(df, f"to_{file_format}")
        except AttributeError:
            raise AirflowException(
                f"pandas.Dataframe has no method 'to_{file_format}'. "
                f"See: https://pandas.pydata.org/docs/reference/io.html."
            )

        api_params = {  # type: ignore[union-attr]
            "filename": self.filename,
            "filetype": "auto",
            "channels": self.channels,
            "initial_comment": self.initial_comment,
        }
        api_params = {k: v for k, v in api_params.items() if v}
        self.log.info("Slack SDK 'files.upload' method parameters %r", api_params)

        with NamedTemporaryFile(suffix=self.filename, mode="w+b") as fp:
            self.log.info(
                "Save pandas.Dataframe to %r with compression '%s' and parameters %r.",
                file_format,
                compression,
                self.df_to_params,
            )
            method(fp, **self.df_to_params)
            fp.seek(0, 0)
            self.slack_hook.call(api_method="files.upload", data=api_params, files={"file": fp})

    def execute(self, context: 'Context') -> None:
        self._validate_sql()
        self.log.info('Running SQL query: %s', self.sql)
        df = self.snowflake_hook.get_pandas_df(self.sql, parameters=self.parameters)

        if self.filename:
            self.send_file(df=df)
        else:
            self.send_text(context=context, df=df)

        self.log.debug('Finished sending Snowflake data to Slack')

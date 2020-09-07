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

from typing import Iterable, Mapping, Optional, Union

from pandas import DataFrame
from tabulate import tabulate

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults


class SnowflakeToSlackOperator(BaseOperator):
    """
    Executes an SQL statement in Snowflake and sends the results to Slack. The results of the query are
    rendered into the 'slack_message' parameter as a Pandas dataframe using a JINJA variable called '{{
    results_df }}'. The 'results_df' variable name can be changed by specifying a different
    'results_df_name' parameter. The Tabulate library is added to the JINJA environment as a filter to
    allow the dataframe to be rendered nicely. For example, set 'slack_message' to {{ results_df |
    tabulate(tablefmt="pretty", headers="keys") }} to send the results to Slack as an ascii rendered table.

    :param sql: The SQL statement to execute on Snowflake (templated)
    :type sql: str
    :param slack_message: The templated Slack message to send with the data returned from Snowflake.
        You can use the default JINJA variable {{ results_df }} to access the pandas dataframe containing the
        SQL results
    :type slack_message: str
    :param snowflake_conn_id: The Snowflake connection id
    :type snowflake_conn_id: str
    :param slack_conn_id: The connection id for Slack
    :type slack_conn_id: str
    :param results_df_name: The name of the JINJA template's dataframe variable, default is 'results_df'
    :type results_df_name: str
    :param parameters: The parameters to pass to the SQL query
    :type parameters: Optional[Union[Iterable, Mapping]]
    :param warehouse: The Snowflake virtual warehouse to use to run the SQL query
    :type warehouse: Optional[str]
    :param database: The Snowflake database to use for the SQL query
    :type database: Optional[str]
    :param schema: The schema to run the SQL against in Snowflake
    :type schema: Optional[str]
    :param role: The role to use when connecting to Snowflake
    :type role: Optional[str]
    :param slack_token: The token to use to authenticate to Slack. If this is not provided, the
        'webhook_token' attribute needs to be specified in the 'Extra' JSON field against the slack_conn_id
    :type slack_token: Optional[str]
    """

    template_fields = ['sql', 'slack_message']
    template_ext = ['.sql', '.jinja', '.j2']
    times_rendered = 0

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        sql: str,
        slack_message: str,
        snowflake_conn_id: str = 'snowflake_default',
        slack_conn_id: str = 'slack_default',
        results_df_name: str = 'results_df',
        parameters: Optional[Union[Iterable, Mapping]] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        slack_token: Optional[str] = None,
        **kwargs,
    ) -> None:
        super(SnowflakeToSlackOperator, self).__init__(**kwargs)

        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.slack_conn_id = slack_conn_id
        self.slack_token = slack_token
        self.slack_message = slack_message
        self.results_df_name = results_df_name

    def _get_query_results(self) -> DataFrame:
        snowflake_hook = self._get_snowflake_hook()

        self.log.info('Running SQL query: %s', self.sql)
        df = snowflake_hook.get_pandas_df(self.sql, parameters=self.parameters)
        return df

    def _render_and_send_slack_message(self, context, df) -> None:
        # Put the dataframe into the context and render the JINJA template fields
        context[self.results_df_name] = df
        self.render_template_fields(context)

        slack_hook = self._get_slack_hook()
        self.log.info('Sending slack message: %s', self.slack_message)
        slack_hook.execute()

    def _get_snowflake_hook(self) -> SnowflakeHook:
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
        )

    def _get_slack_hook(self) -> SlackWebhookHook:
        return SlackWebhookHook(
            http_conn_id=self.slack_conn_id, message=self.slack_message, webhook_token=self.slack_token
        )

    def render_template_fields(self, context, jinja_env=None) -> None:
        # If this is the first render of the template fields, exclude slack_message from rendering since
        # the snowflake results haven't been retrieved yet.
        if self.times_rendered == 0:
            fields_to_render: Iterable[str] = filter(lambda x: x != 'slack_message', self.template_fields)
        else:
            fields_to_render = self.template_fields

        if not jinja_env:
            jinja_env = self.get_template_env()

        # Add the tabulate library into the JINJA environment
        jinja_env.filters['tabulate'] = tabulate

        self._do_render_template_fields(self, fields_to_render, context, jinja_env, set())
        self.times_rendered += 1

    def execute(self, context) -> None:
        if not isinstance(self.sql, str):
            raise AirflowException("Expected 'sql' parameter should be a string.")
        if self.sql is None or self.sql.strip() == "":
            raise AirflowException("Expected 'sql' parameter is missing.")
        if self.slack_message is None or self.slack_message.strip() == "":
            raise AirflowException("Expected 'slack_message' parameter is missing.")

        df = self._get_query_results()
        self._render_and_send_slack_message(context, df)

        self.log.debug('Finished sending Snowflake data to Slack')

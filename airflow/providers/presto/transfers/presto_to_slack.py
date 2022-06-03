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

from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence, Union

from pandas import DataFrame
from tabulate import tabulate

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PrestoToSlackOperator(BaseOperator):
    """
    Executes a single SQL statement in Presto and sends the results to Slack. The results of the query are
    rendered into the 'slack_message' parameter as a Pandas dataframe using a JINJA variable called '{{
    results_df }}'. The 'results_df' variable name can be changed by specifying a different
    'results_df_name' parameter. The Tabulate library is added to the JINJA environment as a filter to
    allow the dataframe to be rendered nicely. For example, set 'slack_message' to {{ results_df |
    tabulate(tablefmt="pretty", headers="keys") }} to send the results to Slack as an ascii rendered table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PrestoToSlackOperator`

    :param sql: The SQL statement to execute on Presto (templated)
    :param slack_message: The templated Slack message to send with the data returned from Presto.
        You can use the default JINJA variable {{ results_df }} to access the pandas dataframe containing the
        SQL results
    :param presto_conn_id: destination presto connection
    :param slack_conn_id: The connection id for Slack
    :param results_df_name: The name of the JINJA template's dataframe variable, default is 'results_df'
    :param parameters: The parameters to pass to the SQL query
    :param slack_token: The token to use to authenticate to Slack. If this is not provided, the
        'webhook_token' attribute needs to be specified in the 'Extra' JSON field against the slack_conn_id
    :param slack_channel: The channel to send message. Override default from Slack connection.
    """

    template_fields: Sequence[str] = ('sql', 'slack_message', 'slack_channel')
    template_ext: Sequence[str] = ('.sql', '.jinja', '.j2')
    template_fields_renderers = {"sql": "sql", "slack_message": "jinja"}
    times_rendered = 0

    def __init__(
        self,
        *,
        sql: str,
        slack_message: str,
        presto_conn_id: str = 'presto_default',
        slack_conn_id: str = 'slack_default',
        results_df_name: str = 'results_df',
        parameters: Optional[Union[Iterable, Mapping]] = None,
        slack_token: Optional[str] = None,
        slack_channel: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.presto_conn_id = presto_conn_id
        self.sql = sql
        self.parameters = parameters
        self.slack_conn_id = slack_conn_id
        self.slack_token = slack_token
        self.slack_message = slack_message
        self.results_df_name = results_df_name
        self.slack_channel = slack_channel

    def _get_query_results(self) -> DataFrame:
        presto_hook = self._get_presto_hook()

        self.log.info('Running SQL query: %s', self.sql)
        df = presto_hook.get_pandas_df(self.sql, parameters=self.parameters)
        return df

    def _render_and_send_slack_message(self, context, df) -> None:
        # Put the dataframe into the context and render the JINJA template fields
        context[self.results_df_name] = df
        self.render_template_fields(context)

        slack_hook = self._get_slack_hook()
        self.log.info('Sending slack message: %s', self.slack_message)
        slack_hook.execute()

    def _get_presto_hook(self) -> PrestoHook:
        return PrestoHook(presto_conn_id=self.presto_conn_id)

    def _get_slack_hook(self) -> SlackWebhookHook:
        return SlackWebhookHook(
            http_conn_id=self.slack_conn_id,
            message=self.slack_message,
            webhook_token=self.slack_token,
            slack_channel=self.slack_channel,
        )

    def render_template_fields(self, context, jinja_env=None) -> None:
        # If this is the first render of the template fields, exclude slack_message from rendering since
        # the presto results haven't been retrieved yet.
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

    def execute(self, context: 'Context') -> None:
        if not self.sql.strip():
            raise AirflowException("Expected 'sql' parameter is missing.")
        if not self.slack_message.strip():
            raise AirflowException("Expected 'slack_message' parameter is missing.")

        df = self._get_query_results()

        self._render_and_send_slack_message(context, df)

        self.log.debug('Finished sending Presto data to Slack')

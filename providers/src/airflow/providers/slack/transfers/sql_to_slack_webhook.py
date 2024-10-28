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
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from deprecated import deprecated
from tabulate import tabulate

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.transfers.base_sql_to_slack import BaseSqlToSlackOperator
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SqlToSlackWebhookOperator(BaseSqlToSlackOperator):
    """
    Executes an SQL statement in a given SQL connection and sends the results to Slack Incoming Webhook.

    The results of the query are rendered into the 'slack_message' parameter as a Pandas
    dataframe using a JINJA variable called '{{ results_df }}'. The 'results_df' variable
    name can be changed by specifying a different 'results_df_name' parameter. The Tabulate
    library is added to the JINJA environment as a filter to allow the dataframe to be
    rendered nicely. For example, set 'slack_message' to {{ results_df |
    tabulate(tablefmt="pretty", headers="keys") }} to send the results to Slack as an ascii
    rendered table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SqlToSlackWebhookOperator`

    .. note::
        You cannot override the default channel (chosen by the user who installed your app),
        Instead, these values will always inherit from the associated Slack App configuration
        (`link <https://api.slack.com/messaging/webhooks#advanced_message_formatting>`_).
        It is possible to change this values only in `Legacy Slack Integration Incoming Webhook
        <https://api.slack.com/legacy/custom-integrations/messaging/webhooks#legacy-customizations>`_.

    .. warning::
        This hook intend to use `Slack Incoming Webhook` connection
        and might not work correctly with `Slack API` connection.

    :param sql: The SQL query to be executed (templated)
    :param slack_message: The templated Slack message to send with the data returned from the SQL connection.
        You can use the default JINJA variable {{ results_df }} to access the pandas dataframe containing the
        SQL results
    :param sql_conn_id: reference to a specific database.
    :param sql_hook_params: Extra config params to be passed to the underlying hook.
           Should match the desired hook constructor params.
    :param slack_webhook_conn_id: :ref:`Slack Incoming Webhook <howto/connection:slack>`
        connection id that has Incoming Webhook token in the password field.
    :param slack_channel: The channel to send message.
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
        slack_webhook_conn_id: str | None = None,
        sql_hook_params: dict | None = None,
        slack_channel: str | None = None,
        slack_message: str,
        results_df_name: str = "results_df",
        parameters: list | tuple | Mapping[str, Any] | None = None,
        slack_conn_id: str | ArgNotSet = NOTSET,
        **kwargs,
    ) -> None:
        if slack_conn_id is not NOTSET:
            warnings.warn(
                "Parameter `slack_conn_id` is deprecated because this attribute initially intend to use with "
                "Slack API however this operator provided integration with Slack Incoming Webhook. "
                "Please use `slack_webhook_conn_id` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=3,
            )
            if slack_webhook_conn_id and slack_conn_id != slack_webhook_conn_id:
                raise ValueError(
                    "Conflicting Connection ids provided, "
                    f"slack_webhook_conn_id={slack_webhook_conn_id!r}, slack_conn_id={slack_conn_id!r}."
                )
            slack_webhook_conn_id = slack_conn_id  # type: ignore[assignment]
        if not slack_webhook_conn_id:
            raise ValueError("Got an empty `slack_webhook_conn_id` value.")
        super().__init__(
            sql=sql,
            sql_conn_id=sql_conn_id,
            sql_hook_params=sql_hook_params,
            parameters=parameters,
            **kwargs,
        )

        self.slack_webhook_conn_id = slack_webhook_conn_id
        self.slack_channel = slack_channel
        self.slack_message = slack_message
        self.results_df_name = results_df_name
        self.kwargs = kwargs

    def _render_and_send_slack_message(self, context, df) -> None:
        # Put the dataframe into the context and render the JINJA template fields
        context[self.results_df_name] = df
        self.render_template_fields(context)

        slack_hook = self._get_slack_hook()
        self.log.info("Sending slack message: %s", self.slack_message)
        slack_hook.send(text=self.slack_message, channel=self.slack_channel)

    def _get_slack_hook(self) -> SlackWebhookHook:
        return SlackWebhookHook(
            slack_webhook_conn_id=self.slack_webhook_conn_id,
            proxy=self.slack_proxy,
            timeout=self.slack_timeout,
            retry_handlers=self.slack_retry_handlers,
        )

    def render_template_fields(self, context, jinja_env=None) -> None:
        # If this is the first render of the template fields, exclude slack_message from rendering since
        # the SQL results haven't been retrieved yet.
        if self.times_rendered == 0:
            fields_to_render: Iterable[str] = (
                x for x in self.template_fields if x != "slack_message"
            )
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

    @property
    @deprecated(
        reason=(
            "`SqlToSlackWebhookOperator.slack_conn_id` property deprecated and will be removed in a future. "
            "Please use `slack_webhook_conn_id` instead."
        ),
        category=AirflowProviderDeprecationWarning,
    )
    def slack_conn_id(self):
        return self.slack_webhook_conn_id

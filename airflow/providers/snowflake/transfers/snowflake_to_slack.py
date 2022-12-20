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
from typing import Iterable, Mapping, Sequence

from airflow.providers.slack.transfers.sql_to_slack import SqlToSlackOperator


class SnowflakeToSlackOperator(SqlToSlackOperator):
    """
    Executes an SQL statement in Snowflake and sends the results to Slack. The results of the query are
    rendered into the 'slack_message' parameter as a Pandas dataframe using a JINJA variable called '{{
    results_df }}'. The 'results_df' variable name can be changed by specifying a different
    'results_df_name' parameter. The Tabulate library is added to the JINJA environment as a filter to
    allow the dataframe to be rendered nicely. For example, set 'slack_message' to {{ results_df |
    tabulate(tablefmt="pretty", headers="keys") }} to send the results to Slack as an ascii rendered table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnowflakeToSlackOperator`

    :param sql: The SQL statement to execute on Snowflake (templated)
    :param slack_message: The templated Slack message to send with the data returned from Snowflake.
        You can use the default JINJA variable {{ results_df }} to access the pandas dataframe containing the
        SQL results
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param slack_conn_id: The connection id for Slack.
    :param results_df_name: The name of the JINJA template's dataframe variable, default is 'results_df'
    :param parameters: The parameters to pass to the SQL query
    :param warehouse: The Snowflake virtual warehouse to use to run the SQL query
    :param database: The Snowflake database to use for the SQL query
    :param schema: The schema to run the SQL against in Snowflake
    :param role: The role to use when connecting to Snowflake
    :param slack_token: The token to use to authenticate to Slack. If this is not provided, the
        'webhook_token' attribute needs to be specified in the 'Extra' JSON field against the slack_conn_id.
    """

    template_fields: Sequence[str] = ("sql", "slack_message")
    template_ext: Sequence[str] = (".sql", ".jinja", ".j2")
    template_fields_renderers = {"sql": "sql", "slack_message": "jinja"}
    times_rendered = 0

    def __init__(
        self,
        *,
        sql: str,
        slack_message: str,
        snowflake_conn_id: str = "snowflake_default",
        slack_conn_id: str = "slack_default",
        results_df_name: str = "results_df",
        parameters: Iterable | Mapping | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        slack_token: str | None = None,
        **kwargs,
    ) -> None:
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

        warnings.warn(
            """
            SnowflakeToSlackOperator is deprecated.
            Please use `airflow.providers.slack.transfers.sql_to_slack.SqlToSlackOperator`.
            """,
            DeprecationWarning,
            stacklevel=2,
        )

        hook_params = {
            "schema": self.schema,
            "role": self.role,
            "database": self.database,
            "warehouse": self.warehouse,
        }
        cleaned_hook_params = {k: v for k, v in hook_params.items() if v is not None}

        super().__init__(
            sql=self.sql,
            sql_conn_id=self.snowflake_conn_id,
            slack_conn_id=self.slack_conn_id,
            slack_webhook_token=self.slack_token,
            slack_message=self.slack_message,
            results_df_name=self.results_df_name,
            parameters=self.parameters,
            sql_hook_params=cleaned_hook_params,
            **kwargs,
        )

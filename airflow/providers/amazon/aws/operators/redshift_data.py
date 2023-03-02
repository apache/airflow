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

import warnings
from typing import TYPE_CHECKING

from airflow.compat.functools import cached_property
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook

if TYPE_CHECKING:
    from mypy_boto3_redshift_data.type_defs import GetStatementResultResponseTypeDef

    from airflow.utils.context import Context


class RedshiftDataOperator(BaseOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster using Redshift Data

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftDataOperator`

    :param database: the name of the database
    :param sql: the SQL statement or list of  SQL statement to run
    :param cluster_identifier: unique identifier of a cluster
    :param db_user: the database username
    :param parameters: the parameters for the SQL statement
    :param secret_arn: the name or ARN of the secret that enables db access
    :param statement_name: the name of the SQL statement
    :param with_event: indicates whether to send an event to EventBridge
    :param wait_for_completion: indicates whether to wait for a result, if True wait, if False don't wait
    :param poll_interval: how often in seconds to check the query status
    :param return_sql_result: if True will return the result of an SQL statement,
        if False (default) will return statement ID
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    template_fields = (
        "cluster_identifier",
        "database",
        "sql",
        "db_user",
        "parameters",
        "statement_name",
        "aws_conn_id",
        "region",
    )
    template_ext = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    statement_id: str | None

    def __init__(
        self,
        database: str,
        sql: str | list,
        cluster_identifier: str | None = None,
        db_user: str | None = None,
        parameters: list | None = None,
        secret_arn: str | None = None,
        statement_name: str | None = None,
        with_event: bool = False,
        wait_for_completion: bool = True,
        poll_interval: int = 10,
        return_sql_result: bool = False,
        aws_conn_id: str = "aws_default",
        region: str | None = None,
        await_result: bool | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database = database
        self.sql = sql
        self.cluster_identifier = cluster_identifier
        self.db_user = db_user
        self.parameters = parameters
        self.secret_arn = secret_arn
        self.statement_name = statement_name
        self.with_event = with_event
        self.await_result = await_result
        self.wait_for_completion = wait_for_completion
        if await_result:
            warnings.warn(
                f"Parameter `{self.__class__.__name__}.await_result` is deprecated and will be removed "
                "in a future release. Please use method `wait_for_completion` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.wait_for_completion = await_result
        if poll_interval > 0:
            self.poll_interval = poll_interval
        else:
            self.log.warning(
                "Invalid poll_interval:",
                poll_interval,
            )
        self.return_sql_result = return_sql_result
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.statement_id: str | None = None

    @cached_property
    def hook(self) -> RedshiftDataHook:
        """Create and return an RedshiftDataHook."""
        return RedshiftDataHook(aws_conn_id=self.aws_conn_id, region_name=self.region)

    def execute_query(self) -> str:
        warnings.warn(
            "This method is deprecated and has been moved to the hook "
            "`airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.statement_id = self.hook.execute_query(
            database=self.database,
            sql=self.sql,
            cluster_identifier=self.cluster_identifier,
            db_user=self.db_user,
            parameters=self.parameters,
            secret_arn=self.secret_arn,
            statement_name=self.statement_name,
            with_event=self.with_event,
            wait_for_completion=self.wait_for_completion,
            poll_interval=self.poll_interval,
        )
        return self.statement_id

    def execute_batch_query(self) -> str:
        warnings.warn(
            "This method is deprecated and has been moved to the hook "
            "`airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.statement_id = self.hook.execute_query(
            database=self.database,
            sql=self.sql,
            cluster_identifier=self.cluster_identifier,
            db_user=self.db_user,
            parameters=self.parameters,
            secret_arn=self.secret_arn,
            statement_name=self.statement_name,
            with_event=self.with_event,
            wait_for_completion=self.wait_for_completion,
            poll_interval=self.poll_interval,
        )
        return self.statement_id

    def wait_for_results(self, statement_id: str):
        warnings.warn(
            "This method is deprecated and has been moved to the hook "
            "`airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.hook.wait_for_results(statement_id=statement_id, poll_interval=self.poll_interval)

    def execute(self, context: Context) -> GetStatementResultResponseTypeDef | str:
        """Execute a statement against Amazon Redshift"""
        self.log.info("Executing statement: %s", self.sql)

        self.statement_id = self.hook.execute_query(
            database=self.database,
            sql=self.sql,
            cluster_identifier=self.cluster_identifier,
            db_user=self.db_user,
            parameters=self.parameters,
            secret_arn=self.secret_arn,
            statement_name=self.statement_name,
            with_event=self.with_event,
            wait_for_completion=self.wait_for_completion,
            poll_interval=self.poll_interval,
        )

        if self.return_sql_result:
            result = self.hook.conn.get_statement_result(Id=self.statement_id)
            self.log.debug("Statement result: %s", result)
            return result
        else:
            return self.statement_id

    def on_kill(self) -> None:
        """Cancel the submitted redshift query"""
        if self.statement_id:
            self.log.info("Received a kill signal.")
            self.log.info("Stopping Query with statementId - %s", self.statement_id)

            try:
                self.hook.conn.cancel_statement(Id=self.statement_id)
            except Exception as ex:
                self.log.error("Unable to cancel query. Exiting. %s", ex)

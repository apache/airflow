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
import sys
from time import sleep
from typing import TYPE_CHECKING, Any, Dict, Optional

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftDataOperator(BaseOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster using Redshift Data

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftDataOperator`

    :param database: the name of the database
    :param sql: the SQL statement text to run
    :param cluster_identifier: unique identifier of a cluster
    :param db_user: the database username
    :param parameters: the parameters for the SQL statement
    :param secret_arn: the name or ARN of the secret that enables db access
    :param statement_name: the name of the SQL statement
    :param with_event: indicates whether to send an event to EventBridge
    :param await_result: indicates whether to wait for a result, if True wait, if False don't wait
    :param poll_interval: how often in seconds to check the query status
    :param aws_conn_id: aws connection to use
    :param region: aws region to use
    """

    template_fields = (
        'cluster_identifier',
        'database',
        'sql',
        'db_user',
        'parameters',
        'statement_name',
        'aws_conn_id',
        'region',
    )
    template_ext = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}

    def __init__(
        self,
        database: str,
        sql: str,
        cluster_identifier: Optional[str] = None,
        db_user: Optional[str] = None,
        parameters: Optional[list] = None,
        secret_arn: Optional[str] = None,
        statement_name: Optional[str] = None,
        with_event: bool = False,
        await_result: bool = True,
        poll_interval: int = 10,
        aws_conn_id: str = 'aws_default',
        region: Optional[str] = None,
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
        if poll_interval > 0:
            self.poll_interval = poll_interval
        else:
            self.log.warning(
                "Invalid poll_interval:",
                poll_interval,
            )
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.statement_id = None

    @cached_property
    def hook(self) -> RedshiftDataHook:
        """Create and return an RedshiftDataHook."""
        return RedshiftDataHook(aws_conn_id=self.aws_conn_id, region_name=self.region)

    def execute_query(self):
        kwargs: Dict[str, Any] = {
            "ClusterIdentifier": self.cluster_identifier,
            "Database": self.database,
            "Sql": self.sql,
            "DbUser": self.db_user,
            "Parameters": self.parameters,
            "WithEvent": self.with_event,
            "SecretArn": self.secret_arn,
            "StatementName": self.statement_name,
        }

        filter_values = {key: val for key, val in kwargs.items() if val is not None}
        resp = self.hook.conn.execute_statement(**filter_values)
        return resp['Id']

    def wait_for_results(self, statement_id):
        while True:
            self.log.info("Polling statement %s", statement_id)
            resp = self.hook.conn.describe_statement(
                Id=statement_id,
            )
            status = resp['Status']
            if status == 'FINISHED':
                return status
            elif status == 'FAILED' or status == 'ABORTED':
                raise ValueError(f"Statement {statement_id!r} terminated with status {status}.")
            else:
                self.log.info(f"Query {status}")
            sleep(self.poll_interval)

    def execute(self, context: 'Context') -> None:
        """Execute a statement against Amazon Redshift"""
        self.log.info(f"Executing statement: {self.sql}")

        self.statement_id = self.execute_query()

        if self.await_result:
            self.wait_for_results(self.statement_id)

        return self.statement_id

    def on_kill(self) -> None:
        """Cancel the submitted redshift query"""
        if self.statement_id:
            self.log.info('Received a kill signal.')
            self.log.info('Stopping Query with statementId - %s', self.statement_id)

            try:
                self.hook.conn.cancel_statement(Id=self.statement_id)
            except Exception as ex:
                self.log.error('Unable to cancel query. Exiting. %s', ex)

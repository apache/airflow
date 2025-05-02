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

import time
from collections.abc import Iterable
from dataclasses import dataclass
from pprint import pformat
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pendulum import duration

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.utils import trim_none_values

if TYPE_CHECKING:
    from mypy_boto3_redshift_data import RedshiftDataAPIServiceClient  # noqa: F401
    from mypy_boto3_redshift_data.type_defs import DescribeStatementResponseTypeDef

FINISHED_STATE = "FINISHED"
FAILED_STATE = "FAILED"
ABORTED_STATE = "ABORTED"
FAILURE_STATES = {FAILED_STATE, ABORTED_STATE}
RUNNING_STATES = {"PICKED", "STARTED", "SUBMITTED"}


@dataclass
class QueryExecutionOutput:
    """Describes the output of a query execution."""

    statement_id: str
    session_id: str | None


class RedshiftDataQueryFailedError(ValueError):
    """Raise an error that redshift data query failed."""


class RedshiftDataQueryAbortedError(ValueError):
    """Raise an error that redshift data query was aborted."""


class RedshiftDataHook(AwsGenericHook["RedshiftDataAPIServiceClient"]):
    """
    Interact with Amazon Redshift Data API.

    Provide thin wrapper around
    :external+boto3:py:class:`boto3.client("redshift-data") <RedshiftDataAPIService.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
        - `Amazon Redshift Data API \
        <https://docs.aws.amazon.com/redshift-data/latest/APIReference/Welcome.html>`__
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift-data"
        super().__init__(*args, **kwargs)

    def execute_query(
        self,
        sql: str | list[str],
        database: str | None = None,
        cluster_identifier: str | None = None,
        db_user: str | None = None,
        parameters: Iterable | None = None,
        secret_arn: str | None = None,
        statement_name: str | None = None,
        with_event: bool = False,
        wait_for_completion: bool = True,
        poll_interval: int = 10,
        workgroup_name: str | None = None,
        session_id: str | None = None,
        session_keep_alive_seconds: int | None = None,
    ) -> QueryExecutionOutput:
        """
        Execute a statement against Amazon Redshift.

        :param sql: the SQL statement or list of  SQL statement to run
        :param database: the name of the database
        :param cluster_identifier: unique identifier of a cluster
        :param db_user: the database username
        :param parameters: the parameters for the SQL statement
        :param secret_arn: the name or ARN of the secret that enables db access
        :param statement_name: the name of the SQL statement
        :param with_event: whether to send an event to EventBridge
        :param wait_for_completion: whether to wait for a result
        :param poll_interval: how often in seconds to check the query status
        :param workgroup_name: name of the Redshift Serverless workgroup. Mutually exclusive with
            `cluster_identifier`. Specify this parameter to query Redshift Serverless. More info
            https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html
        :param session_id: the session identifier of the query
        :param session_keep_alive_seconds: duration in seconds to keep the session alive after the query
            finishes. The maximum time a session can keep alive is 24 hours

        :returns statement_id: str, the UUID of the statement
        """
        kwargs: dict[str, Any] = {
            "ClusterIdentifier": cluster_identifier,
            "Database": database,
            "DbUser": db_user,
            "Parameters": parameters,
            "WithEvent": with_event,
            "SecretArn": secret_arn,
            "StatementName": statement_name,
            "WorkgroupName": workgroup_name,
            "SessionId": session_id,
            "SessionKeepAliveSeconds": session_keep_alive_seconds,
        }

        if sum(x is not None for x in (cluster_identifier, workgroup_name, session_id)) != 1:
            raise ValueError(
                "Exactly one of cluster_identifier, workgroup_name, or session_id must be provided"
            )

        if session_id is not None:
            msg = "session_id must be a valid UUID4"
            try:
                if UUID(session_id).version != 4:
                    raise ValueError(msg)
            except ValueError:
                raise ValueError(msg)

        if session_keep_alive_seconds is not None and (
            session_keep_alive_seconds < 0 or duration(seconds=session_keep_alive_seconds).hours > 24
        ):
            raise ValueError("Session keep alive duration must be between 0 and 86400 seconds.")

        if isinstance(sql, list):
            kwargs["Sqls"] = sql
            resp = self.conn.batch_execute_statement(**trim_none_values(kwargs))
        else:
            kwargs["Sql"] = sql
            resp = self.conn.execute_statement(**trim_none_values(kwargs))

        statement_id = resp["Id"]

        if wait_for_completion:
            self.wait_for_results(statement_id, poll_interval=poll_interval)

        return QueryExecutionOutput(statement_id=statement_id, session_id=resp.get("SessionId"))

    def wait_for_results(self, statement_id: str, poll_interval: int) -> str:
        while True:
            self.log.info("Polling statement %s", statement_id)
            is_finished = self.check_query_is_finished(statement_id)
            if is_finished:
                return FINISHED_STATE

            time.sleep(poll_interval)

    def check_query_is_finished(self, statement_id: str) -> bool:
        """Check whether query finished, raise exception is failed."""
        resp = self.conn.describe_statement(Id=statement_id)
        return self.parse_statement_response(resp)

    def parse_statement_response(self, resp: DescribeStatementResponseTypeDef) -> bool:
        """Parse the response of describe_statement."""
        status = resp["Status"]
        if status == FINISHED_STATE:
            num_rows = resp.get("ResultRows")
            if num_rows is not None:
                self.log.info("Processed %s rows", num_rows)
            return True
        if status in FAILURE_STATES:
            exception_cls = (
                RedshiftDataQueryFailedError if status == FAILED_STATE else RedshiftDataQueryAbortedError
            )
            raise exception_cls(
                f"Statement {resp['Id']} terminated with status {status}. Response details: {pformat(resp)}"
            )

        self.log.info("Query status: %s", status)
        return False

    def get_table_primary_key(
        self,
        table: str,
        database: str,
        schema: str | None = "public",
        cluster_identifier: str | None = None,
        workgroup_name: str | None = None,
        db_user: str | None = None,
        secret_arn: str | None = None,
        statement_name: str | None = None,
        with_event: bool = False,
        wait_for_completion: bool = True,
        poll_interval: int = 10,
    ) -> list[str] | None:
        """
        Return the table primary key.

        Copied from ``RedshiftSQLHook.get_table_primary_key()``

        :param table: Name of the target table
        :param database: the name of the database
        :param schema: Name of the target schema, public by default
        :param cluster_identifier: unique identifier of a cluster
        :param workgroup_name: name of the Redshift Serverless workgroup. Mutually exclusive with
            `cluster_identifier`. Specify this parameter to query Redshift Serverless. More info
            https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html
        :param db_user: the database username
        :param secret_arn: the name or ARN of the secret that enables db access
        :param statement_name: the name of the SQL statement
        :param with_event: indicates whether to send an event to EventBridge
        :param wait_for_completion: indicates whether to wait for a result, if True wait, if False don't wait
        :param poll_interval: how often in seconds to check the query status

        :return: Primary key columns list
        """
        sql = f"""
            select kcu.column_name
            from information_schema.table_constraints tco
                    join information_schema.key_column_usage kcu
                        on kcu.constraint_name = tco.constraint_name
                            and kcu.constraint_schema = tco.constraint_schema
                            and kcu.constraint_name = tco.constraint_name
            where tco.constraint_type = 'PRIMARY KEY'
            and kcu.table_schema = {schema}
            and kcu.table_name = {table}
        """
        stmt_id = self.execute_query(
            sql=sql,
            database=database,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            with_event=with_event,
            wait_for_completion=wait_for_completion,
            poll_interval=poll_interval,
        ).statement_id

        pk_columns = []
        token = ""
        while True:
            kwargs = {"Id": stmt_id}
            if token:
                kwargs["NextToken"] = token
            response = self.conn.get_statement_result(**kwargs)
            # we only select a single column (that is a string),
            # so safe to assume that there is only a single col in the record
            pk_columns += [y["stringValue"] for x in response["Records"] for y in x]
            if "NextToken" in response:
                token = response["NextToken"]
            else:
                break

        return pk_columns or None

    async def is_still_running(self, statement_id: str) -> bool:
        """
        Async function to check whether the query is still running.

        :param statement_id: the UUID of the statement
        """
        async with await self.get_async_conn() as client:
            desc = await client.describe_statement(Id=statement_id)
            return desc["Status"] in RUNNING_STATES

    async def check_query_is_finished_async(self, statement_id: str) -> bool:
        """
        Async function to check statement is finished.

        It takes statement_id, makes async connection to redshift data to get the query status
        by statement_id and returns the query status.

        :param statement_id: the UUID of the statement
        """
        async with await self.get_async_conn() as client:
            resp = await client.describe_statement(Id=statement_id)
            return self.parse_statement_response(resp)

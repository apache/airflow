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

from time import sleep
from typing import TYPE_CHECKING, Any, Iterable

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.utils import trim_none_values

if TYPE_CHECKING:
    from mypy_boto3_redshift_data import RedshiftDataAPIServiceClient  # noqa


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
        database: str,
        sql: str | list[str],
        cluster_identifier: str | None = None,
        db_user: str | None = None,
        parameters: Iterable | None = None,
        secret_arn: str | None = None,
        statement_name: str | None = None,
        with_event: bool = False,
        wait_for_completion: bool = True,
        poll_interval: int = 10,
    ) -> str:
        """
        Execute a statement against Amazon Redshift

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
        }
        if isinstance(sql, list):
            kwargs["Sqls"] = sql
            resp = self.conn.batch_execute_statement(**trim_none_values(kwargs))
        else:
            kwargs["Sql"] = sql
            resp = self.conn.execute_statement(**trim_none_values(kwargs))

        statement_id = resp["Id"]

        if wait_for_completion:
            self.wait_for_results(statement_id, poll_interval=poll_interval)

        return statement_id

    def wait_for_results(self, statement_id, poll_interval):
        while True:
            self.log.info("Polling statement %s", statement_id)
            resp = self.conn.describe_statement(
                Id=statement_id,
            )
            status = resp["Status"]
            if status == "FINISHED":
                return status
            elif status == "FAILED" or status == "ABORTED":
                raise ValueError(
                    f"Statement {statement_id!r} terminated with status {status}, "
                    f"error msg: {resp.get('Error')}"
                )
            else:
                self.log.info("Query %s", status)
            sleep(poll_interval)

    def get_table_primary_key(
        self,
        table: str,
        database: str,
        schema: str | None = "public",
        cluster_identifier: str | None = None,
        db_user: str | None = None,
        secret_arn: str | None = None,
        statement_name: str | None = None,
        with_event: bool = False,
        wait_for_completion: bool = True,
        poll_interval: int = 10,
    ) -> list[str] | None:
        """
        Helper method that returns the table primary key.

        Copied from ``RedshiftSQLHook.get_table_primary_key()``

        :param table: Name of the target table
        :param database: the name of the database
        :param schema: Name of the target schema, public by default
        :param sql: the SQL statement or list of  SQL statement to run
        :param cluster_identifier: unique identifier of a cluster
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
            db_user=db_user,
            secret_arn=secret_arn,
            statement_name=statement_name,
            with_event=with_event,
            wait_for_completion=wait_for_completion,
            poll_interval=poll_interval,
        )
        pk_columns = []
        token = ""
        while True:
            kwargs = dict(Id=stmt_id)
            if token:
                kwargs["NextToken"] = token
            response = self.conn.get_statement_result(**kwargs)
            # we only select a single column (that is a string),
            # so safe to assume that there is only a single col in the record
            pk_columns += [y["stringValue"] for x in response["Records"] for y in x]
            if "NextToken" not in response.keys():
                break
            else:
                token = response["NextToken"]

        return pk_columns or None

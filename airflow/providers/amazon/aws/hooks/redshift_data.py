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

import asyncio
from io import StringIO
from pprint import pformat
from time import sleep
from typing import TYPE_CHECKING, Any, Iterable

import botocore.exceptions
from asgiref.sync import sync_to_async
from snowflake.connector.util_text import split_statements

from airflow.exceptions import AirflowException
from airflow.models.param import ParamsDict
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseAsyncHook, AwsGenericHook
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
                num_rows = resp.get("ResultRows")
                if num_rows is not None:
                    self.log.info("Processed %s rows", num_rows)
                return status
            elif status == "FAILED" or status == "ABORTED":
                raise ValueError(
                    f"Statement {statement_id!r} terminated with status {status}. "
                    f"Response details: {pformat(resp)}"
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


class RedshiftDataAsyncHook(AwsBaseAsyncHook):
    """
    RedshiftDataHook inherits from AwsBaseHook to connect with AWS redshift
    by using boto3 client_type as redshift-data we can interact with redshift cluster database
    and execute the query

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param verify: Whether or not to verify SSL certificates.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
    :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
    :param config: Configuration for botocore client.
        (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(self, *args: Any, poll_interval: int = 0, **kwargs: Any) -> None:
        aws_connection_type: str = "redshift-data"
        try:
            # for apache-airflow-providers-amazon>=3.0.0
            kwargs["client_type"] = aws_connection_type
            kwargs["resource_type"] = aws_connection_type
            super().__init__(*args, **kwargs)
        except ValueError:
            # for apache-airflow-providers-amazon>=4.1.0
            kwargs["client_type"] = aws_connection_type
            super().__init__(*args, **kwargs)
        self.client_type = aws_connection_type
        self.poll_interval = poll_interval

    def get_conn_params(self) -> dict[str, str | int]:
        """Helper method to retrieve connection args"""
        if not self.aws_conn_id:
            raise AirflowException("Required connection details is missing !")

        connection_object = self.get_connection(self.aws_conn_id)
        extra_config = connection_object.extra_dejson

        conn_params: dict[str, str | int] = {}

        if "db_user" in extra_config:
            conn_params["db_user"] = extra_config.get("db_user", None)
        else:
            raise AirflowException("Required db user is missing !")

        if "database" in extra_config:
            conn_params["database"] = extra_config.get("database", None)
        elif connection_object.schema:
            conn_params["database"] = connection_object.schema
        else:
            raise AirflowException("Required Database name is missing !")

        if "access_key_id" in extra_config or "aws_access_key_id" in extra_config:
            conn_params["aws_access_key_id"] = (
                extra_config["access_key_id"]
                if "access_key_id" in extra_config
                else extra_config["aws_access_key_id"]
            )
            conn_params["aws_secret_access_key"] = (
                extra_config["secret_access_key"]
                if "secret_access_key" in extra_config
                else extra_config["aws_secret_access_key"]
            )
        elif connection_object.login:
            conn_params["aws_access_key_id"] = connection_object.login
            conn_params["aws_secret_access_key"] = connection_object.password
        else:
            raise AirflowException("Required access_key_id, aws_secret_access_key")

        if "region" in extra_config or "region_name" in extra_config:
            self.log.info("Retrieving region_name from Connection.extra_config['region_name']")
            conn_params["region_name"] = (
                extra_config["region"] if "region" in extra_config else extra_config["region_name"]
            )
        else:
            raise AirflowException("Required Region name is missing !")

        if "aws_session_token" in extra_config:
            self.log.info(
                "session token retrieved from extra, please note you are responsible for renewing these.",
            )
            conn_params["aws_session_token"] = extra_config["aws_session_token"]

        if "cluster_identifier" in extra_config:
            self.log.info("Retrieving cluster_identifier from Connection.extra_config['cluster_identifier']")
            conn_params["cluster_identifier"] = extra_config["cluster_identifier"]
        else:
            raise AirflowException("Required Cluster identifier is missing !")

        return conn_params

    def execute_query(
        self, sql: dict[Any, Any] | Iterable[Any], params: ParamsDict | dict[Any, Any]
    ) -> tuple[list[str], dict[str, str]]:
        """
        Runs an SQL statement, which can be data manipulation language (DML)
        or data definition language (DDL)

        :param sql: list of query ids
        """
        if not sql:
            raise AirflowException("SQL query is None.")
        try:
            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = self.get_conn()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = self.get_conn()
            conn_params = self.get_conn_params()
            query_ids: list[str] = []
            for sql_statement in sql:
                self.log.info("Executing statement: %s", sql_statement)
                response = client.execute_statement(
                    Database=conn_params["database"],
                    ClusterIdentifier=conn_params["cluster_identifier"],
                    DbUser=conn_params["db_user"],
                    Sql=sql_statement,
                    WithEvent=True,
                )
                query_ids.append(response["Id"])
            return query_ids, {"status": "success", "message": "success"}
        except botocore.exceptions.ClientError as error:
            return [], {"status": "error", "message": str(error)}

    async def get_query_status(self, query_ids: list[str]) -> dict[str, str | list[str]]:
        """
        Async function to get the Query status by query Ids.
        The function takes list of query_ids, makes async connection to redshift data to get the query status
        by query id and returns the query status. In case of success, it returns a list of query IDs of the
        queriesthat have a status `FINISHED`. In the case of partial failure meaning if any of queries fail
        or is aborted by the user we return an error as a whole.

        :param query_ids: list of query ids
        """
        try:
            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = await sync_to_async(self.get_conn)()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = await sync_to_async(self.get_conn)()
            completed_ids: list[str] = []
            for query_id in query_ids:
                while await self.is_still_running(query_id):
                    await asyncio.sleep(self.poll_interval)
                res = client.describe_statement(Id=query_id)
                if res["Status"] == "FINISHED":
                    completed_ids.append(query_id)
                elif res["Status"] == "FAILED":
                    msg = "Error: " + res["QueryString"] + " query Failed due to, " + res["Error"]
                    return {"status": "error", "message": msg, "query_id": query_id, "type": res["Status"]}
                elif res["Status"] == "ABORTED":
                    return {
                        "status": "error",
                        "message": "The query run was stopped by the user.",
                        "query_id": query_id,
                        "type": res["Status"],
                    }
            return {"status": "success", "completed_ids": completed_ids}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}

    async def is_still_running(self, qid: str) -> bool | dict[str, str]:
        """
        Async function to check whether the query is still running to return True or in
        "PICKED", "STARTED" or "SUBMITTED" state to return False.
        """
        try:
            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = await sync_to_async(self.get_conn)()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = await sync_to_async(self.get_conn)()
            desc = client.describe_statement(Id=qid)
            if desc["Status"] in ["PICKED", "STARTED", "SUBMITTED"]:
                return True
            return False
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}

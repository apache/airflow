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
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.utils import trim_none_values

if TYPE_CHECKING:
    from mypy_boto3_redshift_data import RedshiftDataAPIServiceClient  # noqa


class RedshiftDataHook(AwsGenericHook["RedshiftDataAPIServiceClient"]):
    """
    Interact with AWS Redshift Data, using the boto3 library
    Hook attribute `conn` has all methods that listed in documentation

    .. seealso::
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html
        - https://docs.aws.amazon.com/redshift-data/latest/APIReference/Welcome.html

    Additional arguments (such as ``aws_conn_id`` or ``region_name``) may be specified and
        are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
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
        parameters: list | None = None,
        secret_arn: str | None = None,
        statement_name: str | None = None,
        with_event: bool = False,
        await_result: bool = True,
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
        :param await_result: indicates whether to wait for a result, if True wait, if False don't wait
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

        if await_result:
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

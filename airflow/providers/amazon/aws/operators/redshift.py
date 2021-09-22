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
from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_statement import RedshiftStatementHook


class RedshiftOperator(BaseOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster

    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements)
    :param redshift_conn_id: reference to
        :ref:`Amazon Redshift connection id<howto/connection:redshift>`
    :type redshift_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)

    def __init__(
        self,
        *,
        sql: Any,
        redshift_conn_id: str = 'redshift_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def get_hook(self) -> RedshiftStatementHook:
        """Create and return RedshiftStatementHook.
        :return RedshiftStatementHook: A RedshiftStatementHook instance.
        """
        return RedshiftStatementHook(redshift_conn_id=self.redshift_conn_id)

    def execute(self, context: dict) -> None:
        """Execute a statement against Amazon Redshift"""
        self.log.info(f"Executing statement: {self.sql}")
        hook = self.get_hook()
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)

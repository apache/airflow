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

from typing import TYPE_CHECKING, Dict, Iterable, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftSQLOperator(BaseOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftSQLOperator`

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param redshift_conn_id: reference to
        :ref:`Amazon Redshift connection id<howto/connection:redshift>`
    :param parameters: (optional) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)

    def __init__(
        self,
        *,
        sql: Optional[Union[Dict, Iterable]],
        redshift_conn_id: str = 'redshift_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters

    def get_hook(self) -> RedshiftSQLHook:
        """Create and return RedshiftSQLHook.
        :return RedshiftSQLHook: A RedshiftSQLHook instance.
        """
        return RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

    def execute(self, context: 'Context') -> None:
        """Execute a statement against Amazon Redshift"""
        self.log.info(f"Executing statement: {self.sql}")
        hook = self.get_hook()
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)

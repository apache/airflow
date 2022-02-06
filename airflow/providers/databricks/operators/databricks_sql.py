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
#
"""This module contains Databricks operators."""

from typing import Any, Iterable, List, Mapping, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.utils.context import Context


class DatabricksSqlOperator(BaseOperator):
    """
    Executes SQL code in a Databricks SQL endpoint or Databricks cluster

    :param databricks_conn_id: Reference to
        :ref:`Databricks connection id<howto/connection:databricks>`
    :param http_path:
    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}

    def __init__(
        self,
        *,
        sql: Union[str, List[str]],
        databricks_conn_id: str = 'databricks_default',
        http_path: Optional[str] = None,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        session_configuration=None,
        do_xcom_push: bool = True,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksSqlOperator``."""
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.sql = sql
        self._http_path = http_path
        self.parameters = parameters
        self.do_xcom_push = do_xcom_push
        self.session_config = session_configuration

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id, http_path=self._http_path, session_configuration=self.session_config
        )

    def execute(self, context: Context) -> Any:
        self.log.info('Executing: %s', self.sql)
        hook = self._get_hook()
        results = hook.run(self.sql, parameters=self.parameters)
        # self.log.debug('Results: %s', results)
        if self.do_xcom_push:
            return results

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
from typing import Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.utils.decorators import apply_defaults


class JdbcOperator(BaseOperator):
    """
    Executes sql code in a database using jdbc driver.

    Requires jaydebeapi.

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param jdbc_conn_id: reference to a predefined database
    :type jdbc_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        jdbc_conn_id: str = 'jdbc_default',
        autocommit: bool = False,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parameters = parameters
        self.sql = sql
        self.jdbc_conn_id = jdbc_conn_id
        self.autocommit = autocommit
        self.hook = None

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
        hook.run(self.sql, self.autocommit, parameters=self.parameters)

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
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.decorators import apply_defaults


class OracleOperator(BaseOperator):
    """
    Executes sql code in a specific Oracle database

    :param sql: the sql code to be executed. Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :type sql: str or list[str]
    :param oracle_conn_id: reference to a specific Oracle database
    :type oracle_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        oracle_conn_id: str = 'oracle_default',
        parameters: Optional[Union[Mapping, Iterable]] = None,
        autocommit: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context) -> None:
        self.log.info('Executing: %s', self.sql)
        hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)

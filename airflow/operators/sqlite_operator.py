# -*- coding: utf-8 -*-
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
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.decorators import apply_defaults


class SqliteOperator(BaseOperator):
    """
    Executes sql code in a specific Sqlite database

    :param sql: the sql code to be executed. (templated)
    :type sql: str or string pointing to a template file. File must have
        a '.sql' extensions.
    :param sqlite_conn_id: reference to a specific sqlite database
    :type sqlite_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#cdaaed'

    @apply_defaults
    def __init__(
            self,
            sql: str,
            sqlite_conn_id: str = 'sqlite_default',
            parameters: Optional[Union[Mapping, Iterable]] = None,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sqlite_conn_id = sqlite_conn_id
        self.sql = sql
        self.parameters = parameters or []

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = SqliteHook(sqlite_conn_id=self.sqlite_conn_id)
        hook.run(self.sql, parameters=self.parameters)

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
from typing import Mapping, Optional

from airflow.models import BaseOperator
from airflow.providers.exasol.hooks.exasol import ExasolHook
from airflow.utils.decorators import apply_defaults


class ExasolOperator(BaseOperator):
    """
    Executes sql code in a specific Exasol database

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param exasol_conn_id: reference to a specific Exasol database
    :type exasol_conn_id: string
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping
    :param schema: (optional) name of the schema which overwrite defined one in connection
    :type schema: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql: str,
            exasol_conn_id: str = 'exasol_default',
            autocommit: bool = False,
            parameters: Optional[Mapping] = None,
            schema: Optional[str] = None,
            *args, **kwargs):
        super(ExasolOperator, self).__init__(*args, **kwargs)
        self.exasol_conn_id = exasol_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.schema = schema

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = ExasolHook(exasol_conn_id=self.exasol_conn_id,
                          schema=self.schema)
        hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters)

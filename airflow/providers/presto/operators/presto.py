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
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.utils.decorators import apply_defaults


class PrestoOperator(BaseOperator):
    """
    Executes sql code in a specific Presto database

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :type sql: str or list[str]
    :param presto_conn_id: reference to a specific presto database
    :type presto_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    :param catalog: name of catalog which overwrite defined one in connection
    :type catalog: str
    :param schema: name of schema which overwrite defined one in connection
    :type schema: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql: str,
            presto_conn_id: str = 'presto_default',
            parameters: Optional[Union[Mapping, Iterable]] = None,
            catalog: Optional[str] = None,
            schema: Optional[str] = None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.presto_conn_id = presto_conn_id
        self.sql = sql
        self.parameters = parameters
        self.catalog = catalog
        self.schema = schema

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = PrestoHook(presto_conn_id=self.presto_conn_id,
                          schema=self.schema,
                          catalog=self.catalog)
        hook.run(
            self.sql,
            parameters=self.parameters)

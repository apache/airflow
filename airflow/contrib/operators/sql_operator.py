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

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SqlOperator(BaseOperator):
    """
    Executes sql code in a database.

    This abstract operator can be instantiated directly,
    and does not need to be derived into subclasses for each DbApiHook subclass.
    It will automatically usesthe correct DbApiHook subclass implmenetation,
    made possible by reflecting upon the Connection's assigned `conn_type`.

    :param conn_id: reference to a predefined sql database connection
    :type conn_id: str
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            conn_id,
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(SqlOperator, self).__init__(*args, **kwargs)
        self.parameters = parameters
        self.sql = sql
        self.conn_id = conn_id
        self.autocommit = autocommit

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = BaseHook.get_hook(conn_id=self.conn_id)
        hook.run(sql=self.sql,
                 autocommit=self.autocommit,
                 parameters=self.parameters)

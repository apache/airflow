# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MsSqlOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file.
    File must have a '.sql' extensions.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, mssql_conn_id='mssql_default', parameters=None,
            autocommit=False, *args, **kwargs):
        super(MsSqlOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)

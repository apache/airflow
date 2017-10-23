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
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class JdbcOperator(BaseOperator):
    """
    Executes sql code in a database using jdbc driver.

    Requires jaydebeapi.

    :param jdbc_url: driver specific connection url with string variables, e.g. for exasol jdbc:exa:{0}:{1};schema={2}
    Template vars are defined like this: {0} = hostname, {1} = port, {2} = dbschema, {3} = extra
    :type jdbc_url: string
    :param jdbc_driver_name: classname of the specific jdbc driver, for exasol com.exasol.jdbc.EXADriver
    :type jdbc_driver_name: string
    :param jdbc_driver_loc: absolute path to jdbc driver location, for example /var/exasol/exajdbc.jar
    :type jdbc_driver_loc: string

    :param conn_id: reference to a predefined database
    :type conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            jdbc_conn_id='jdbc_default', autocommit=False, parameters=None,
            *args, **kwargs):
        super(JdbcOperator, self).__init__(*args, **kwargs)
        self.parameters = parameters

        self.sql = sql
        self.jdbc_conn_id = jdbc_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)

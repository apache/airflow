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
<<<<<<< HEAD
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

_log = logging.getLogger(__name__)


class MySqlOperator(BaseOperator):
    """
    Executes sql code in a specific MySQL database

    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: string
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
            self, sql, mysql_conn_id='mysql_default', parameters=None,
            autocommit=False, *args, **kwargs):
        super(MySqlOperator, self).__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        _log.info('Executing: ' + str(self.sql))
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters)
=======
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module is deprecated. Please use `airflow.providers.mysql.operators.mysql`."""

import warnings

# pylint: disable=unused-import
from airflow.providers.mysql.operators.mysql import MySqlOperator  # noqa

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.mysql.operators.mysql`.",
    DeprecationWarning, stacklevel=2
)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d

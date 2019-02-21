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

from builtins import str

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SqlSensor(BaseSensorOperator):
    """
    Runs a sql statement until a criteria is met. It will keep trying while
    sql returns no row, or if the first cell in (0, '0', '').

    :param conn_id: The connection to run the sensor against
    :type conn_id: str
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    :type sql: str
    :param parameters: The parameters to render the SQL query with (optional).
    :type parameters: mapping or iterable
    """
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#7c7287'

    @apply_defaults
    def __init__(self, conn_id, sql, parameters=None, *args, **kwargs):
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        super(SqlSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        conn = BaseHook.get_connection(self.conn_id)

        allowed_conn_type = {'google_cloud_platform', 'jdbc', 'mssql',
                             'mysql', 'oracle', 'postgres',
                             'presto', 'sqlite', 'vertica'}
        if conn.conn_type not in allowed_conn_type:
            raise AirflowException("The connection type is not supported by SqlSensor. " +
                                   "Supported connection types: {}".format(list(allowed_conn_type)))
        hook = conn.get_hook()

        self.log.info('Poking: %s (with parameters %s)', self.sql, self.parameters)
        records = hook.get_records(self.sql, self.parameters)
        if not records:
            return False
        else:
            if str(records[0][0]) in ('0', '',):
                return False
            else:
                return True

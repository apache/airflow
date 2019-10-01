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
"""DbApiHook Implementation for Apache Drill"""

from pydrill.client import PyDrill
from pydrill.exceptions import PyDrillException

from airflow.hooks.dbapi_hook import DbApiHook


class DrillHook(DbApiHook):
    """
    Interact with Drill through  PyDrill!

    >>> dh = DrillHook()
    >>> dh.get_records('SELECT full_name FROM cp.`employee.json` LIMIT 1')
    [{'full_name': 'Sheri Nowmer'}]
    """

    conn_name_attr = 'drill_conn_id'
    default_conn_name = 'drill_default'

    def __init__(self, *args, **kwargs,):
        super().__init__(*args, **kwargs)
        self.drill_conn_id = self.default_conn_name

    def get_conn(self):
        """
       Returns a drill connection object
        """
        conn = self.get_connection(self.drill_conn_id)

        auth = None
        if conn.login and conn.password:
            auth = conn.login + ":" + conn.password

        return PyDrill(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            auth=auth,

        )

    def get_records(self, sql, parameters=None):
        """
        Get a set of records from Drill
        """
        try:
            return self.get_conn().query(sql=sql).rows
        except Exception as e:
            raise PyDrillException(e)

    def get_pandas_df(self, sql, parameters=None):
        """
            Executes the sql and returns a pandas dataframe
        """
        try:
            return self.get_conn().query(sql=sql).to_dataframe()
        except Exception as e:
            raise PyDrillException(e)

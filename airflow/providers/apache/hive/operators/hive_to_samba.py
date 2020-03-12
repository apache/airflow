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

"""
This module contains operator to move data from Hive to Samba.
"""

from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars


class Hive2SambaOperator(BaseOperator):
    """
    Executes hql code in a specific Hive database and loads the
    results of the query as a csv to a Samba location.

    :param hql: the hql to be exported. (templated)
    :type hql: str
    :param destination_filepath: the file path to where the file will be pushed onto samba
    :type destination_filepath: str
    :param samba_conn_id: reference to the samba destination
    :type samba_conn_id: str
    :param hiveserver2_conn_id: reference to the hiveserver2 service
    :type hiveserver2_conn_id: str
    """

    template_fields = ('hql', 'destination_filepath')
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(self,
                 hql: str,
                 destination_filepath: str,
                 samba_conn_id: str = 'samba_default',
                 hiveserver2_conn_id: str = 'hiveserver2_default',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.samba_conn_id = samba_conn_id
        self.destination_filepath = destination_filepath
        self.hql = hql.strip().rstrip(';')

    def execute(self, context):
        with NamedTemporaryFile() as tmp_file:
            self.log.info("Fetching file from Hive")
            hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)
            hive.to_csv(hql=self.hql, csv_filepath=tmp_file.name, hive_conf=context_to_airflow_vars(context))
            self.log.info("Pushing to samba")
            samba = SambaHook(samba_conn_id=self.samba_conn_id)
            samba.push_from_local(self.destination_filepath, tmp_file.name)

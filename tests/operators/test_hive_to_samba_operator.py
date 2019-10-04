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

import unittest
from unittest.mock import Mock, PropertyMock, patch

from airflow.operators.hive_to_samba_operator import Hive2SambaOperator
from airflow.utils.operator_helpers import context_to_airflow_vars


class TestHive2SambaOperator(unittest.TestCase):

    def setUp(self):
        self.kwargs = dict(
            hql='hql',
            destination_filepath='destination_filepath',
            samba_conn_id='samba_default',
            hiveserver2_conn_id='hiveserver2_default',
            task_id='test_hive_to_samba_operator',
            dag=None
        )

    @patch('airflow.operators.hive_to_samba_operator.SambaHook')
    @patch('airflow.operators.hive_to_samba_operator.HiveServer2Hook')
    @patch('airflow.operators.hive_to_samba_operator.NamedTemporaryFile')
    def test_execute(self, mock_tmp_file, mock_hive_hook, mock_samba_hook):
        type(mock_tmp_file).name = PropertyMock(return_value='tmp_file')
        mock_tmp_file.return_value.__enter__ = Mock(return_value=mock_tmp_file)
        context = {}

        Hive2SambaOperator(**self.kwargs).execute(context)

        mock_hive_hook.assert_called_once_with(hiveserver2_conn_id=self.kwargs['hiveserver2_conn_id'])
        mock_hive_hook.return_value.to_csv.assert_called_once_with(
            hql=self.kwargs['hql'],
            csv_filepath=mock_tmp_file.name,
            hive_conf=context_to_airflow_vars(context))
        mock_samba_hook.assert_called_once_with(samba_conn_id=self.kwargs['samba_conn_id'])
        mock_samba_hook.return_value.push_from_local.assert_called_once_with(
            self.kwargs['destination_filepath'], mock_tmp_file.name)

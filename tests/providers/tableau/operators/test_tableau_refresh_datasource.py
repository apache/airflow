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
from unittest.mock import Mock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.tableau.operators.tableau_refresh_datasource import TableauRefreshDatasourceOperator


class TestTableauRefreshDatasourceOperator(unittest.TestCase):
    """
    Test class for TableauRefreshDatasourceOperator
    """

    def setUp(self):
        """
        setup
        """
        self.mock_datasources = []
        for i in range(3):
            mock_datasource = Mock()
            mock_datasource.id = i
            mock_datasource.name = f'ds_{i}'
            self.mock_datasources.append(mock_datasource)
        self.kwargs = {'site_id': 'test_site', 'task_id': 'task', 'dag': None}

    @patch('airflow.providers.tableau.operators.tableau_refresh_datasource.TableauHook')
    def test_execute(self, mock_tableau_hook):
        """
        Test Execute
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauRefreshDatasourceOperator(blocking=False, datasource_name='ds_2', **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id

    @patch('airflow.providers.tableau.operators.tableau_refresh_datasource.TableauHook')
    def test_execute_blocking(self, mock_tableau_hook):
        """
        Test execute blocking
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauRefreshDatasourceOperator(datasource_name='ds_2', **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id
        mock_tableau_hook.waiting_until_succeeded.assert_called_once_with(
            job_id=job_id,
        )

    @patch('airflow.providers.tableau.operators.tableau_refresh_datasource.TableauHook')
    def test_execute_missing_datasource(self, mock_tableau_hook):
        """
        Test execute missing datasource
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauRefreshDatasourceOperator(datasource_name='test', **self.kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})

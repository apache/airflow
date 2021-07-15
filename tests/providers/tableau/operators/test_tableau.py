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
from airflow.providers.tableau.operators.tableau import TableauOperator


class TestTableauOperator(unittest.TestCase):
    """
    Test class for TableauOperator
    """

    def setUp(self):
        """
        setup
        """

        self.mocked_workbooks = []
        self.mock_datasources = []

        for i in range(3):
            mock_workbook = Mock()
            mock_workbook.id = i
            mock_workbook.name = f'wb_{i}'
            self.mocked_workbooks.append(mock_workbook)

            mock_datasource = Mock()
            mock_datasource.id = i
            mock_datasource.name = f'ds_{i}'
            self.mock_datasources.append(mock_datasource)

        self.kwargs = {
            'site_id': 'test_site',
            'task_id': 'task',
            'dag': None,
            'match_with': 'name',
            'method': 'refresh'
        }

    @patch('airflow.providers.tableau.operators.tableau.TableauHook')
    def test_execute_workbooks(self, mock_tableau_hook):
        """
        Test Execute Workbooks
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(blocking_refresh=False, find='wb_2', resource='workbooks', **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.workbooks.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.workbooks.refresh.return_value.id == job_id

    @patch('airflow.providers.tableau.sensors.tableau_job_status.TableauJobStatusSensor')
    @patch('airflow.providers.tableau.operators.tableau.TableauHook')
    def test_execute_workbooks_blocking(self, mock_tableau_hook, mock_tableau_job_status_sensor):
        """
        Test execute workbooks blocking
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(find='wb_2', resource='workbooks', **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.workbooks.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.workbooks.refresh.return_value.id == job_id
        mock_tableau_job_status_sensor.assert_called_once_with(
            job_id=job_id,
            site_id=self.kwargs['site_id'],
            tableau_conn_id='tableau_default',
            task_id='wait_until_succeeded',
            dag=None,
        )

    @patch('airflow.providers.tableau.operators.tableau.TableauHook')
    def test_execute_missing_workbook(self, mock_tableau_hook):
        """
        Test execute missing workbook
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(find='test', resource='workbooks', **self.kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})

    @patch('airflow.providers.tableau.operators.tableau.TableauHook')
    def test_execute_datasources(self, mock_tableau_hook):
        """
        Test Execute datasources
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(find='test', resource='datasources', **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id

    @patch('airflow.providers.tableau.operators.tableau.TableauHook')
    def test_execute_datasources_blocking(self, mock_tableau_hook):
        """
        Test execute datasources blocking
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(
            return_value=mock_tableau_hook)
        operator = TableauOperator(find='test', resource='datasources', **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id
        mock_tableau_hook.waiting_until_succeeded.assert_called_once_with(
            job_id=job_id,
        )

    @patch('airflow.providers.tableau.operators.tableau.TableauHook')
    def test_execute_missing_datasource(self, mock_tableau_hook):
        """
        Test execute missing datasource
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(find='test', resource='datasources', **self.kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})


    def test_execute_unavailable_resource(self):
        """
        Test execute unavailable resource
        """
        operator = TableauOperator(resource='test', find='test', **self.kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})

    @patch('airflow.providers.tableau.operators.tableau.TableauHook')
    def test_get_resource_id(self, mock_tableau_hook):
        """
        Test get resource id
        """
        resource_id = 'res_id'
        operator = TableauOperator(
            resource='task', 
            find=resource_id, 
            method='run',
            task_id='t',
            dag=None
        )
        assert operator._get_resource_id(mock_tableau_hook) == resource_id

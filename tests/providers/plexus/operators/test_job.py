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

from unittest import mock
from unittest.mock import Mock
import pytest
from requests.exceptions import Timeout
from airflow.exceptions import AirflowException
from airflow.providers.plexus.operators.job import PlexusJobOperator


class TestPlexusOperator:
    @pytest.fixture
    def valid_job_params(self):
        return {
            'task_id': 'test',
            'job_params': {
                'name': 'test',
                'app': 'pipeline',
                'queue': 'queue',
                'num_nodes': 1,
                'num_cores': 1,
            },
        }

    @pytest.fixture
    def invalid_job_params(self, request):
        job_params = {'name': 'test', 'app': 'pipeline', 'queue': 'queue', 'num_nodes': 1, 'num_cores': 1}
        job_params.pop(request.param, None)
        return {'task_id': 'test', 'job_params': job_params}

    @pytest.fixture
    def invalid_lookups(self):
        return {
            'task_id': 'test',
            'job_params': {
                'name': 'test',
                'app': '_pipeline',
                'queue': '_queue',
                'num_nodes': 1,
                'num_cores': 1,
            },
        }

    def test_valid_job_params(self, valid_job_params):
        """test params are initialized correctly"""
        expected_job_params = {
            'name': 'test',
            'app': 'pipeline',
            'queue': 'queue',
            'num_nodes': 1,
            'num_cores': 1,
            'billing_account_id': None,
        }
        plexus_job_operator = PlexusJobOperator(**valid_job_params)
        assert plexus_job_operator.job_params == expected_job_params
        assert plexus_job_operator.is_service is None

    @pytest.mark.parametrize(
        "invalid_job_params", ['name', 'app', 'queue', 'num_nodes', 'num_cores'], indirect=True
    )
    @mock.patch('airflow.providers.plexus.operators.job.PlexusHook')
    def test_invalid_job_params(self, mock_hook, invalid_job_params):
        """test required job params are supplied"""
        plexus_job_operator = PlexusJobOperator(**invalid_job_params)
        with pytest.raises(AirflowException):
            plexus_job_operator.construct_job_params(mock_hook)

    @mock.patch('airflow.providers.plexus.operators.job.requests')
    @mock.patch('airflow.providers.plexus.operators.job.PlexusHook')
    def test_valid_api_lookups(self, mock_hook, mock_requests, valid_job_params):
        """test api lookups are correct"""
        queue_mock = Mock(
            **{
                'json.return_value': {
                    'results': [
                        {'id': 2, 'public_name': 'queue2'},
                        {'id': 3, 'public_name': 'queue3'},
                        {'id': 1, 'public_name': 'queue'},
                    ]
                }
            }
        )
        billing_mock = Mock(**{'json.return_value': {'results': [{'id': 1}]}})
        app_mock = Mock(
            **{
                'json.return_value': {
                    'results': [
                        {'name': 'pipeline2', 'id': 2, 'is_service': True},
                        {'name': 'pipeline3', 'id': 3, 'is_service': False},
                        {'name': 'pipeline', 'id': 1, 'is_service': False},
                    ]
                }
            }
        )
        mock_requests.get.side_effect = [queue_mock, billing_mock, app_mock]

        plexus_job_operator = PlexusJobOperator(**valid_job_params)
        assert plexus_job_operator._api_lookup('queue', mock_hook) == 1
        assert plexus_job_operator._api_lookup('billing_account_id', mock_hook) == 1
        assert plexus_job_operator.is_service is None
        assert plexus_job_operator._api_lookup('app', mock_hook) == 1
        assert plexus_job_operator.is_service is not None

    @mock.patch('airflow.providers.plexus.operators.job.requests')
    @mock.patch('airflow.providers.plexus.operators.job.PlexusHook')
    def test_invalid_api_lookups(self, mock_hook, mock_requests, invalid_lookups):
        """test api lookups that are not found"""
        queue_mock = Mock(**{'json.return_value': {'results': [{'id': 2, 'public_name': 'queue2'}]}})
        billing_mock = Mock(**{'json.return_value': {'results': [{'id': None}]}})
        app_mock = Mock(
            **{'json.return_value': {'results': [{'name': 'pipeline2', 'id': 2, 'is_service': True}]}}
        )
        mock_requests.get.side_effect = [queue_mock, billing_mock, app_mock]

        plexus_job_operator = PlexusJobOperator(**invalid_lookups)
        with pytest.raises(AirflowException):
            plexus_job_operator._api_lookup('queue', mock_hook)
            plexus_job_operator._api_lookup('app', mock_hook)
            plexus_job_operator._api_lookup('billing_account_id', mock_hook)

    @mock.patch('airflow.providers.plexus.operators.job.time')
    @mock.patch('airflow.providers.plexus.operators.job.requests')
    @mock.patch.object(PlexusJobOperator, 'construct_job_params')
    @mock.patch('airflow.providers.plexus.operators.job.PlexusHook')
    def test_create_job(self, mock_hook, mock_construct_params, mock_request, mock_time, valid_job_params):
        """test submit/get job"""
        mock_construct_params.return_value = {
            'name': 'test',
            'app': 1,
            'queue': 2,
            'num_nodes': 1,
            'num_cores': 1,
            'billing_account_id': 3,
        }

        # assert Error for is_service is None
        plexus_job_operator = PlexusJobOperator(**valid_job_params)
        with pytest.raises(AirflowException):
            plexus_job_operator.execute(None)

        # assert create_job timeout
        plexus_job_operator.is_service = False
        mock_request.post.side_effect = Timeout
        with pytest.raises(Timeout):
            plexus_job_operator.execute(None)

        # assert create_job error
        mock_request.post.side_effect = None
        mock_request.post.return_value = Mock(ok=False, status_code=400, reason='Bad', text='Bad')
        with pytest.raises(AirflowException):
            plexus_job_operator.execute(None)

        # assert get_job error
        mock_request.post.return_value = Mock(
            **{'ok': True, 'json.return_value': {'id': 1, 'last_state': 'Pending'}}
        )
        mock_request.get.return_value = Mock(ok=False)
        with pytest.raises(AirflowException):
            plexus_job_operator.execute(None)

        # assert get_job state error
        mock_request.get.return_value = Mock(**{'ok': True, 'json.return_value': {'last_state': 'Failed'}})
        with pytest.raises(AirflowException):
            plexus_job_operator.execute(None)

        # assert get_job state error
        mock_request.get.return_value = Mock(**{'ok': True, 'json.return_value': {'last_state': 'Cancelled'}})
        with pytest.raises(AirflowException):
            plexus_job_operator.execute(None)

        # assert get_job timeout
        mock_request.get.side_effect = Timeout
        with pytest.raises(Timeout):
            plexus_job_operator.execute(None)

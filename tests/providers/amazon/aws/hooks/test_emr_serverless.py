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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook

task_id = 'test_emr_serverless_create_application_operator'
application_id = 'test_application_id'
release_label = 'test'
job_type = 'test'
client_request_token = 'eac427d0-1c6d4df=-96aa-32423412'
config = {'name': 'test_application_emr_serverless'}


class TestEmrServerlessHook:
    def test_conn_attribute(self):
        hook = EmrServerlessHook(aws_conn_id='aws_default')
        assert hasattr(hook, 'conn')
        # Testing conn is a cached property
        conn = hook.conn
        conn2 = hook.conn
        assert conn is conn2

    def test_waiter_failure_then_success(self):
        mock_call_function = mock.MagicMock()
        mock_call_function.side_effect = [{'response': 'test_failure'}, {'response': 'test_success'}]
        success_state = {'test_success'}
        hook = EmrServerlessHook()
        waiter_response = hook.waiter(
            get_state_callable=mock_call_function,
            get_state_args={},
            parse_response=['response'],
            desired_state=success_state,
            failure_states={},
            object_type='test_object',
            action='testing',
            check_interval_seconds=1,
        )
        assert mock_call_function.call_count == 2
        assert waiter_response is None

    def test_waiter_success_state(self):
        mock_call_function = mock.MagicMock()
        mock_call_function.return_value = {'response': 'test_success'}
        success_state = {'test_success'}
        hook = EmrServerlessHook()
        waiter_response = hook.waiter(
            get_state_callable=mock_call_function,
            get_state_args={},
            parse_response=['response'],
            desired_state=success_state,
            failure_states={},
            object_type='test_object',
            action='testing',
        )
        mock_call_function.assert_called_once()
        assert waiter_response is None

    def test_waiter_failure_state(self):
        mock_call_function = mock.MagicMock()
        failure_state = {'test_failure'}
        mock_call_function.return_value = {'response': 'test_failure'}
        hook = EmrServerlessHook()
        with pytest.raises(AirflowException) as ex_message:
            hook.waiter(
                get_state_callable=mock_call_function,
                get_state_args={},
                parse_response=['response'],
                desired_state={},
                failure_states=failure_state,
                object_type='test_object',
                action='testing',
            )
        mock_call_function.assert_called_once()
        assert str(ex_message.value) == f"Test_Object reached failure state {','.join(failure_state)}."

    def test_nested_waiter_success_state(self):
        mock_call_function = mock.MagicMock()
        mock_call_function.return_value = {
            'layer1': {'key1': 'value1', 'layer2': {'response': 'test_success'}}
        }
        success_state = {'test_success'}
        hook = EmrServerlessHook()
        waiter_response = hook.waiter(
            get_state_callable=mock_call_function,
            get_state_args={},
            parse_response=['layer1', 'layer2', 'response'],
            desired_state=success_state,
            failure_states={},
            object_type='test_object',
            action='testing',
        )
        mock_call_function.assert_called_once()
        assert waiter_response is None

    def test_waiter_timeout(self):
        mock_call_function = mock.MagicMock()
        success_state = {'test_success'}
        mock_call_function.return_value = {'response': 'pending'}
        hook = EmrServerlessHook()
        with pytest.raises(RuntimeError) as ex_message:
            hook.waiter(
                get_state_callable=mock_call_function,
                get_state_args={},
                parse_response=['response'],
                desired_state=success_state,
                failure_states={},
                object_type='test_object',
                action='testing',
                check_interval_seconds=1,
                countdown=3,
            )
        assert mock_call_function.call_count == 4
        assert (
            str(ex_message.value)
            == f'{"test_object".title()} still not {"testing".lower()} after the allocated time limit.'
        )

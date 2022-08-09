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
from datetime import timedelta
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.hooks.ecs import EcsHook, EcsTaskLogFetcher, should_retry, should_retry_eni

try:
    from moto import mock_ecs
except ImportError:
    mock_ecs = None

DEFAULT_CONN_ID: str = 'aws_default'
REGION: str = 'us-east-1'


@pytest.fixture
def mock_conn():
    with mock.patch.object(EcsHook, 'conn') as _conn:
        yield _conn


@pytest.mark.skipif(mock_ecs is None, reason="mock_ecs package not present")
class TestEksHooks:
    def test_hook(self) -> None:
        hook = EcsHook(region_name=REGION)
        assert hook.conn is not None
        assert hook.aws_conn_id == DEFAULT_CONN_ID
        assert hook.region_name == REGION

    def test_get_cluster_state(self, mock_conn) -> None:
        mock_conn.describe_clusters.return_value = {'clusters': [{'status': 'ACTIVE'}]}
        assert EcsHook().get_cluster_state(cluster_name='cluster_name') == 'ACTIVE'

    def test_get_task_definition_state(self, mock_conn) -> None:
        mock_conn.describe_task_definition.return_value = {'taskDefinition': {'status': 'ACTIVE'}}
        assert EcsHook().get_task_definition_state(task_definition='task_name') == 'ACTIVE'

    def test_get_task_state(self, mock_conn) -> None:
        mock_conn.describe_tasks.return_value = {'tasks': [{'lastStatus': 'ACTIVE'}]}
        assert EcsHook().get_task_state(cluster='cluster_name', task='task_name') == 'ACTIVE'


class TestShouldRetry(unittest.TestCase):
    def test_return_true_on_valid_reason(self):
        self.assertTrue(should_retry(EcsOperatorError([{'reason': 'RESOURCE:MEMORY'}], 'Foo')))

    def test_return_false_on_invalid_reason(self):
        self.assertFalse(should_retry(EcsOperatorError([{'reason': 'CLUSTER_NOT_FOUND'}], 'Foo')))


class TestShouldRetryEni(unittest.TestCase):
    def test_return_true_on_valid_reason(self):
        self.assertTrue(
            should_retry_eni(
                EcsTaskFailToStart(
                    "The task failed to start due to: "
                    "Timeout waiting for network interface provisioning to complete."
                )
            )
        )

    def test_return_false_on_invalid_reason(self):
        self.assertFalse(
            should_retry_eni(
                EcsTaskFailToStart(
                    "The task failed to start due to: "
                    "CannotPullContainerError: "
                    "ref pull has been retried 5 time(s): failed to resolve reference"
                )
            )
        )


class TestEcsTaskLogFetcher(unittest.TestCase):
    @mock.patch('logging.Logger')
    def set_up_log_fetcher(self, logger_mock):
        self.logger_mock = logger_mock

        self.log_fetcher = EcsTaskLogFetcher(
            log_group="test_log_group",
            log_stream_name="test_log_stream_name",
            fetch_interval=timedelta(milliseconds=1),
            logger=logger_mock,
        )

    def setUp(self):
        self.set_up_log_fetcher()

    @mock.patch(
        'threading.Event.is_set',
        side_effect=(False, False, False, True),
    )
    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        side_effect=(
            iter(
                [
                    {'timestamp': 1617400267123, 'message': 'First'},
                    {'timestamp': 1617400367456, 'message': 'Second'},
                ]
            ),
            iter(
                [
                    {'timestamp': 1617400467789, 'message': 'Third'},
                ]
            ),
            iter([]),
        ),
    )
    def test_run(self, get_log_events_mock, event_is_set_mock):

        self.log_fetcher.run()

        self.logger_mock.info.assert_has_calls(
            [
                mock.call('[2021-04-02 21:51:07,123] First'),
                mock.call('[2021-04-02 21:52:47,456] Second'),
                mock.call('[2021-04-02 21:54:27,789] Third'),
            ]
        )

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        side_effect=ClientError({"Error": {"Code": "ResourceNotFoundException"}}, None),
    )
    def test_get_log_events_with_expected_error(self, get_log_events_mock):
        with pytest.raises(StopIteration):
            next(self.log_fetcher._get_log_events())

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        side_effect=Exception(),
    )
    def test_get_log_events_with_unexpected_error(self, get_log_events_mock):
        with pytest.raises(Exception):
            next(self.log_fetcher._get_log_events())

    def test_event_to_str(self):
        events = [
            {'timestamp': 1617400267123, 'message': 'First'},
            {'timestamp': 1617400367456, 'message': 'Second'},
            {'timestamp': 1617400467789, 'message': 'Third'},
        ]
        assert [self.log_fetcher._event_to_str(event) for event in events] == (
            [
                '[2021-04-02 21:51:07,123] First',
                '[2021-04-02 21:52:47,456] Second',
                '[2021-04-02 21:54:27,789] Third',
            ]
        )

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=(),
    )
    def test_get_last_log_message_with_no_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_message() is None

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=iter(
            [
                {'timestamp': 1617400267123, 'message': 'First'},
                {'timestamp': 1617400367456, 'message': 'Second'},
            ]
        ),
    )
    def test_get_last_log_message_with_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_message() == 'Second'

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=iter(
            [
                {'timestamp': 1617400267123, 'message': 'First'},
                {'timestamp': 1617400367456, 'message': 'Second'},
                {'timestamp': 1617400367458, 'message': 'Third'},
            ]
        ),
    )
    def test_get_last_log_messages_with_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_messages(2) == ['Second', 'Third']

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events',
        return_value=(),
    )
    def test_get_last_log_messages_with_no_log_events(self, mock_log_events):
        assert self.log_fetcher.get_last_log_messages(2) == []

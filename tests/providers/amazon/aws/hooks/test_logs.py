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
from __future__ import annotations

from unittest import mock
from unittest.mock import ANY, patch

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook


@mock_aws
class TestAwsLogsHook:
    @pytest.mark.parametrize(
        "get_log_events_response, num_skip_events, expected_num_events, end_time",
        [
            # 3 empty responses with different tokens
            (
                [
                    {"nextForwardToken": "1", "events": []},
                    {"nextForwardToken": "2", "events": []},
                    {"nextForwardToken": "3", "events": []},
                ],
                0,
                0,
                None,
            ),
            # 2 events on the second response with same token
            (
                [
                    {"nextForwardToken": "", "events": []},
                    {"nextForwardToken": "", "events": [{}, {}]},
                ],
                0,
                2,
                None,
            ),
            # Different tokens, 2 events on the second response then 3 empty responses
            (
                [
                    {"nextForwardToken": "1", "events": []},
                    {"nextForwardToken": "2", "events": [{}, {}]},
                    {"nextForwardToken": "3", "events": []},
                    {"nextForwardToken": "4", "events": []},
                    {"nextForwardToken": "5", "events": []},
                    # This one is ignored
                    {"nextForwardToken": "6", "events": [{}, {}]},
                ],
                0,
                2,
                10,
            ),
            # 2 events on the second response, then 2 empty responses, then 2 consecutive responses with
            # 2 events with the same token
            (
                [
                    {"nextForwardToken": "1", "events": []},
                    {"nextForwardToken": "2", "events": [{}, {}]},
                    {"nextForwardToken": "3", "events": []},
                    {"nextForwardToken": "4", "events": []},
                    {"nextForwardToken": "6", "events": [{}, {}]},
                    {"nextForwardToken": "6", "events": [{}, {}]},
                    # This one is ignored
                    {"nextForwardToken": "6", "events": [{}, {}]},
                ],
                0,
                6,
                20,
            ),
        ],
    )
    @patch("airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.conn", new_callable=mock.PropertyMock)
    def test_get_log_events(
        self, mock_conn, get_log_events_response, num_skip_events, expected_num_events, end_time
    ):
        mock_conn().get_log_events.side_effect = get_log_events_response

        log_group_name = "example-group"
        log_stream_name = "example-log-stream"
        hook = AwsLogsHook(aws_conn_id="aws_default", region_name="us-east-1")
        events = hook.get_log_events(
            log_group=log_group_name,
            log_stream_name=log_stream_name,
            skip=num_skip_events,
            end_time=end_time,
        )

        events = list(events)

        assert len(events) == expected_num_events
        kwargs = {
            "logGroupName": log_group_name,
            "logStreamName": log_stream_name,
            "startFromHead": True,
            "startTime": 0,
            "nextToken": ANY,
        }
        if end_time:
            kwargs["endTime"] = end_time
        mock_conn().get_log_events.assert_called_with(**kwargs)

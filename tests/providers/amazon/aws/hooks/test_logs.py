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
from unittest.mock import patch

import pytest
from moto import mock_logs

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook


@mock_logs
class TestAwsLogsHook:
    @pytest.mark.parametrize(
        "get_log_events_response, num_skip_events, expected_num_events",
        [
            # No event
            ({"events": []}, 0, 0),
            # 2 events in each call with no token, skip 2
            ({"events": [{}, {}]}, 2, 4),
            # 2 events in each call with no token
            ({"events": [{}, {}]}, 0, 6),
            # 2 events in each call. The third as a token
            (
                [
                    {"events": [{}, {}]},
                    {"events": [{}, {}]},
                    {"nextForwardToken": "token", "events": [{}, {}]},
                    {"events": [{}, {}]},
                    {"events": [{}, {}]},
                    {"events": [{}, {}]},
                ],
                0,
                12,
            ),
            # 2 responses with same nextForwardToken
            ({"nextForwardToken": "token", "events": [{}, {}]}, 0, 4),
        ],
    )
    @patch("airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.conn", new_callable=mock.PropertyMock)
    def test_get_log_events(self, mock_conn, get_log_events_response, num_skip_events, expected_num_events):
        if isinstance(get_log_events_response, list):
            mock_conn().get_log_events.side_effect = get_log_events_response
        else:
            mock_conn().get_log_events.return_value = get_log_events_response

        hook = AwsLogsHook(aws_conn_id="aws_default", region_name="us-east-1")
        events = hook.get_log_events(
            log_group="example-group",
            log_stream_name="example-log-stream",
            skip=num_skip_events,
        )

        events = list(events)

        assert len(events) == expected_num_events

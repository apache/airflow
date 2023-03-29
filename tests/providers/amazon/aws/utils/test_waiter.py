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

from typing import Any
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.utils.waiter import waiter

SUCCESS_STATES = {"Created"}
FAILURE_STATES = {"Failed"}


def generate_response(state: str) -> dict[str, Any]:
    return {
        "Status": {
            "State": state,
        },
    }


def assert_expected_waiter_type(waiter: mock.MagicMock, expected: str):
    """
    There does not appear to be a straight-forward way to assert the type of waiter.
    Instead, get the class name and check if it contains the expected name.

    :param waiter: A mocked Boto3 Waiter object.
    :param expected: The expected class name of the Waiter object, for example "ClusterActive".
    """
    assert expected in str(type(waiter.call_args[0][0]))


class TestWaiter:
    @pytest.mark.parametrize(
        "get_state_responses, fails, expected_exception, expected_num_calls",
        [
            ([generate_response("Created")], False, None, 1),
            ([generate_response("Failed")], True, AirflowException, 1),
            (
                [generate_response("Pending"), generate_response("Pending"), generate_response("Created")],
                False,
                None,
                3,
            ),
            (
                [generate_response("Pending"), generate_response("Failed")],
                True,
                AirflowException,
                2,
            ),
            (
                [generate_response("Pending"), generate_response("Pending"), generate_response("Failed")],
                True,
                AirflowException,
                3,
            ),
            ([generate_response("Pending") for i in range(10)], True, RuntimeError, 5),
        ],
    )
    @mock.patch("time.sleep", return_value=None)
    def test_waiter(self, _, get_state_responses, fails, expected_exception, expected_num_calls):
        mock_get_state = MagicMock()
        mock_get_state.side_effect = get_state_responses
        get_state_args = {}

        if fails:
            with pytest.raises(expected_exception):
                waiter(
                    get_state_callable=mock_get_state,
                    get_state_args=get_state_args,
                    parse_response=["Status", "State"],
                    desired_state=SUCCESS_STATES,
                    failure_states=FAILURE_STATES,
                    object_type="test_object",
                    action="testing",
                    check_interval_seconds=1,
                    countdown=5,
                )
        else:
            waiter(
                get_state_callable=mock_get_state,
                get_state_args=get_state_args,
                parse_response=["Status", "State"],
                desired_state=SUCCESS_STATES,
                failure_states=FAILURE_STATES,
                object_type="test_object",
                action="testing",
                check_interval_seconds=1,
                countdown=5,
            )

        assert mock_get_state.call_count == expected_num_calls

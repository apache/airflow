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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.utils.waiter import waiter


def get_state_test(test_id: str) -> dict[str, Any]:
    pass


SUCCESS_STATES = {"Created"}
FAILURE_STATES = {"Failed"}


class TestWaiter:
    def _generate_response(self, test_id, state: str) -> dict[str, Any]:
        return {
            "Id": test_id,
            "Status": {
                "State": state,
            },
        }

    @mock.patch("tests.providers.amazon.aws.utils.test_waiter.get_state_test")
    def test_waiter(self, mock_get_state):
        test_id = "test_id"
        mock_get_state.return_value = self._generate_response(test_id, "Created")
        waiter(
            get_state_callable=get_state_test,
            get_state_args={"test_id": test_id},
            parse_response=["Status", "State"],
            desired_state=SUCCESS_STATES,
            failure_states=FAILURE_STATES,
            object_type="test_object",
            action="testing",
        )

        assert mock_get_state.called_once()

    @mock.patch("tests.providers.amazon.aws.utils.test_waiter.get_state_test")
    def test_waiter_failure(self, mock_get_state):
        test_id = "test_id"
        mock_get_state.return_value = self._generate_response(test_id, "Failed")
        with pytest.raises(AirflowException) as ex_message:
            waiter(
                get_state_callable=get_state_test,
                get_state_args={"test_id": test_id},
                parse_response=["Status", "State"],
                desired_state=SUCCESS_STATES,
                failure_states=FAILURE_STATES,
                object_type="test_object",
                action="testing",
            )
        assert mock_get_state.called_once()
        assert "Test_Object reached failure state Failed." in str(ex_message.value)

    @mock.patch("tests.providers.amazon.aws.utils.test_waiter.get_state_test")
    @mock.patch("time.sleep", return_value=None)
    def test_waiter_multiple_attempts_success(self, _, mock_get_state):
        test_id = "test_id"
        test_data = [self._generate_response(test_id, "Pending") for i in range(2)]
        test_data.append(self._generate_response(test_id, "Created"))
        mock_get_state.side_effect = test_data

        waiter(
            get_state_callable=get_state_test,
            get_state_args={"test_id": test_id},
            parse_response=["Status", "State"],
            desired_state=SUCCESS_STATES,
            failure_states=FAILURE_STATES,
            object_type="test_object",
            action="testing",
            check_interval_seconds=1,
            countdown=5,
        )
        assert mock_get_state.call_count == 3

    @mock.patch("tests.providers.amazon.aws.utils.test_waiter.get_state_test")
    @mock.patch("time.sleep", return_value=None)
    def test_waiter_multiple_attempts_fail(self, _, mock_get_state):
        test_id = "test_id"
        test_data = [self._generate_response(test_id, "Pending") for i in range(2)]
        test_data.append(self._generate_response(test_id, "Failed"))
        mock_get_state.side_effect = test_data
        with pytest.raises(AirflowException) as ex_message:
            waiter(
                get_state_callable=get_state_test,
                get_state_args={"test_id": test_id},
                parse_response=["Status", "State"],
                desired_state=SUCCESS_STATES,
                failure_states=FAILURE_STATES,
                object_type="test_object",
                action="testing",
                check_interval_seconds=1,
                countdown=5,
            )
        assert mock_get_state.call_count == 3
        assert "Test_Object reached failure state Failed." in str(ex_message.value)

    @mock.patch("tests.providers.amazon.aws.utils.test_waiter.get_state_test")
    @mock.patch("time.sleep", return_value=None)
    def test_waiter_multiple_attempts_pending(self, _, mock_get_state):
        test_id = "test_id"
        mock_get_state.return_value = self._generate_response(test_id, "Pending")
        with pytest.raises(RuntimeError) as ex_message:
            waiter(
                get_state_callable=get_state_test,
                get_state_args={"test_id": test_id},
                parse_response=["Status", "State"],
                desired_state=SUCCESS_STATES,
                failure_states=FAILURE_STATES,
                object_type="test_object",
                action="testing",
                check_interval_seconds=1,
                countdown=5,
            )
            assert mock_get_state.call_count == 5
            assert "Test_Object still not testing after the allocated time limit." in str(ex_message.value)

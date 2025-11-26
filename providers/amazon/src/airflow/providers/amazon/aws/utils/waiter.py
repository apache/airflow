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

import logging
import time
from collections.abc import Callable
from enum import Enum

from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)


def waiter(
    get_state_callable: Callable,
    get_state_args: dict,
    parse_response: list,
    desired_state: set,
    failure_states: set,
    object_type: str,
    action: str,
    countdown: int | float | None = 25 * 60,
    check_interval_seconds: int = 60,
) -> None:
    """
    Call get_state_callable until it reaches the desired_state or the failure_states.

    PLEASE NOTE:  While not yet deprecated, we are moving away from this method
                  and encourage using the custom boto waiters as explained in
                  https://github.com/apache/airflow/tree/main/airflow/providers/amazon/aws/waiters

    :param get_state_callable: A callable to run until it returns True
    :param get_state_args: Arguments to pass to get_state_callable
    :param parse_response: Dictionary keys to extract state from response of get_state_callable
    :param desired_state: Wait until the getter returns this value
    :param failure_states: A set of states which indicate failure and should throw an
        exception if any are reached before the desired_state
    :param object_type: Used for the reporting string. What are you waiting for? (application, job, etc.)
    :param action: Used for the reporting string. What action are you waiting for? (created, deleted, etc.)
    :param countdown: Number of seconds the waiter should wait for the desired state before timing out.
        Defaults to 25 * 60 seconds. None = infinite.
    :param check_interval_seconds: Number of seconds waiter should wait before attempting
        to retry get_state_callable. Defaults to 60 seconds.
    """
    while True:
        state = get_state(get_state_callable(**get_state_args), parse_response)
        if state in desired_state:
            break
        if state in failure_states:
            raise AirflowException(f"{object_type.title()} reached failure state {state}.")

        if countdown is None:
            countdown = float("inf")

        if countdown > check_interval_seconds:
            countdown -= check_interval_seconds
            log.info("Waiting for %s to be %s.", object_type.lower(), action.lower())
            time.sleep(check_interval_seconds)
        else:
            message = f"{object_type.title()} still not {action.lower()} after the allocated time limit."
            log.error(message)
            raise RuntimeError(message)


def get_state(response, keys) -> str:
    value = response
    for key in keys:
        if value is not None:
            value = value.get(key, None)
    return value


class WaitPolicy(str, Enum):
    """
    Used to control the waiting behaviour within EMRClusterJobFlowOperator.

    Choices:
    - WAIT_FOR_COMPLETION - Will wait for the cluster to report "Running" state
    - WAIT_FOR_STEPS_COMPLETION - Will wait for the cluster to report "Terminated" state
    """

    WAIT_FOR_COMPLETION = "wait_for_completion"
    WAIT_FOR_STEPS_COMPLETION = "wait_for_steps_completion"


WAITER_POLICY_NAME_MAPPING: dict[WaitPolicy, str] = {
    WaitPolicy.WAIT_FOR_COMPLETION: "job_flow_waiting",
    WaitPolicy.WAIT_FOR_STEPS_COMPLETION: "job_flow_terminated",
}

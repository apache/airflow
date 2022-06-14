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

from time import sleep
from typing import Callable, Dict, List, Set

from airflow.exceptions import AirflowException


# This method should be replaced with boto waiters which would implement timeouts and backoff nicely.
def waiter(
    get_state_callable: Callable,
    get_state_args: Dict,
    parse_response: List,
    desired_state: Set,
    failure_states: Set,
    object_type: str,
    action: str,
) -> None:
    """
    Will run the sensor until it turns True.

    :param get_state_callable: A callable to run until it returns True, likely a Sensor
    :param get_state_args: Arguments to pass the sensor
    :param parse_response: Dictionary keys to extract state from callable response
    :param desired_state: Wait until the getter returns this value
    :param failure_states: A set of states which indicate failure and should throw an
      exception if any are reached before the desired_state
    :param object_type: Used for the reporting string. What are you waiting for? (application, job, etc)
    :param action: Used for the reporting string. What action are you waiting for? (created, deleted, etc)
    """
    check_interval_seconds: int = 15
    timeout_seconds: int = 25 * 60
    response = get_state_callable(**get_state_args)
    countdown: int = timeout_seconds
    status: str = get_state(response, parse_response)
    while status not in desired_state:
        if status in failure_states:
            raise AirflowException(f'{object_type.title()} reached failure state {status}.')
        if countdown >= check_interval_seconds:
            countdown -= check_interval_seconds
            print(f'Waiting for {object_type.lower()} to be {action.lower()}.')
            sleep(check_interval_seconds)
            status = get_state(get_state_callable(**get_state_args), parse_response)
        else:
            message = f'{object_type.title()} still not {action.lower()} after the allocated time limit.'
            print(message)
            raise RuntimeError(message)


def get_state(response, keys) -> str:
    value = response
    for key in keys:
        if value is not None:
            value = value.get(key, None)
    return value

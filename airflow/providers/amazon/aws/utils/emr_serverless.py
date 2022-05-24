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
from typing import Callable, Dict


# This method should be replaced with boto waiters which would implement timeouts and backoff nicely.
def waiter(sensor: Callable, sensor_args: Dict, object_type: str, action: str) -> None:
    """
    Will run the sensor until it turns True.

    :param sensor: A callable to run until it returns True, likely a Sensor
    :param sensor_args: Arguments to pass the sensor
    :param object_type: Used for the reporting string. What are you waiting for? (application, job, etc)
    :param action: Used for the reporting string. What action are you waiting for? (created, deleted, etc)
    """
    check_interval_seconds = 15
    timeout_seconds = 25 * 60

    countdown = timeout_seconds
    while not sensor(**sensor_args):
        if countdown >= check_interval_seconds:
            countdown -= check_interval_seconds
            print(f'Waiting for {object_type.lower()} to be {action.lower()}.')
            sleep(check_interval_seconds)
        else:
            message = f'{object_type.title()} still not {action.lower()} after the allocated time limit.'
            print(message)
            raise RuntimeError(message)

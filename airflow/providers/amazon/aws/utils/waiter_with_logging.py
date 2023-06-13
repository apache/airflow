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

from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException


def wait(
    waiter,
    waiter_delay,
    max_attempts,
    state_args,
    failure_message,
    status_message,
):
    log = logging.getLogger(__name__)
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            waiter.wait(
                **state_args,
                WaiterConfig={
                    "Delay": 1,
                    "MaxAttempts": 1,
                },
            )
            break
        except WaiterError as error:
            if "terminal failure" in str(error):
                raise AirflowException(f"{failure_message}: {error}")
            response_data = []
            for arg in status_message["args"]:
                response_data.append(get_state(error.last_response, arg))

            log.info(status_message["message"] + " : " + " - ".join(response_data))
            time.sleep(waiter_delay)
    if attempt >= max_attempts:
        raise AirflowException("Waiter error: max attempts reached")


def get_state(response, keys) -> str:
    value = response
    for key in keys:
        if value != "":
            if isinstance(value, list):
                try:
                    index = int(key)
                    if 0 <= index < len(value):
                        value = value[index]
                    else:
                        value = ""
                except ValueError:
                    value = ""  # Invalid index, set value to empty string
            else:
                value = value.get(key, "")
    return value

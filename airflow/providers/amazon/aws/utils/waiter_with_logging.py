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

import jmespath
from botocore.exceptions import WaiterError
from botocore.waiter import Waiter

from airflow.exceptions import AirflowException


def wait(
    waiter: Waiter,
    waiter_delay: int,
    max_attempts: int,
    args: dict,
    failure_message: str,
    status_message: str,
    status_args: list,
) -> None:
    """
    Use a boto waiter to poll an AWS service for the specified state. Although this function
    uses boto waiters to poll the state of the service, it logs the response of the service
    after every attempt, which is not currently supported by boto waiters.

    :param waiter: The boto waiter to use.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param args: The arguments to pass to the waiter.
    :param failure_message: The message to log if a failure state is reached.
    :param status_message: The message logged when printing the status of the service.
    :param status_args: A list containing the arguments to retrieve status information from
        the waiter response.
        e.g.
        response = {"Cluster": {"state": "CREATING"}}
        status_args = ["Cluster.state"]

        response = {
        "Clusters": [{"state": "CREATING", "details": "User initiated."},]
        }
        status_args = ["Clusters[0].state", "Clusters[0].details"]
    """
    log = logging.getLogger(__name__)
    attempt = 0
    while True:
        attempt += 1
        try:
            waiter.wait(**args, WaiterConfig={"MaxAttempts": 1})
            break
        except WaiterError as error:
            if "terminal failure" in str(error):
                raise AirflowException(f"{failure_message}: {error}")
            status_string = _format_status_string(status_args, error.last_response)
            log.info("%s: %s", status_message, status_string)
            time.sleep(waiter_delay)

            if attempt >= max_attempts:
                raise AirflowException("Waiter error: max attempts reached")


def _format_status_string(args, response):
    """
    Loops through the supplied args list and generates a string
    which contains values from the waiter response.
    """
    values = []
    for arg in args:
        value = jmespath.search(arg, response)
        if value is not None and value != "":
            values.append(str(value))

    return " - ".join(values)

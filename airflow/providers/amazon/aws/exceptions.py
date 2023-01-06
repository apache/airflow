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

from airflow import AirflowException

# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state


class EcsTaskFailToStart(Exception):
    """Raise when ECS tasks fail to start AFTER processing the request."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)

    def __reduce__(self):
        return EcsTaskFailToStart, (self.message)


class EcsOperatorError(Exception):
    """Raise when ECS cannot handle the request."""

    def __init__(self, failures: list, message: str):
        self.failures = failures
        self.message = message
        super().__init__(message)

    def __reduce__(self):
        return EcsOperatorError, (self.failures, self.message)


class S3HookUriParseFailure(AirflowException):
    """When parse_s3_url fails to parse URL, this error is thrown."""

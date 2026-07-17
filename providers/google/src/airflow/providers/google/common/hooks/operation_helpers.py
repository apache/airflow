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
"""This module contains a helper class to work with `google.api_core.operation.Operation` object."""

from __future__ import annotations

from typing import TYPE_CHECKING

from google.api_core.exceptions import GoogleAPICallError

from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from proto import Message


class OperationHelper:
    """Helper class to work with `operation.Operation` objects."""

    @staticmethod
    def wait_for_operation_result(
        operation: Operation,
        timeout: int | None = None,
        polling: Retry | None = None,
        retry: Retry | None = None,
    ) -> Message:
        """
        Wait for long-lasting operation result to be retrieved.

        For advance usage please check the docs on:
        :class:`google.api_core.future.polling.PollingFuture`
        :class:`google.api_core.retry.Retry`

        :param operation: The initial operation to get result from.
        :param timeout: How long (in seconds) to wait for the operation to complete.
            If None, wait indefinitely. Overrides polling.timeout if both specified.
        :param polling: How often and for how long to call polling RPC periodically.
        :param retry: How to retry the operation polling if error occurs.
        """
        try:
            return operation.result(timeout=timeout, polling=polling, retry=retry)
        except GoogleAPICallError as ex:
            raise AirflowException("Google API error on operation result call") from ex
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def wait_for_operation(
        self,
        operation: Operation,
        timeout: float | int | None = None,
    ):
        """
        Legacy method name wrapper.

        Intended to use with existing hooks/operators, until the proper deprecation and replacement provided.
        """
        if isinstance(timeout, float):
            timeout = int(timeout)

        return self.wait_for_operation_result(operation=operation, timeout=timeout)

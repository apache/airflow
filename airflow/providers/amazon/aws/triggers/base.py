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

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class AwsBaseWaiterTrigger(BaseTrigger):
    """
    Base class for all AWS Triggers that follow the "standard" model of just waiting on a waiter.

    Subclasses need to implement the hook() method.

    :param serialized_fields: Fields that are specific to the subclass trigger and need to be serialized
        to be passed to the __init__ method on deserialization.
        The conn id, region, and waiter delay & attempts are always serialized.
        format: {<parameter_name>: <parameter_value>}

    :param waiter_name: The name of the (possibly custom) boto waiter to use.

    :param waiter_args: The arguments to pass to the waiter.
    :param failure_message: The message to log if a failure state is reached.
    :param status_message: The message logged when printing the status of the service.
    :param status_queries: A list containing the JMESPath queries to retrieve status information from
        the waiter response. See https://jmespath.org/tutorial.html

    :param return_key: The key to use for the return_value in the TriggerEvent this emits on success.
        Defaults to "value".
    :param return_value: A value that'll be returned in the return_key field of the TriggerEvent.
        Set to None if there is nothing to return.

    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials. To be used to build the hook.
    :param region_name: The AWS region where the resources to watch are. To be used to build the hook.
    :param verify: Whether or not to verify SSL certificates. To be used to build the hook.
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client.
        To be used to build the hook. For available key-values see:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    def __init__(
        self,
        *,
        serialized_fields: dict[str, Any],
        waiter_name: str,
        waiter_args: dict[str, Any],
        failure_message: str,
        status_message: str,
        status_queries: list[str],
        return_key: str = "value",
        return_value: Any,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ):
        super().__init__()
        # parameters that should be hardcoded in the child's implem
        self.serialized_fields = serialized_fields

        self.waiter_name = waiter_name
        self.waiter_args = waiter_args
        self.failure_message = failure_message
        self.status_message = status_message
        self.status_queries = status_queries

        self.return_key = return_key
        self.return_value = return_value

        # parameters that should be passed directly from the child's parameters
        self.waiter_delay = waiter_delay
        self.attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    def serialize(self) -> tuple[str, dict[str, Any]]:
        # here we put together the "common" params,
        # and whatever extras we got from the subclass in serialized_fields
        params = dict(
            {
                "waiter_delay": self.waiter_delay,
                "waiter_max_attempts": self.attempts,
                "aws_conn_id": self.aws_conn_id,
            },
            **self.serialized_fields,
        )

        # if we serialize the None value from this, it breaks subclasses that don't have it in their ctor.
        params.update(
            prune_dict(
                {
                    # Keep previous behaviour when empty string in region_name evaluated as `None`
                    "region_name": self.region_name or None,
                    "verify": self.verify,
                    "botocore_config": self.botocore_config,
                }
            )
        )

        return (
            # remember that self is an instance of the subclass here, not of this class.
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            params,
        )

    @abstractmethod
    def hook(self) -> AwsGenericHook:
        """Override in subclasses to return the right hook."""

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self.hook()
        async with hook.async_conn as client:
            waiter = hook.get_waiter(self.waiter_name, deferrable=True, client=client)
            await async_wait(
                waiter,
                self.waiter_delay,
                self.attempts,
                self.waiter_args,
                self.failure_message,
                self.status_message,
                self.status_queries,
            )
            yield TriggerEvent({"status": "success", self.return_key: self.return_value})

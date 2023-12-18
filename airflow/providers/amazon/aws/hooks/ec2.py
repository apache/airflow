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

import functools
import time
from typing import Callable, TypeVar

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.typing_compat import ParamSpec

PS = ParamSpec("PS")
RT = TypeVar("RT")


def only_client_type(func: Callable[PS, RT]) -> Callable[PS, RT]:
    @functools.wraps(func)
    def checker(*args, **kwargs) -> RT:
        if args[0]._api_type == "client_type":
            return func(*args, **kwargs)

        ec2_doc_link = "https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html"
        raise AirflowException(
            f"""
            This method is only callable when using client_type API for interacting with EC2.
            Create the EC2Hook object as follows to use this method

            ec2 = EC2Hook(api_type="client_type")

            Read following for details on client_type and resource_type APIs:
            1. {ec2_doc_link}#client
            2. {ec2_doc_link}#service-resource
            """
        )

    return checker


class EC2Hook(AwsBaseHook):
    """
    Interact with Amazon Elastic Compute Cloud (EC2).

    Provide thick wrapper around :external+boto3:py:class:`boto3.client("ec2") <EC2.Client>`
    or :external+boto3:py:class:`boto3.resource("ec2") <EC2.ServiceResource>`.

    :param api_type: If set to ``client_type`` then hook use ``boto3.client("ec2")`` capabilities,
        If set to ``resource_type`` then hook use ``boto3.resource("ec2")`` capabilities.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    API_TYPES = frozenset({"resource_type", "client_type"})

    def __init__(self, api_type="resource_type", *args, **kwargs) -> None:
        if api_type not in self.API_TYPES:
            raise AirflowException("api_type can only be one of %s", self.API_TYPES)

        kwargs[api_type] = "ec2"

        self._api_type = api_type

        super().__init__(*args, **kwargs)

    def get_instance(self, instance_id: str, filters: list | None = None):
        """
        Get EC2 instance by id and return it.

        :param instance_id: id of the AWS EC2 instance
        :param filters: List of filters to specify instances to get
        :return: Instance object
        """
        if self._api_type == "client_type":
            return self.get_instances(filters=filters, instance_ids=[instance_id])[0]

        return self.conn.Instance(id=instance_id)

    @only_client_type
    def stop_instances(self, instance_ids: list) -> dict:
        """
        Stop instances with given ids.

        :param instance_ids: List of instance ids to stop
        :return: Dict with key `StoppingInstances` and value as list of instances being stopped
        """
        self.log.info("Stopping instances: %s", instance_ids)

        return self.conn.stop_instances(InstanceIds=instance_ids)

    @only_client_type
    def start_instances(self, instance_ids: list) -> dict:
        """
        Start instances with given ids.

        :param instance_ids: List of instance ids to start
        :return: Dict with key `StartingInstances` and value as list of instances being started
        """
        self.log.info("Starting instances: %s", instance_ids)

        return self.conn.start_instances(InstanceIds=instance_ids)

    @only_client_type
    def terminate_instances(self, instance_ids: list) -> dict:
        """
        Terminate instances with given ids.

        :param instance_ids: List of instance ids to terminate
        :return: Dict with key `TerminatingInstances` and value as list of instances being terminated
        """
        self.log.info("Terminating instances: %s", instance_ids)

        return self.conn.terminate_instances(InstanceIds=instance_ids)

    @only_client_type
    def describe_instances(self, filters: list | None = None, instance_ids: list | None = None):
        """
        Describe EC2 instances, optionally applying filters and selective instance ids.

        :param filters: List of filters to specify instances to describe
        :param instance_ids: List of instance IDs to describe
        :return: Response from EC2 describe_instances API
        """
        filters = filters or []
        instance_ids = instance_ids or []

        self.log.info("Filters provided: %s", filters)
        self.log.info("Instance ids provided: %s", instance_ids)

        return self.conn.describe_instances(Filters=filters, InstanceIds=instance_ids)

    @only_client_type
    def get_instances(self, filters: list | None = None, instance_ids: list | None = None) -> list:
        """
        Get list of instance details, optionally applying filters and selective instance ids.

        :param instance_ids: List of ids to get instances for
        :param filters: List of filters to specify instances to get
        :return: List of instances
        """
        description = self.describe_instances(filters=filters, instance_ids=instance_ids)

        return [
            instance for reservation in description["Reservations"] for instance in reservation["Instances"]
        ]

    @only_client_type
    def get_instance_ids(self, filters: list | None = None) -> list:
        """
        Get list of instance ids, optionally applying filters to fetch selective instances.

        :param filters: List of filters to specify instances to get
        :return: List of instance ids
        """
        return [instance["InstanceId"] for instance in self.get_instances(filters=filters)]

    async def get_instance_state_async(self, instance_id: str) -> str:
        async with self.async_conn as client:
            response = await client.describe_instances(InstanceIds=[instance_id])
            return response["Reservations"][0]["Instances"][0]["State"]["Name"]

    def get_instance_state(self, instance_id: str) -> str:
        """
        Get EC2 instance state by id and return it.

        :param instance_id: id of the AWS EC2 instance
        :return: current state of the instance
        """
        if self._api_type == "client_type":
            return self.get_instances(instance_ids=[instance_id])[0]["State"]["Name"]

        return self.get_instance(instance_id=instance_id).state["Name"]

    def wait_for_state(self, instance_id: str, target_state: str, check_interval: float) -> None:
        """
        Wait EC2 instance until its state is equal to the target_state.

        :param instance_id: id of the AWS EC2 instance
        :param target_state: target state of instance
        :param check_interval: time in seconds that the job should wait in
            between each instance state checks until operation is completed
        :return: None
        """
        instance_state = self.get_instance_state(instance_id=instance_id)

        while instance_state != target_state:
            time.sleep(check_interval)
            instance_state = self.get_instance_state(instance_id=instance_id)

            self.log.info(
                "instance state: %s. Same as target: %s", instance_state, instance_state == target_state
            )

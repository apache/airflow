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

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EC2StartInstanceOperator(BaseOperator):
    """
    Start AWS EC2 instance using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2StartInstanceOperator`

    :param instance_id: id of the AWS EC2 instance
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: (optional) aws region name associated with the client
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
    """

    template_fields: Sequence[str] = ("instance_id", "region_name")
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        instance_id: str,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        check_interval: float = 15,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.check_interval = check_interval

    def execute(self, context: Context):
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        self.log.info("Starting EC2 instance %s", self.instance_id)
        instance = ec2_hook.get_instance(instance_id=self.instance_id)
        instance.start()
        ec2_hook.wait_for_state(
            instance_id=self.instance_id,
            target_state="running",
            check_interval=self.check_interval,
        )


class EC2StopInstanceOperator(BaseOperator):
    """
    Stop AWS EC2 instance using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2StopInstanceOperator`

    :param instance_id: id of the AWS EC2 instance
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: (optional) aws region name associated with the client
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
    """

    template_fields: Sequence[str] = ("instance_id", "region_name")
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        instance_id: str,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        check_interval: float = 15,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.check_interval = check_interval

    def execute(self, context: Context):
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        self.log.info("Stopping EC2 instance %s", self.instance_id)
        instance = ec2_hook.get_instance(instance_id=self.instance_id)
        instance.stop()
        ec2_hook.wait_for_state(
            instance_id=self.instance_id,
            target_state="stopped",
            check_interval=self.check_interval,
        )


class EC2CreateInstanceOperator(BaseOperator):
    """
    Create and start a specified number of EC2 Instances using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2CreateInstanceOperator`

    :param image_id: ID of the AMI used to create the instance.
    :param max_count: Maximum number of instances to launch. Defaults to 1.
    :param min_count: Minimum number of instances to launch. Defaults to 1.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region name associated with the client.
    :param poll_interval: Number of seconds to wait before attempting to
        check state of instance. Only used if wait_for_completion is True. Default is 20.
    :param max_attempts: Maximum number of attempts when checking state of instance.
        Only used if wait_for_completion is True. Default is 20.
    :param config: Dictionary for arbitrary parameters to the boto3 run_instances call.
    :param wait_for_completion: If True, the operator will wait for the instance to be
        in the `running` state before returning.
    """

    template_fields: Sequence[str] = (
        "image_id",
        "max_count",
        "min_count",
        "aws_conn_id",
        "region_name",
        "config",
        "wait_for_completion",
    )

    def __init__(
        self,
        image_id: str,
        max_count: int = 1,
        min_count: int = 1,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        poll_interval: int = 20,
        max_attempts: int = 20,
        config: dict | None = None,
        wait_for_completion: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image_id = image_id
        self.max_count = max_count
        self.min_count = min_count
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.config = config or {}
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name, api_type="client_type")
        instances = ec2_hook.conn.run_instances(
            ImageId=self.image_id,
            MinCount=self.min_count,
            MaxCount=self.max_count,
            **self.config,
        )["Instances"]

        instance_ids = self._on_kill_instance_ids = [instance["InstanceId"] for instance in instances]
        for instance_id in instance_ids:
            self.log.info("Created EC2 instance %s", instance_id)

            if self.wait_for_completion:
                ec2_hook.get_waiter("instance_running").wait(
                    InstanceIds=[instance_id],
                    WaiterConfig={
                        "Delay": self.poll_interval,
                        "MaxAttempts": self.max_attempts,
                    },
                )

        # leave "_on_kill_instance_ids" in place for finishing post-processing
        return instance_ids

    def on_kill(self) -> None:
        instance_ids = getattr(self, "_on_kill_instance_ids", [])

        if instance_ids:
            self.log.info("on_kill: Terminating instance/s %s", ", ".join(instance_ids))
            ec2_hook = EC2Hook(
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                api_type="client_type",
            )
            ec2_hook.conn.terminate_instances(InstanceIds=instance_ids)
        super().on_kill()


class EC2TerminateInstanceOperator(BaseOperator):
    """
    Terminate EC2 Instances using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2TerminateInstanceOperator`

    :param instance_id: ID of the instance to be terminated.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region name associated with the client.
    :param poll_interval: Number of seconds to wait before attempting to
        check state of instance. Only used if wait_for_completion is True. Default is 20.
    :param max_attempts: Maximum number of attempts when checking state of instance.
        Only used if wait_for_completion is True. Default is 20.
    :param wait_for_completion: If True, the operator will wait for the instance to be
        in the `terminated` state before returning.
    """

    template_fields: Sequence[str] = ("instance_ids", "region_name", "aws_conn_id", "wait_for_completion")

    def __init__(
        self,
        instance_ids: str | list[str],
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        poll_interval: int = 20,
        max_attempts: int = 20,
        wait_for_completion: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_ids = instance_ids
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        if isinstance(self.instance_ids, str):
            self.instance_ids = [self.instance_ids]
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name, api_type="client_type")
        ec2_hook.conn.terminate_instances(InstanceIds=self.instance_ids)

        for instance_id in self.instance_ids:
            self.log.info("Terminating EC2 instance %s", instance_id)
            if self.wait_for_completion:
                ec2_hook.get_waiter("instance_terminated").wait(
                    InstanceIds=[instance_id],
                    WaiterConfig={
                        "Delay": self.poll_interval,
                        "MaxAttempts": self.max_attempts,
                    },
                )


class EC2RebootInstanceOperator(BaseOperator):
    """
    Reboot Amazon EC2 instances.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2RebootInstanceOperator`

    :param instance_ids: ID of the instance(s) to be rebooted.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region name associated with the client.
    :param poll_interval: Number of seconds to wait before attempting to
        check state of instance. Only used if wait_for_completion is True. Default is 20.
    :param max_attempts: Maximum number of attempts when checking state of instance.
        Only used if wait_for_completion is True. Default is 20.
    :param wait_for_completion: If True, the operator will wait for the instance to be
        in the `running` state before returning.
    """

    template_fields: Sequence[str] = ("instance_ids", "region_name")
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        instance_ids: str | list[str],
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        poll_interval: int = 20,
        max_attempts: int = 20,
        wait_for_completion: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_ids = instance_ids
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        if isinstance(self.instance_ids, str):
            self.instance_ids = [self.instance_ids]
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name, api_type="client_type")
        self.log.info("Rebooting EC2 instances %s", ", ".join(self.instance_ids))
        ec2_hook.conn.reboot_instances(InstanceIds=self.instance_ids)

        if self.wait_for_completion:
            ec2_hook.get_waiter("instance_running").wait(
                InstanceIds=self.instance_ids,
                WaiterConfig={
                    "Delay": self.poll_interval,
                    "MaxAttempts": self.max_attempts,
                },
            )


class EC2HibernateInstanceOperator(BaseOperator):
    """
    Hibernate Amazon EC2 instances.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2HibernateInstanceOperator`

    :param instance_ids: ID of the instance(s) to be hibernated.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region name associated with the client.
    :param poll_interval: Number of seconds to wait before attempting to
        check state of instance. Only used if wait_for_completion is True. Default is 20.
    :param max_attempts: Maximum number of attempts when checking state of instance.
        Only used if wait_for_completion is True. Default is 20.
    :param wait_for_completion: If True, the operator will wait for the instance to be
        in the `stopped` state before returning.
    """

    template_fields: Sequence[str] = ("instance_ids", "region_name")
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        instance_ids: str | list[str],
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        poll_interval: int = 20,
        max_attempts: int = 20,
        wait_for_completion: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_ids = instance_ids
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        if isinstance(self.instance_ids, str):
            self.instance_ids = [self.instance_ids]
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name, api_type="client_type")
        self.log.info("Hibernating EC2 instances %s", ", ".join(self.instance_ids))
        instances = ec2_hook.get_instances(instance_ids=self.instance_ids)

        for instance in instances:
            hibernation_options = instance.get("HibernationOptions")
            if not hibernation_options or not hibernation_options["Configured"]:
                raise AirflowException(f"Instance {instance['InstanceId']} is not configured for hibernation")

        ec2_hook.conn.stop_instances(InstanceIds=self.instance_ids, Hibernate=True)

        if self.wait_for_completion:
            ec2_hook.get_waiter("instance_stopped").wait(
                InstanceIds=self.instance_ids,
                WaiterConfig={
                    "Delay": self.poll_interval,
                    "MaxAttempts": self.max_attempts,
                },
            )

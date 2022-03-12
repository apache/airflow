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
#

from typing import TYPE_CHECKING, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EC2StartInstanceOperator(BaseOperator):
    """
    Start AWS EC2 instance using boto3.

    :param instance_id: id of the AWS EC2 instance
    :param aws_conn_id: aws connection to use
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
        aws_conn_id: str = "aws_default",
        region_name: Optional[str] = None,
        check_interval: float = 15,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.check_interval = check_interval

    def execute(self, context: 'Context'):
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

    :param instance_id: id of the AWS EC2 instance
    :param aws_conn_id: aws connection to use
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
        aws_conn_id: str = "aws_default",
        region_name: Optional[str] = None,
        check_interval: float = 15,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.check_interval = check_interval

    def execute(self, context: 'Context'):
        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        self.log.info("Stopping EC2 instance %s", self.instance_id)
        instance = ec2_hook.get_instance(instance_id=self.instance_id)
        instance.stop()
        ec2_hook.wait_for_state(
            instance_id=self.instance_id,
            target_state="stopped",
            check_interval=self.check_interval,
        )

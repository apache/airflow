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

import time
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.utils.decorators import apply_defaults


class EC2Operator(BaseOperator):
    """
    Manage AWS EC2 instance using boto3.
    Change instance state by applying given operation.

    :param operation: action to be taken on AWS EC2 instance
        valid values: "start", "stop"
    :type operation: str
    :param instance_id: id of the AWS EC2 instance
    :type instance_id: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    :param region_name: (optional) aws region name associated with the client
    :type region_name: Optional[str]
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
    :type check_interval: float
    """

    template_fields = ["operation", "region_name"]
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"
    valid_operations = ["start", "stop"]
    operation_target_state_map = {
        "start": "running",
        "stop": "stopped",
    }

    @apply_defaults
    def __init__(self,
                 operation: str,
                 instance_id: str,
                 aws_conn_id: str = "aws_default",
                 region_name: Optional[str] = None,
                 check_interval: float = 15,
                 *args,
                 **kwargs):
        if operation not in self.valid_operations:
            raise AirflowException(f"Invalid operation: {operation}")
        super().__init__(*args, **kwargs)
        self.operation = operation
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.check_interval = check_interval
        self.target_state = self.operation_target_state_map[self.operation]
        self.hook = self.get_hook()

    def execute(self, context):
        self.log.info("Executing: %s %s", self.operation, self.instance_id)

        instance = self.hook.get_conn().Instance(id=self.instance_id)

        if self.operation == "start":
            instance.start()
        elif self.operation == "stop":
            instance.stop()

        while instance.state["Name"] != self.target_state:
            self.log.info("instance state: %s", instance.state)
            time.sleep(self.check_interval)
            instance = self.hook.get_conn().Instance(id=self.instance_id)

    def get_hook(self):
        """
        Return EC2Hook object.

        :return: ec2 hook
        :rtype: EC2Hook
        """
        return EC2Hook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )

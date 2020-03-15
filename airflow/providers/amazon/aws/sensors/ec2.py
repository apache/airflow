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

from typing import Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class EC2Sensor(BaseSensorOperator):
    """
    Check the state of the AWS EC2 instance until
    state of the instance become equal to the target state.

    :param target_state: target state of instances
    :type target_state: str
    :param instance_id: id of the AWS EC2 Instances
    :type instance_id: str
    :param region_name: (optional) aws region name associated with the client
    :type region_name: Optional[str]
    """

    template_fields = ["target_state", "region_name"]
    ui_color = "#cc8811"
    ui_fgcolor = "#ffffff"
    valid_states = ["running", "stopped"]

    @apply_defaults
    def __init__(self,
                 target_state: str,
                 instance_id: str,
                 aws_conn_id: str = "aws_default",
                 region_name: Optional[str] = None,
                 *args,
                 **kwargs):
        if target_state not in self.valid_states:
            raise AirflowException(f"Invalid target_state: {target_state}")
        super().__init__(*args, **kwargs)
        self.target_state = target_state
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.hook = self.get_hook()

    def poke(self, context):
        instance = self.hook.get_conn().Instance(id=self.instance_id)
        self.log.info("instance state: %s", instance.state)
        return instance.state["Name"] == self.target_state

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

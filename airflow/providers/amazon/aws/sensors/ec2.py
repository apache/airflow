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

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EC2InstanceStateSensor(BaseSensorOperator):
    """
    Check the state of the AWS EC2 instance until
    state of the instance become equal to the target state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EC2InstanceStateSensor`

    :param target_state: target state of instance
    :param instance_id: id of the AWS EC2 instance
    :param region_name: (optional) aws region name associated with the client
    """

    template_fields: Sequence[str] = ("target_state", "instance_id", "region_name")
    ui_color = "#cc8811"
    ui_fgcolor = "#ffffff"
    valid_states = ["running", "stopped", "terminated"]

    def __init__(
        self,
        *,
        target_state: str,
        instance_id: str,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ):
        if target_state not in self.valid_states:
            raise ValueError(f"Invalid target_state: {target_state}")
        super().__init__(**kwargs)
        self.target_state = target_state
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    @cached_property
    def hook(self):
        return EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def poke(self, context: Context):
        instance_state = self.hook.get_instance_state(instance_id=self.instance_id)
        self.log.info("instance state: %s", instance_state)
        return instance_state == self.target_state

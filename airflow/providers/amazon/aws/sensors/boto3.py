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

from typing import TYPE_CHECKING, Callable

from airflow.providers.amazon.aws.operators.boto3 import Boto3BaseOperator
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class Boto3Sensor(BaseSensorOperator, Boto3BaseOperator):
    """Boto3Sensor can wait for some AWS resource parameter to meet some criteria

    Example:
        Boto3Sensor(
            task_id="wait_until_rds_instance_available",
            aws_conn_id=AWS_CONNECTION_ID,
            client_type="rds",
            client_method="describe_db_instances",
            method_kwargs={"DBInstanceIdentifier": db_instance_identifier},
            poke_handler=lambda x: x["DBInstances"][0]["DBInstanceStatus"] == "available",
            mode="reschedule",
            poke_interval=120,
        )
    """

    def __init__(self, poke_handler: Callable, **kwargs) -> None:
        """
        Args:
            poke_handler (Callable): python function that accepts boto3 call result and returns True if criteria met, False otherwise.
        """
        self.poke_handler = poke_handler
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        result = self.boto3_action(**self.method_kwargs)
        return self.poke_handler(result)

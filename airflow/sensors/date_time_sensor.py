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

import datetime
from typing import Dict, Union

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class DateTimeSensor(BaseSensorOperator):
    """
    Waits until the specified datetime.

    :param target_time: datetime after which the job succeeds. (templated)
    :type target_time: str or datetime.datetime
    """

    template_fields = ("target_time",)

    @apply_defaults
    def __init__(
        self, target_time: Union[str, datetime.datetime], *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        if isinstance(target_time, datetime.datetime):
            self.target_time = target_time.isoformat()
        elif isinstance(target_time, str):
            self.target_time = target_time
        else:
            raise TypeError(
                "Expected str or datetime.datetime type for target_time. Got {}".format(
                    type(target_time)
                )
            )

    def poke(self, context: Dict) -> bool:
        self.log.info("Checking if the time (%s) has come", self.target_time)
        return timezone.utcnow() > timezone.parse(self.target_time)

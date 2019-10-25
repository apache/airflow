# -*- coding: utf-8 -*-
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
"""This module is deprecated. Please use `airflow.sensors.base_sensor`."""

import warnings
from typing import Iterable

# pylint: disable=unused-import
from airflow.sensors.base_sensor import BaseSensor
from airflow.utils.decorators import apply_defaults

warnings.warn(
    "This module is deprecated. Please use `airflow.models.BaseSensor`.",
    DeprecationWarning, stacklevel=2)


class BaseSensorOperator(BaseSensor):
    ui_color = '#e6f1f2'  # type: str
    valid_modes = ['poke', 'reschedule']  # type: Iterable[str]

    @apply_defaults
    def __init__(self,
                 poke_interval: float = 60,
                 timeout: float = 60 * 60 * 24 * 7,
                 soft_fail: bool = False,
                 mode: str = 'poke',
                 *args,
                 **kwargs) -> None:
        warnings.warn(
            "This class is deprecated. Please use `airflow.models.BaseSensor`.",
            DeprecationWarning, stacklevel=2)
        super().__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.timeout = timeout
        self.mode = mode
        self._validate_input_values()

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
# fmt: off
"""
Sensors.

:sphinx-autoapi-skip:
"""

from __future__ import annotations

from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    "base": {
        "BaseSensorOperator": "airflow.sdk.bases.sensor.BaseSensorOperator",
        "PokeReturnValue": "airflow.sdk.bases.sensor.PokeReturnValue",
        "poke_mode_only": "airflow.sdk.bases.sensor.poke_mode_only",
    },
    "python": {
        "PythonSensor": "airflow.providers.standard.sensors.python.PythonSensor",
    },
    "bash": {
        "BashSensor": "airflow.providers.standard.sensors.bash.BashSensor",
    },
    "date_time": {
        "DateTimeSensor": "airflow.providers.standard.sensors.date_time.DateTimeSensor",
        "DateTimeSensorAsync": "airflow.providers.standard.sensors.date_time.DateTimeSensorAsync",
    },
    "time_sensor": {
        "TimeSensor": "airflow.providers.standard.sensors.time.TimeSensor",
        "TimeSensorAsync": "airflow.providers.standard.sensors.time.TimeSensorAsync",
    },
    "weekday": {
        "DayOfWeekSensor": "airflow.providers.standard.sensors.weekday.DayOfWeekSensor",
    },
    "time_delta": {
        "TimeDeltaSensor": "airflow.providers.standard.sensors.time_delta.TimeDeltaSensor",
        "TimeDeltaSensorAsync": "airflow.providers.standard.sensors.time_delta.TimeDeltaSensorAsync",
    },
    "filesystem": {
        "FileSensor": "airflow.providers.standard.sensors.filesystem.FileSensor",
    },
    "external_task": {
        "ExternalTaskSensor": "airflow.providers.standard.sensors.external_task.ExternalTaskSensor",
        "ExternalTaskMarker": "airflow.providers.standard.sensors.external_task.ExternalTaskMarker",
        "ExternalDagLink": "airflow.providers.standard.sensors.external_task.ExternalDagLink"
    }
}
add_deprecated_classes(__deprecated_classes, __name__)

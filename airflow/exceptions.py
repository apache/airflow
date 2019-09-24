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
#
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Exceptions used by Airflow"""


class AirflowException(Exception):
    """
    Base class for all Airflow's errors.
    Each custom exception should be derived from this class
    """
    status_code = 500


class AirflowBadRequest(AirflowException):
    """Raise when the application or server cannot handle the request"""
    status_code = 400


class AirflowNotFoundException(AirflowException):
    """Raise when the requested object/resource is not available in the system"""
    status_code = 404


class AirflowConfigException(AirflowException):
    """Raise when there is configuration problem"""


class AirflowSensorTimeout(AirflowException):
    """Raise when there is a timeout on sensor polling"""


class AirflowRescheduleException(AirflowException):
    """
    Raise when the task should be re-scheduled at a later time.

    :param reschedule_date: The date when the task should be rescheduled
    :type reschedule_date: datetime.datetime
    """
    def __init__(self, reschedule_date):
        self.reschedule_date = reschedule_date


class InvalidStatsNameException(AirflowException):
    """Raise when name of the stats is invalid"""


class AirflowTaskTimeout(AirflowException):
    """Raise when the task execution times-out"""


class AirflowWebServerTimeout(AirflowException):
    """Raise when the web server times out"""


class AirflowSkipException(AirflowException):
    """Raise when the task should be skipped"""


class AirflowDagCycleException(AirflowException):
    """Raise when there is a cycle in Dag definition"""


class DagNotFound(AirflowNotFoundException):
    """Raise when a DAG is not available in the system"""


class DagRunNotFound(AirflowNotFoundException):
    """Raise when a DAG Run is not available in the system"""


class DagRunAlreadyExists(AirflowBadRequest):
    """Raise when creating a DAG run for DAG which already has DAG run entry"""


class DagFileExists(AirflowBadRequest):
    """Raise when a DAG ID is still in DagBag i.e., DAG file is in DAG folder"""


class TaskNotFound(AirflowNotFoundException):
    """Raise when a Task is not available in the system"""


class TaskInstanceNotFound(AirflowNotFoundException):
    """Raise when a Task Instance is not available in the system"""


class PoolNotFound(AirflowNotFoundException):
    """Raise when a Pool is not available in the system"""


class NoAvailablePoolSlot(AirflowException):
    """Raise when there is not enough slots in pool"""


class DagConcurrencyLimitReached(AirflowException):
    """Raise when DAG concurrency limit is reached"""


class TaskConcurrencyLimitReached(AirflowException):
    """Raise when task concurrency limit is reached"""

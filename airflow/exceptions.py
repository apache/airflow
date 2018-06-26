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

from __future__ import absolute_import
from __future__ import unicode_literals

from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


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
    pass


class AirflowSensorTimeout(AirflowException):
    pass


class AirflowTaskTimeout(AirflowException):
    pass


class AirflowWebServerTimeout(AirflowException):
    pass


class AirflowSkipException(AirflowException):
    pass


class AirflowDagCycleException(AirflowException):
    pass


class DagNotFound(AirflowNotFoundException):
    """Raise when a DAG is not available in the system"""
    pass


class DagRunNotFound(AirflowNotFoundException):
    """Raise when a DAG Run is not available in the system"""
    pass


class DagRunAlreadyExists(AirflowBadRequest):
    """Raise when creating a DAG run for DAG which already has DAG run entry"""
    pass


class DagFileExists(AirflowBadRequest):
    """Raise when a DAG ID is still in DagBag i.e., DAG file is in DAG folder"""
    pass


class TaskNotFound(AirflowNotFoundException):
    """Raise when a Task is not available in the system"""
    pass


class TaskInstanceNotFound(AirflowNotFoundException):
    """Raise when a Task Instance is not available in the system"""
    pass


class PoolNotFound(AirflowNotFoundException):
    """Raise when a Pool is not available in the system"""
    pass


class AirflowFaultException(AirflowException):
    """
    Exception class with more information needed
    to determine the type of fault that may be retryable.
    """

    def __init__(self, instance, orig_exception=None, return_code=None,
                 std_out=None, std_err=None, **kwargs):
        """
        Initialize an instance of this class.
        :param instance: Reference to the instance that raised this Exception
        :param orig_exception: original exception
        :param return_code: Return code as int
        :param std_out: Std out as string
        :param std_err: Std err as string
        :param kwargs: Dictionary containing additional parameters
        """
        super(AirflowFaultException, self).__init__(std_out)

        # Reference to the object (typically a Hook)
        # that raised this exception.
        self.instance = instance
        self.orig_exception = orig_exception
        self.return_code = return_code
        self.std_out = std_out
        self.std_err = std_err
        self.kwargs = kwargs

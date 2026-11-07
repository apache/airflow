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

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.log import cloud_logging_task_handler, stackdriver_task_handler


@pytest.mark.parametrize(
    ("old_class", "new_class", "kwargs"),
    [
        (
            stackdriver_task_handler.StackdriverRemoteLogIO,
            cloud_logging_task_handler.CloudLoggingRemoteLogIO,
            {"base_log_folder": "/tmp/x"},
        ),
        (
            stackdriver_task_handler.StackdriverTaskHandler,
            cloud_logging_task_handler.CloudLoggingTaskHandler,
            {},
        ),
    ],
)
def test_deprecated_class_warns_and_subclasses_new_class(old_class, new_class, kwargs):
    with pytest.warns(AirflowProviderDeprecationWarning, match="CloudLogging"):
        obj = old_class(**kwargs)
    assert isinstance(obj, new_class)


@pytest.mark.parametrize(
    ("old_constant", "new_constant"),
    [
        (stackdriver_task_handler.DEFAULT_LOGGER_NAME, cloud_logging_task_handler.DEFAULT_LOGGER_NAME),
        (stackdriver_task_handler.LABEL_DAG_ID, cloud_logging_task_handler.LABEL_DAG_ID),
        (stackdriver_task_handler.LABEL_LOGICAL_DATE, cloud_logging_task_handler.LABEL_LOGICAL_DATE),
        (stackdriver_task_handler.LABEL_TASK_ID, cloud_logging_task_handler.LABEL_TASK_ID),
        (stackdriver_task_handler.LABEL_TRY_NUMBER, cloud_logging_task_handler.LABEL_TRY_NUMBER),
    ],
)
def test_deprecated_constants_are_importable(old_constant, new_constant):
    assert old_constant == new_constant

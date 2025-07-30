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

from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    "setup_teardown": {
        "BaseSetupTeardownContext": "airflow.sdk.definitions._internal.setup_teardown.BaseSetupTeardownContext",
        "SetupTeardownContext": "airflow.sdk.definitions._internal.setup_teardown.SetupTeardownContext",
    },
    "xcom": {
        "XCOM_RETURN_KEY": "airflow.models.xcom.XCOM_RETURN_KEY",
    },
    "task_group": {
        "TaskGroup": "airflow.sdk.TaskGroup",
        "get_task_group_children_getter": "airflow.sdk.definitions.taskgroup.get_task_group_children_getter",
        "task_group_to_dict": "airflow.sdk.definitions.taskgroup.task_group_to_dict",
    },
    "timezone": {
        # Since we have corrected all uses inside core to use the internal version, anything hitting this
        # should be in user code or custom providers, so redirect them to the public interface in Task SDK
        "*": "airflow.sdk.timezone"
    },
    "decorators": {
        "remove_task_decorator": "airflow.sdk.definitions._internal.decorators.remove_task_decorator",
        "fixup_decorator_warning_stack": "airflow.sdk.definitions._internal.decorators.fixup_decorator_warning_stack",
    },
}

add_deprecated_classes(__deprecated_classes, __name__)

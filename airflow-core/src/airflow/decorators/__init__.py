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
    __name__: {
        "dag": "airflow.sdk.dag",
        "setup": "airflow.sdk.setup",
        "task": "airflow.sdk.task",
        "task_group": "airflow.sdk.task_group",
        "teardown": "airflow.sdk.teardown",
    },
    "base": {
        "DecoratedMappedOperator": "airflow.sdk.bases.decorator.DecoratedMappedOperator",
        "DecoratedOperator": "airflow.sdk.bases.decorator.DecoratedOperator",
        "TaskDecorator": "airflow.sdk.bases.decorator.TaskDecorator",
        "TaskDecoratorCollection": "airflow.sdk.definitions.decorators.TaskDecoratorCollection",
        "get_unique_task_id": "airflow.sdk.bases.decorator.get_unique_task_id",
        "task_decorator_factory": "airflow.sdk.bases.decorator.task_decorator_factory",
    },
}
add_deprecated_classes(__deprecated_classes, __name__)

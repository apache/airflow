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

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

ADOPTED = "adopted"
if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.state import TaskInstanceState

    # TaskInstance key, command, configuration, pod_template_file
    KubernetesJobType = Tuple[TaskInstanceKey, CommandType, Any, Optional[str]]

    # key, pod state, pod_name, namespace, resource_version
    KubernetesResultsType = Tuple[TaskInstanceKey, Optional[Union[TaskInstanceState, str]], str, str, str]

    # pod_name, namespace, pod state, annotations, resource_version
    KubernetesWatchType = Tuple[str, str, Optional[Union[TaskInstanceState, str]], Dict[str, str], str]

ALL_NAMESPACES = "ALL_NAMESPACES"
POD_EXECUTOR_DONE_KEY = "airflow_executor_done"

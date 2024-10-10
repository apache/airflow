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

from enum import Enum, unique


@unique
class ConnectorSource(Enum):
    """Enum of supported executor import sources."""

    CORE = "core"
    PLUGIN = "plugin"
    CUSTOM_PATH = "custom path"


LOCAL_EXECUTOR = "LocalExecutor"
LOCAL_KUBERNETES_EXECUTOR = "LocalKubernetesExecutor"
SEQUENTIAL_EXECUTOR = "SequentialExecutor"
CELERY_EXECUTOR = "CeleryExecutor"
CELERY_KUBERNETES_EXECUTOR = "CeleryKubernetesExecutor"
KUBERNETES_EXECUTOR = "KubernetesExecutor"
EDGE_EXECUTOR = "EdgeExecutor"
DEBUG_EXECUTOR = "DebugExecutor"
MOCK_EXECUTOR = "MockExecutor"
CORE_EXECUTOR_NAMES = {
    LOCAL_EXECUTOR,
    LOCAL_KUBERNETES_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
    CELERY_EXECUTOR,
    CELERY_KUBERNETES_EXECUTOR,
    KUBERNETES_EXECUTOR,
    EDGE_EXECUTOR,
    DEBUG_EXECUTOR,
    MOCK_EXECUTOR,
}

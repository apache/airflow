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
"""Executors."""
from __future__ import annotations

from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    "celery_executor": {
        "app": "airflow.providers.celery.executors.celery_executor_utils.app",
        "CeleryExecutor": "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
    },
    "celery_kubernetes_executor": {
        "CeleryKubernetesExecutor": "airflow.providers.celery.executors."
        "celery_kubernetes_executor.CeleryKubernetesExecutor",
    },
    "dask_executor": {
        "DaskExecutor": "airflow.providers.daskexecutor.executors.dask_executor.DaskExecutor",
    },
}

add_deprecated_classes(__deprecated_classes, __name__)

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
    "kubernetes_executor": {
        "KubernetesExecutor": "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor.KubernetesExecutor",
    },
    "kubernetes_executor_types": {
        "ALL_NAMESPACES": "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor_types.ALL_NAMESPACES",
        "POD_EXECUTOR_DONE_KEY": "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor_types.POD_EXECUTOR_DONE_KEY",
    },
    "kubernetes_executor_utils": {
        "AirflowKubernetesScheduler": "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor_utils.AirflowKubernetesScheduler",
        "KubernetesJobWatcher": "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor_utils.KubernetesJobWatcher",
        "ResourceVersion": "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor_utils.ResourceVersion",
    },
    "local_kubernetes_executor": {
        "LocalKubernetesExecutor": "airflow.providers.cncf.kubernetes.executors.LocalKubernetesExecutor",
    },
}

add_deprecated_classes(
    __deprecated_classes,
    __name__,
    {},
    "For Celery executors, the `celery` provider should be >= 3.3.0. "
    "For Kubernetes executors, the `cncf.kubernetes` provider should be >= 7.4.0 for that. "
    "For Dask executors, any version of `daskexecutor` provider is needed.",
)

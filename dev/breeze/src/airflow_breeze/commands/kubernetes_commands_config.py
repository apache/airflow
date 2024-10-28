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

KUBERNETES_CLUSTER_COMMANDS: dict[str, str | list[str]] = {
    "name": "K8S cluster management commands",
    "commands": [
        "setup-env",
        "create-cluster",
        "configure-cluster",
        "build-k8s-image",
        "upload-k8s-image",
        "deploy-airflow",
        "delete-cluster",
    ],
}
KUBERNETES_INSPECTION_COMMANDS: dict[str, str | list[str]] = {
    "name": "K8S inspection commands",
    "commands": ["status", "logs"],
}

KUBERNETES_TESTING_COMMANDS: dict[str, str | list[str]] = {
    "name": "K8S testing commands",
    "commands": ["tests", "run-complete-tests", "shell", "k9s", "logs"],
}
KUBERNETES_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze k8s setup-env": [
        {
            "name": "K8S setup flags",
            "options": ["--force-venv-setup"],
        }
    ],
    "breeze k8s create-cluster": [
        {
            "name": "K8S cluster creation flags",
            "options": [
                "--python",
                "--kubernetes-version",
                "--force-recreate-cluster",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--kubernetes-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s build-k8s-image": [
        {
            "name": "Build image flags",
            "options": [
                "--python",
                "--image-tag",
                "--rebuild-base-image",
                "--copy-local-sources",
                "--use-uv",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s configure-cluster": [
        {
            "name": "Configure cluster flags",
            "options": [
                "--python",
                "--kubernetes-version",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--kubernetes-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s upload-k8s-image": [
        {
            "name": "Upload image flags",
            "options": [
                "--python",
                "--kubernetes-version",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--kubernetes-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s configure-k8s-cluster": [
        {
            "name": "Configure cluster flags",
            "options": [
                "--python",
                "--kubernetes-version",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--kubernetes-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s deploy-airflow": [
        {
            "name": "Airflow deploy flags",
            "options": [
                "--python",
                "--kubernetes-version",
                "--executor",
                "--upgrade",
                "--wait-time-in-seconds",
                "--use-standard-naming",
                "--multi-namespace-mode",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--kubernetes-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s delete-cluster": [
        {
            "name": "K8S cluster delete flags",
            "options": ["--python", "--kubernetes-version", "--all"],
        },
    ],
    "breeze k8s status": [
        {
            "name": "K8S cluster status flags",
            "options": [
                "--python",
                "--kubernetes-version",
                "--wait-time-in-seconds",
                "--all",
            ],
        },
    ],
    "breeze k8s logs": [
        {
            "name": "K8S logs flags",
            "options": ["--python", "--kubernetes-version", "--all"],
        },
    ],
    "breeze k8s tests": [
        {
            "name": "K8S tests flags",
            "options": [
                "--python",
                "--kubernetes-version",
                "--executor",
                "--force-venv-setup",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--kubernetes-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s run-complete-tests": [
        {
            "name": "K8S cluster creation flags",
            "options": [
                "--force-recreate-cluster",
            ],
        },
        {
            "name": "Airflow deploy flags",
            "options": [
                "--upgrade",
                "--wait-time-in-seconds",
                "--use-standard-naming",
            ],
        },
        {
            "name": "Build image flags",
            "options": [
                "--image-tag",
                "--rebuild-base-image",
                "--copy-local-sources",
                "--use-uv",
            ],
        },
        {
            "name": "K8S tests flags",
            "options": [
                "--python",
                "--kubernetes-version",
                "--executor",
                "--force-venv-setup",
            ],
        },
        {
            "name": "Parallel options",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--kubernetes-versions",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze k8s k9s": [
        {
            "name": "K8S k9s flags",
            "options": [
                "--use-docker",
                "--python",
                "--kubernetes-version",
            ],
        }
    ],
    "breeze k8s shell": [
        {
            "name": "K8S shell flags",
            "options": [
                "--python",
                "--kubernetes-version",
                "--executor",
                "--force-venv-setup",
            ],
        }
    ],
}

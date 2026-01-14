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

from kubernetes.client import models as k8s

from airflow.providers.common.compat.sdk import AirflowException


def convert_env_vars(env_vars) -> list[k8s.V1EnvVar]:
    """
    Convert a dictionary of key:value into a list of env_vars.

    :param env_vars:
    :return:
    """
    if isinstance(env_vars, dict):
        res = []
        for k, v in env_vars.items():
            res.append(k8s.V1EnvVar(name=k, value=v))
        return res
    if isinstance(env_vars, list):
        if all([isinstance(e, k8s.V1EnvVar) for e in env_vars]):
            return env_vars
    raise AirflowException(f"Expected dict or list of V1EnvVar, got {type(env_vars)}")

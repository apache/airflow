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

import inspect

import pytest

from airflow.providers.cncf.kubernetes.decorators.kubernetes import kubernetes_task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


def extract_init_param_names(cls):
    sig = inspect.signature(cls.__init__)
    return {
        name for name, param in sig.parameters.items() if name != "self" and param.kind != param.VAR_KEYWORD
    }


def extract_decorator_param_names(decorator_func):
    sig = inspect.signature(decorator_func)
    return {name for name, param in sig.parameters.items() if param.kind != param.VAR_KEYWORD}


def test_kubernetes_task_decorator_signature_matches_operator():
    operator_params = extract_init_param_names(KubernetesPodOperator)
    decorator_params = extract_decorator_param_names(kubernetes_task)

    # Ignore decorator-specific arguments
    ignored_params = {"python_callable", "multiple_outputs"}
    filtered_decorator_params = decorator_params - ignored_params

    # These operator params should be present in the decorator
    missing_params = operator_params - filtered_decorator_params

    if missing_params:
        pytest.fail(
            f"The following KubernetesPodOperator params are missing from the "
            f"kubernetes_task decorator: {sorted(missing_params)}"
        )

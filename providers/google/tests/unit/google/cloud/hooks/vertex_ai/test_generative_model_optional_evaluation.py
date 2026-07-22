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

import importlib
import sys
from collections.abc import Iterator
from types import ModuleType
from typing import cast
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

pytest.importorskip("google.cloud.aiplatform_v1")

EVALUATION_MODULE = "vertexai.preview.evaluation"
HOOK_MODULE = "airflow.providers.google.cloud.hooks.vertex_ai.generative_model"
BASE_HOOK_INIT = "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__"
INSTALL_EXTRA_REGEX = r"apache-airflow-providers-google\[evaluation\]"

_MISSING = object()


@pytest.fixture
def generative_model_hook_module_without_evaluation() -> Iterator[ModuleType]:
    modules = cast("dict[str, object]", sys.modules)
    original_evaluation_module = modules.get(EVALUATION_MODULE, _MISSING)
    original_hook_module = modules.get(HOOK_MODULE, _MISSING)

    modules[EVALUATION_MODULE] = None
    hook_module = importlib.import_module(HOOK_MODULE)
    hook_module = importlib.reload(hook_module)

    try:
        yield hook_module
    finally:
        if original_evaluation_module is _MISSING:
            modules.pop(EVALUATION_MODULE, None)
        else:
            modules[EVALUATION_MODULE] = original_evaluation_module

        if original_hook_module is _MISSING:
            modules.pop(HOOK_MODULE, None)
        else:
            importlib.reload(hook_module)


def test_get_eval_task_raises_optional_provider_feature_exception_without_evaluation_extra(
    generative_model_hook_module_without_evaluation: ModuleType,
):
    with mock.patch(BASE_HOOK_INIT, new=mock_base_gcp_hook_default_project_id):
        hook = generative_model_hook_module_without_evaluation.GenerativeModelHook()

    with pytest.raises(AirflowOptionalProviderFeatureException, match=INSTALL_EXTRA_REGEX):
        hook.get_eval_task(dataset={}, metrics=[], experiment="test-experiment")


def test_run_evaluation_raises_optional_provider_feature_exception_without_evaluation_extra(
    generative_model_hook_module_without_evaluation: ModuleType,
):
    with mock.patch(BASE_HOOK_INIT, new=mock_base_gcp_hook_default_project_id):
        hook = generative_model_hook_module_without_evaluation.GenerativeModelHook()

    with pytest.raises(AirflowOptionalProviderFeatureException, match=INSTALL_EXTRA_REGEX):
        hook.run_evaluation(
            project_id="test-project",
            location="us-central1",
            pretrained_model="gemini-pro",
            eval_dataset={},
            metrics=[],
            experiment_name="test-experiment",
            experiment_run_name="test-run",
            prompt_template="{prompt}",
        )

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

from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

pytest.importorskip("google.cloud.aiplatform_v1")

from airflow.providers.google.cloud.hooks.vertex_ai.generative_model import (
    GenerativeModelHook,
)

HOOK_MODULE = "airflow.providers.google.cloud.hooks.vertex_ai.generative_model"
BASE_HOOK_INIT = "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__"
INSTALL_EXTRA_REGEX = r"apache-airflow-providers-google\[evaluation\]"

# Patching the guard variable (instead of reloading the hook module with the evaluation import
# blocked) keeps the module and class objects stable for other test files in the same session.
MISSING_EVALUATION_ERRORS = (
    pytest.param(
        ImportError("No module named 'vertexai.preview.evaluation'"),
        id="missing-evaluation-module",
    ),
    pytest.param(
        ImportError("No module named 'sklearn'"),
        id="missing-sklearn",
    ),
)


@pytest.fixture
def hook() -> GenerativeModelHook:
    with mock.patch(BASE_HOOK_INIT, new=mock_base_gcp_hook_default_project_id):
        return GenerativeModelHook()


@pytest.mark.parametrize("import_error", MISSING_EVALUATION_ERRORS)
def test_get_eval_task_raises_optional_provider_feature_exception_without_evaluation_extra(
    hook: GenerativeModelHook,
    import_error: ImportError,
):
    with mock.patch(f"{HOOK_MODULE}._evaluation_import_error", import_error):
        with pytest.raises(AirflowOptionalProviderFeatureException, match=INSTALL_EXTRA_REGEX):
            hook.get_eval_task(dataset={}, metrics=[], experiment="test-experiment")


@pytest.mark.parametrize("import_error", MISSING_EVALUATION_ERRORS)
def test_run_evaluation_raises_optional_provider_feature_exception_without_evaluation_extra(
    hook: GenerativeModelHook,
    import_error: ImportError,
):
    with mock.patch(f"{HOOK_MODULE}._evaluation_import_error", import_error):
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

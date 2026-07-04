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

import builtins
import runpy
from types import SimpleNamespace
from unittest import mock

import pytest

# Only the base ``google-cloud-aiplatform`` package is required to import the hook.
pytest.importorskip("google.cloud.aiplatform_v1")

from airflow.providers.google.cloud.hooks.vertex_ai import generative_model

_REAL_IMPORT = builtins.__import__


def _import_except_vertex_evaluation(name, import_globals=None, import_locals=None, fromlist=(), level=0):
    if name.startswith("vertexai.preview.evaluation") or (
        name == "vertexai.preview" and "evaluation" in (fromlist or ())
    ):
        raise ImportError("No module named 'vertexai.preview.evaluation'")
    return _REAL_IMPORT(name, import_globals, import_locals, fromlist, level)


class TestVertexEvaluationImport:
    def test_hook_module_imports_without_evaluation_dependencies(self):
        with mock.patch("builtins.__import__", side_effect=_import_except_vertex_evaluation):
            runpy.run_path(generative_model.__file__)

    def test_missing_evaluation_dependencies_raises_actionable_error(self):
        with mock.patch("builtins.__import__", side_effect=_import_except_vertex_evaluation):
            with pytest.raises(ImportError, match=r"apache-airflow-providers-google\[vertex-eval\]"):
                generative_model._import_vertex_evaluation()

    @mock.patch.object(generative_model, "_import_vertex_evaluation", autospec=True)
    def test_get_eval_task_uses_lazy_evaluation_import(self, mock_import):
        def create_eval_task(*, dataset, metrics, experiment):
            return None

        eval_task_constructor = mock.create_autospec(
            create_eval_task,
            return_value=mock.sentinel.eval_task,
        )
        mock_import.return_value = SimpleNamespace(EvalTask=eval_task_constructor)
        hook = object.__new__(generative_model.GenerativeModelHook)

        result = hook.get_eval_task(
            dataset=mock.sentinel.dataset,
            metrics=mock.sentinel.metrics,
            experiment=mock.sentinel.experiment,
        )

        mock_import.assert_called_once_with()
        eval_task_constructor.assert_called_once_with(
            dataset=mock.sentinel.dataset,
            metrics=mock.sentinel.metrics,
            experiment=mock.sentinel.experiment,
        )
        assert result is mock.sentinel.eval_task

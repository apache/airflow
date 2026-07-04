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
from unittest import mock

import pytest

# Only the base ``google-cloud-aiplatform`` (``vertexai``) is required to import
# the hook module; the optional ``[evaluation]`` extra is deliberately absent so
# this test can prove the lazy-import guard's behavior.
pytest.importorskip("google.cloud.aiplatform_v1")

from airflow.providers.google.cloud.hooks.vertex_ai import generative_model


class TestVertexEvaluationImport:
    """Tests for the lazy-import helper that guards the optional evaluation stack.

    This file intentionally does not import ``vertexai.preview.evaluation`` at module
    level so it runs even in the lean environment where the ``vertex-eval`` extra is
    not installed -- which is exactly the situation the guard exists to handle.
    """

    def test_missing_evaluation_extra_raises_actionable_error(self):
        """Users without ``[vertex-eval]`` get a pip-install hint, not a bare ModuleNotFoundError."""
        real_import = builtins.__import__

        def _blocked_import(name, import_globals=None, import_locals=None, fromlist=(), level=0):
            # Simulate the ``[evaluation]`` extra being absent regardless of what is
            # actually installed, so the guard's behavior is asserted deterministically
            # (patching ``sys.modules`` is not reliable across vertexai versions).
            if name == "vertexai.preview" and "evaluation" in (fromlist or ()):
                raise ImportError("No module named 'vertexai.preview.evaluation'")
            return real_import(name, import_globals, import_locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=_blocked_import):
            with pytest.raises(ImportError, match=r"apache-airflow-providers-google\[vertex-eval\]"):
                generative_model._import_vertex_evaluation()

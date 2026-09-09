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

import sys
from unittest.mock import patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import get_delete_handlers, ray


def test_get_delete_handlers_does_not_require_vertex_ray():
    with patch.dict(sys.modules, {"vertex_ray": None}):
        handlers = get_delete_handlers()

    assert "vertex_ai_raycluster" in handlers


def test_get_vertex_ray_raises_helpful_error_when_dependency_is_missing():
    with patch.dict(sys.modules, {"vertex_ray": None}):
        with pytest.raises(ImportError, match="google-cloud-aiplatform\\[ray\\]"):
            ray._get_vertex_ray()

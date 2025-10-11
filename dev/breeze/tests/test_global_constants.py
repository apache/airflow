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

import pytest
import requests

from airflow_breeze.global_constants import ALLOWED_KUBERNETES_VERSIONS


class TestGlobalConstants:
    @pytest.mark.parametrize("k8s_version", ALLOWED_KUBERNETES_VERSIONS)
    def test_kubernetes_versions_available_on_docker_hub(self, k8s_version):
        """Test that all Kubernetes versions in ALLOWED_KUBERNETES_VERSIONS exist on Docker Hub."""
        url = f"https://registry.hub.docker.com/v2/repositories/kindest/node/tags/{k8s_version}/"
        response = requests.get(url, timeout=10)
        assert response.status_code == 200, f"Kubernetes version {k8s_version} not found on Docker Hub"

        data = response.json()
        assert data["name"] == k8s_version, f"Version mismatch for {k8s_version}"

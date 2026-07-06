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

from unittest.mock import patch

import pytest
import yaml

from airflow_breeze.utils.kubernetes_utils import get_kubernetes_port_numbers

SINGLE_NODE_CONFIG = """\
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  ipFamily: ipv4
  apiServerAddress: "127.0.0.1"
  apiServerPort: 48366
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30007
        hostPort: 18150
        listenAddress: "127.0.0.1"
        protocol: TCP
"""

# Rendered config of a cluster created before the switch to single-node clusters —
# port extraction must keep working for clusters that already exist on disk.
LEGACY_TWO_NODE_CONFIG = """\
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  ipFamily: ipv4
  apiServerAddress: "127.0.0.1"
  apiServerPort: 48366
nodes:
  - role: control-plane
  - role: worker
    extraPortMappings:
      - containerPort: 30007
        hostPort: 18150
        listenAddress: "127.0.0.1"
        protocol: TCP
"""

NO_FORWARDED_PORT_CONFIG = """\
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  ipFamily: ipv4
  apiServerAddress: "127.0.0.1"
  apiServerPort: 48366
nodes:
  - role: control-plane
"""


@pytest.mark.parametrize(
    "config", [SINGLE_NODE_CONFIG, LEGACY_TWO_NODE_CONFIG], ids=["single-node", "legacy-two-node"]
)
@patch("airflow_breeze.utils.kubernetes_utils._get_kind_cluster_config_content", autospec=True)
def test_get_kubernetes_port_numbers_extracts_ports_from_any_node(mock_get_content, config):
    mock_get_content.return_value = yaml.safe_load(config)
    assert get_kubernetes_port_numbers(python="3.10", kubernetes_version="v1.30.13") == (48366, 18150)


@patch("airflow_breeze.utils.kubernetes_utils._get_kind_cluster_config_content", autospec=True)
def test_get_kubernetes_port_numbers_raises_when_forwarded_port_mapping_is_missing(mock_get_content):
    mock_get_content.return_value = yaml.safe_load(NO_FORWARDED_PORT_CONFIG)
    with pytest.raises(ValueError, match="extraPortMappings"):
        get_kubernetes_port_numbers(python="3.10", kubernetes_version="v1.30.13")

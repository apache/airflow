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

from importlib.metadata import EntryPoint
from pathlib import Path

import yaml


def test_provider_entry_point_matches_provider_yaml():
    """The installed entry point metadata must match the source provider.yaml."""
    project_root = Path(__file__).parents[3]
    entry_point_value = "airflow.providers.huggingface.get_provider_info:get_provider_info"
    assert entry_point_value in (project_root / "pyproject.toml").read_text()

    entry_point = EntryPoint(name="provider_info", value=entry_point_value, group="apache_airflow_provider")
    provider_info = entry_point.load()()
    provider_yaml = yaml.safe_load((project_root / "provider.yaml").read_text())

    assert provider_info == provider_yaml

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
from ci.prek.check_provider_yaml_files import _resolve_provider_yaml_files


def _touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


class TestResolveProviderYamlFiles:
    @pytest.mark.parametrize(
        "file_names, raw_files, expected",
        (
            (
                ["samba/provider.yaml"],
                ["samba/provider.yaml"],
                ["samba/provider.yaml"],
            ),
            (
                [
                    "samba/provider.yaml",
                    "samba/src/airflow/providers/samba/hooks/samba.py",
                ],
                ["samba/src/airflow/providers/samba/hooks/samba.py"],
                ["samba/provider.yaml"],
            ),
            (
                [
                    "ibm/mq/provider.yaml",
                    "ibm/mq/src/airflow/providers/ibm/mq/hooks/mq.py",
                ],
                ["ibm/mq/src/airflow/providers/ibm/mq/hooks/mq.py"],
                ["ibm/mq/provider.yaml"],
            ),
            (
                [
                    "samba/provider.yaml",
                    "samba/src/airflow/providers/samba/hooks/samba.py",
                    "ibm/mq/provider.yaml",
                    "ibm/mq/src/airflow/providers/ibm/mq/hooks/mq.py",
                ],
                [
                    "samba/provider.yaml",
                    "samba/src/airflow/providers/samba/hooks/samba.py",
                    "ibm/mq/src/airflow/providers/ibm/mq/hooks/mq.py",
                ],
                ["ibm/mq/provider.yaml", "samba/provider.yaml"],
            ),
            (["unrelated/hooks/hook.py"], ["unrelated/hooks/hook.py"], []),
        ),
        ids=[
            "provider_yaml_path_preserved",
            "top_level_provider_hook_file_resolves",
            "nested_namespace_provider_hook_file_resolves_without_known_list",
            "mixed_input_dedups_and_sorts",
            "no_provider_found",
        ],
    )
    def test__resolve_provider_yaml_files(self, tmp_path, monkeypatch, file_names, raw_files, expected):
        monkeypatch.chdir(tmp_path)
        for file_name in file_names:
            _touch(tmp_path / file_name)

        result = _resolve_provider_yaml_files(raw_files)

        assert result == expected

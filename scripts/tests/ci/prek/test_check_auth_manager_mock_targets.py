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
from check_auth_manager_mock_targets import (
    find_bad_patch_targets,
    get_methods,
    get_overridden_methods,
    main,
)

BASE = "airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager"
SIMPLE = "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"


def test_get_methods_skips_private_methods(tmp_path):
    module = tmp_path / "module.py"
    module.write_text(
        "class Manager:\n    def public(self): ...\n    async def run(self): ...\n    def _private(self): ...\n"
    )

    assert get_methods(module, "Manager") == {"public", "run"}


def test_get_methods_raises_for_missing_class(tmp_path):
    module = tmp_path / "module.py"
    module.write_text("class Other: ...\n")

    with pytest.raises(ValueError, match="class Manager not found"):
        get_methods(module, "Manager")


def test_overridden_methods_follow_the_real_classes():
    overridden = get_overridden_methods()

    assert "get_authorized_assets" in overridden
    assert "get_authorized_dag_ids" not in overridden


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param(
            f'@mock.patch("{BASE}.get_authorized_assets")', [(1, "get_authorized_assets")], id="base"
        ),
        pytest.param(f'@mock.patch("{SIMPLE}.get_authorized_assets")', [], id="simple"),
        pytest.param(f'@mock.patch("{BASE}.get_authorized_dag_ids")', [], id="not-overridden"),
        pytest.param(
            f'x = 1\n@mock.patch(\n    "{BASE}.is_authorized_dag",\n)',
            [(3, "is_authorized_dag")],
            id="multiline",
        ),
    ],
)
def test_find_bad_patch_targets(text, expected):
    overridden = {"get_authorized_assets", "is_authorized_dag"}

    assert find_bad_patch_targets(text, overridden) == expected


def test_main_reports_bad_target_and_fails(tmp_path, capsys):
    test_file = tmp_path / "test_routes.py"
    test_file.write_text(f'@mock.patch("{BASE}.get_authorized_assets")\n')

    assert main([str(test_file)]) == 1
    assert "patch SimpleAuthManager.get_authorized_assets instead" in capsys.readouterr().out


def test_main_passes_good_target(tmp_path):
    test_file = tmp_path / "test_routes.py"
    test_file.write_text(f'@mock.patch("{SIMPLE}.get_authorized_assets")\n')

    assert main([str(test_file)]) == 0

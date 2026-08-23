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

from pathlib import Path

import pytest
from ci.prek.check_providers_subpackages_all_have_init import (
    _needs_path_extension_in_src,
    _what_kind_of_test_init_py_needed,
)


class TestPathExtensionDecisions:
    @pytest.mark.parametrize(
        ("folder", "namespace", "expected"),
        [
            pytest.param("unit/apache", "apache", True, id="namespace-folder-is-shared"),
            pytest.param("unit/acme", "acme", True, id="unknown-namespace-folder-is-shared"),
            pytest.param("unit/hive", "apache", False, id="provider-own-folder-is-not-shared"),
            pytest.param("unit/amazon", None, False, id="top-level-provider-has-no-shared-folder"),
        ],
    )
    def test_only_the_namespace_folder_needs_path_extension(self, tmp_path, folder, namespace, expected):
        need_init_py, need_path_extension = _what_kind_of_test_init_py_needed(
            tmp_path, tmp_path / folder, namespace
        )

        assert need_init_py is True
        assert need_path_extension is expected

    @pytest.mark.parametrize(
        ("relative_path", "namespace", "expected"),
        [
            pytest.param(".", None, True, id="airflow-itself"),
            pytest.param("providers", None, True, id="providers-package"),
            pytest.param("providers/apache", "apache", True, id="namespace-package-is-shared"),
            pytest.param("providers/acme", "acme", True, id="unknown-namespace-package-is-shared"),
            pytest.param("providers/hive", "apache", False, id="provider-package-is-not-shared"),
            pytest.param("providers/amazon", None, False, id="top-level-provider-package-is-not-shared"),
            pytest.param("providers/apache/hive", "apache", False, id="deeper-package-is-not-shared"),
        ],
    )
    def test_only_shared_folders_need_path_extension(self, relative_path, namespace, expected):
        assert _needs_path_extension_in_src(Path(relative_path), namespace) is expected

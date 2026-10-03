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

from check_prek_hooks_pycache_prefix import find_hooks_without_pycache_prefix

CONFIG = """\
repos:
  - repo: meta
    hooks:
      - id: with-prefix
        env:
          PYTHONPYCACHEPREFIX: .build/pycache
  - repo: local
    hooks:
      - id: no-env
      - id: other-env
        env:
          FOO: bar
      - id: wrong-prefix
        env:
          PYTHONPYCACHEPREFIX: /tmp/pycache
"""


def test_find_hooks_without_pycache_prefix(tmp_path):
    config = tmp_path / ".pre-commit-config.yaml"
    config.write_text(CONFIG)

    assert find_hooks_without_pycache_prefix(config) == ["no-env", "other-env", "wrong-prefix"]


def test_find_hooks_without_pycache_prefix_empty_config(tmp_path):
    config = tmp_path / ".pre-commit-config.yaml"
    config.write_text("")

    assert find_hooks_without_pycache_prefix(config) == []

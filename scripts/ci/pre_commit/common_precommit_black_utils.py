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

import os
import sys
from functools import lru_cache
from pathlib import Path

from black import Mode, TargetVersion, format_str, parse_pyproject_toml

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported

from common_precommit_utils import AIRFLOW_BREEZE_SOURCES_PATH  # isort: skip # noqa E402


@lru_cache(maxsize=None)
def black_mode(is_pyi: bool = Mode.is_pyi) -> Mode:
    config = parse_pyproject_toml(os.fspath(AIRFLOW_BREEZE_SOURCES_PATH / "pyproject.toml"))
    target_versions = {TargetVersion[val.upper()] for val in config.get("target_version", ())}

    return Mode(
        target_versions=target_versions,
        line_length=config.get("line_length", Mode.line_length),
        is_pyi=is_pyi,
    )


def black_format(content: str, is_pyi: bool = Mode.is_pyi) -> str:
    return format_str(content, mode=black_mode(is_pyi=is_pyi))

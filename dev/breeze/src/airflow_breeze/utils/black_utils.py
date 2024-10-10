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
from functools import cache

from black import Mode, TargetVersion, format_str, parse_pyproject_toml

from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT


@cache
def _black_mode() -> Mode:
    config = parse_pyproject_toml(os.path.join(AIRFLOW_SOURCES_ROOT, "pyproject.toml"))
    target_versions = {TargetVersion[val.upper()] for val in config.get("target_version", ())}
    return Mode(
        target_versions=target_versions,
        line_length=config.get("line_length", Mode.line_length),
    )


def black_format(content) -> str:
    return format_str(content, mode=_black_mode())

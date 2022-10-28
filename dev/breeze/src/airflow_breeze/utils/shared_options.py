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

__verbose_value: bool = os.environ.get("VERBOSE", "false")[0].lower() == "t"


def set_verbose(verbose: bool):
    global __verbose_value
    __verbose_value = verbose


def get_verbose(verbose_override: bool | None = None) -> bool:
    if verbose_override is None:
        return __verbose_value
    return verbose_override


__dry_run_value: bool = os.environ.get("DRY_RUN", "false")[0].lower() == "t"


def set_dry_run(dry_run: bool):
    global __dry_run_value
    __dry_run_value = dry_run


def get_dry_run(dry_run_override: bool | None = None) -> bool:
    if dry_run_override is None:
        return __dry_run_value
    return dry_run_override


__forced_answer: str | None = None


def set_forced_answer(answer: str | None):
    global __forced_answer
    __forced_answer = answer


def get_forced_answer(answer_override: str | None = None) -> str | None:
    if answer_override is None:
        return __forced_answer
    return answer_override

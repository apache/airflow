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

from airflow_breeze.utils.coertions import coerce_bool_value


class _SharedOptions:
    verbose_value: bool = coerce_bool_value(os.environ.get("VERBOSE", ""))
    dry_run_value: bool = coerce_bool_value(os.environ.get("DRY_RUN", ""))
    forced_answer: str | None = None


def set_verbose(verbose: bool):
    _SharedOptions.verbose_value = verbose


def get_verbose(verbose_override: bool | None = None) -> bool:
    if verbose_override is None:
        return _SharedOptions.verbose_value
    return verbose_override


def set_dry_run(dry_run: bool):
    _SharedOptions.dry_run_value = dry_run


def get_dry_run(dry_run_override: bool | None = None) -> bool:
    if dry_run_override is None:
        return _SharedOptions.dry_run_value
    return dry_run_override


def set_forced_answer(answer: str | None):
    _SharedOptions.forced_answer = answer


def get_forced_answer(answer_override: str | None = None) -> str | None:
    if answer_override is None:
        return _SharedOptions.forced_answer
    return answer_override

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

# Disable caching inside tests so config can be freely mocked per test.
if os.getenv("PYTEST_VERSION"):

    def _no_cache(func):
        return func

    cache = _no_cache
else:
    from functools import lru_cache

    cache = lru_cache()

from airflow.providers.common.compat.sdk import conf

_CONFIG_SECTION = "informatica"


@cache
def disabled_operators() -> set[str]:
    """Return FQCNs listed in ``[informatica] disabled_for_operators``."""
    option = conf.get(_CONFIG_SECTION, "disabled_for_operators", fallback="")
    return set(op.strip() for op in option.split(";") if op.strip())


@cache
def auto_lineage_enabled() -> bool:
    """Return True when ``[informatica] auto_lineage_enabled`` is set."""
    return conf.getboolean(_CONFIG_SECTION, "auto_lineage_enabled", fallback=True)


@cache
def listener_disabled() -> bool:
    """Return True when ``[informatica] listener_disabled`` is set."""
    return conf.getboolean(_CONFIG_SECTION, "listener_disabled", fallback=False)


def is_operator_disabled(operator: object) -> bool:
    """Return True when the operator's fully-qualified class name is disabled."""
    op_class = type(operator)
    fqcn = f"{op_class.__module__}.{op_class.__name__}"
    return fqcn in disabled_operators()

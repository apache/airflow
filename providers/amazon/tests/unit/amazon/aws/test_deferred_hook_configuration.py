#
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
"""
The half of the deferred hook configuration invariant that needs the classes themselves.

The static half -- that every ``self.defer()`` hands its hook configuration to the trigger -- is a
prek hook, ``scripts/ci/prek/check_deferred_hook_configuration.py``, because it only needs to parse
the provider. This one has to import it, so it stays a test.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil

import pytest

import airflow.providers.amazon.aws.triggers as triggers_module
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger


def find_waiter_triggers() -> list[type[AwsBaseWaiterTrigger]]:
    """Import every trigger module, then walk the subclass tree."""
    for module in pkgutil.iter_modules(triggers_module.__path__):
        try:
            importlib.import_module(f"{triggers_module.__name__}.{module.name}")
        except ImportError:
            # triggers/eks.py reaches into cncf.kubernetes. Installing amazon without its optional
            # peers is supported, and must not turn collection of this module into an error.
            continue

    found: set[type[AwsBaseWaiterTrigger]] = set()
    pending = [AwsBaseWaiterTrigger]
    while pending:
        for subclass in pending.pop().__subclasses__():
            if subclass not in found:
                found.add(subclass)
                pending.append(subclass)
    return sorted(found, key=lambda cls: cls.__name__)


@pytest.mark.parametrize(
    "trigger_class",
    find_waiter_triggers(),
    ids=lambda cls: cls.__name__,
)
def test_waiter_trigger_can_build_a_hook(trigger_class):
    """The hook a trigger names must accept what ``_hook_parameters`` will pass it."""
    if trigger_class.hook is not AwsBaseWaiterTrigger.hook:
        pytest.skip(f"{trigger_class.__name__} builds its hook by hand")
    inspect.signature(trigger_class.aws_hook_class).bind_partial(
        aws_conn_id=None, region_name=None, verify=None, config=None
    )

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

from airflow.providers.sandbox.backends.base import SandboxProvider
from airflow.providers.sandbox.backends.local import LocalProvider
from airflow.providers.sandbox.provider_loader import load_provider


def test_loads_builtin_alias():
    provider = load_provider("local")
    assert isinstance(provider, LocalProvider)
    assert isinstance(provider, SandboxProvider)


def test_loads_by_fqcn():
    provider = load_provider("airflow.providers.sandbox.backends.local:LocalProvider")
    assert isinstance(provider, LocalProvider)


@pytest.mark.parametrize("bad", ["", "nope", "module.without.colon"])
def test_rejects_bad_names(bad):
    with pytest.raises((ValueError, TypeError, ImportError)):
        load_provider(bad)

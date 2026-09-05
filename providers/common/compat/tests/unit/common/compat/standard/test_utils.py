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

from airflow.providers.common.compat.standard import utils

EXPECTED_EXPORTS = (
    "SkipMixin",
    "XCOM_SKIPMIXIN_KEY",
    "XCOM_SKIPMIXIN_SKIPPED",
    "XCOM_SKIPMIXIN_FOLLOWED",
    "write_python_script",
    "prepare_virtualenv",
)


def test_public_exports():
    assert set(utils.__all__) == set(EXPECTED_EXPORTS)


@pytest.mark.parametrize("name", EXPECTED_EXPORTS)
def test_all_compat_imports_work(name):
    assert getattr(utils, name) is not None


def test_invalid_import_raises_attribute_error():
    with pytest.raises(AttributeError, match="has no attribute 'NonExistentClass'"):
        _ = utils.NonExistentClass

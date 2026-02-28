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

from airflowctl.utils.module_loading import import_string


def test_import_string_imports_attribute_successfully():
    assert import_string("builtins.str") is str


def test_import_string_rejects_invalid_dotted_path():
    with pytest.raises(ImportError, match="doesn't look like a module path"):
        import_string("invalid_path")


def test_import_string_raises_when_module_is_missing():
    with pytest.raises(ImportError):
        import_string("not_a_real_module.SomeClass")


def test_import_string_raises_when_attribute_is_missing():
    with pytest.raises(ImportError, match='does not define a "missing_attr" attribute/class'):
        import_string("builtins.missing_attr")

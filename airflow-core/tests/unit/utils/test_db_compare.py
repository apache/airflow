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

from types import SimpleNamespace

import pytest

from airflow.utils.db import compare_server_default


@pytest.mark.parametrize(
    ("inspected_default", "rendered_metadata_default"),
    [
        ("(false)", "0"),
        ("(-(1))", "-1"),
    ],
)
def test_compare_server_default_ignores_equivalent_mysql_defaults(
    inspected_default, rendered_metadata_default
):
    context = SimpleNamespace(connection=SimpleNamespace(dialect=SimpleNamespace(name="mysql")))
    metadata_column = SimpleNamespace(name="has_import_errors", table=SimpleNamespace(name="dag"))

    result = compare_server_default(
        context=context,
        inspected_column=None,
        metadata_column=metadata_column,
        inspected_default=inspected_default,
        metadata_default=None,
        rendered_metadata_default=rendered_metadata_default,
    )

    assert result is False


def test_compare_server_default_preserves_non_equivalent_mysql_defaults():
    context = SimpleNamespace(connection=SimpleNamespace(dialect=SimpleNamespace(name="mysql")))
    metadata_column = SimpleNamespace(name="has_import_errors", table=SimpleNamespace(name="dag"))

    result = compare_server_default(
        context=context,
        inspected_column=None,
        metadata_column=metadata_column,
        inspected_default="1",
        metadata_default=None,
        rendered_metadata_default="0",
    )

    assert result is None

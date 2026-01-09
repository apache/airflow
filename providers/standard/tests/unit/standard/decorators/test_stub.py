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

import contextlib

import pytest

from airflow.providers.standard.decorators.stub import stub


def fn_ellipsis(): ...


def fn_pass(): ...


def fn_doc():
    """Some string"""


def fn_doc_pass():
    """Some string"""
    pass


def fn_code():
    return None


@pytest.mark.parametrize(
    ("fn", "error"),
    [
        pytest.param(fn_ellipsis, contextlib.nullcontext(), id="ellipsis"),
        pytest.param(fn_pass, contextlib.nullcontext(), id="pass"),
        pytest.param(fn_doc, contextlib.nullcontext(), id="doc"),
        pytest.param(fn_doc_pass, contextlib.nullcontext(), id="doc-and-pass"),
        pytest.param(fn_code, pytest.raises(ValueError, match="must be an empty function"), id="not-empty"),
    ],
)
def test_stub_signature(fn, error):
    with error:
        stub(fn)()

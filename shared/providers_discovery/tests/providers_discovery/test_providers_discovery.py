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
from __future__ import annotations

import pytest

from airflow_shared.providers_discovery import LazyDictWithCache


@pytest.mark.parametrize(
    ("value", "expected_outputs"),
    [
        ("a", "a"),
        (1, 1),
        (None, None),
        (lambda: 0, 0),
        (lambda: None, None),
        (lambda: "z", "z"),
    ],
)
def test_lazy_cache_dict_resolving(value, expected_outputs):
    lazy_cache_dict = LazyDictWithCache()
    lazy_cache_dict["key"] = value
    assert lazy_cache_dict["key"] == expected_outputs
    # Retrieve it again to see if it is correctly returned again
    assert lazy_cache_dict["key"] == expected_outputs


def test_lazy_cache_dict_raises_error():
    def raise_method():
        raise RuntimeError("test")

    lazy_cache_dict = LazyDictWithCache()
    lazy_cache_dict["key"] = raise_method
    with pytest.raises(RuntimeError, match="test"):
        _ = lazy_cache_dict["key"]


def test_lazy_cache_dict_del_item():
    lazy_cache_dict = LazyDictWithCache()

    def answer():
        return 42

    lazy_cache_dict["spam"] = answer
    assert "spam" in lazy_cache_dict._raw_dict
    assert "spam" not in lazy_cache_dict._resolved  # Not resoled yet
    assert lazy_cache_dict["spam"] == 42
    assert "spam" in lazy_cache_dict._resolved
    del lazy_cache_dict["spam"]
    assert "spam" not in lazy_cache_dict._raw_dict
    assert "spam" not in lazy_cache_dict._resolved

    lazy_cache_dict["foo"] = answer
    assert lazy_cache_dict["foo"] == 42
    assert "foo" in lazy_cache_dict._resolved
    # Emulate some mess in data, e.g. value from `_raw_dict` deleted but not from `_resolved`
    del lazy_cache_dict._raw_dict["foo"]
    assert "foo" in lazy_cache_dict._resolved
    with pytest.raises(KeyError):
        # Error expected here, but we still expect to remove also record into `resolved`
        del lazy_cache_dict["foo"]
    assert "foo" not in lazy_cache_dict._resolved

    lazy_cache_dict["baz"] = answer
    # Key in `_resolved` not created yet
    assert "baz" in lazy_cache_dict._raw_dict
    assert "baz" not in lazy_cache_dict._resolved
    del lazy_cache_dict._raw_dict["baz"]
    assert "baz" not in lazy_cache_dict._raw_dict
    assert "baz" not in lazy_cache_dict._resolved


def test_lazy_cache_dict_clear():
    def answer():
        return 42

    lazy_cache_dict = LazyDictWithCache()
    assert len(lazy_cache_dict) == 0
    lazy_cache_dict["spam"] = answer
    lazy_cache_dict["foo"] = answer
    lazy_cache_dict["baz"] = answer

    assert len(lazy_cache_dict) == 3
    assert len(lazy_cache_dict._raw_dict) == 3
    assert not lazy_cache_dict._resolved
    assert lazy_cache_dict["spam"] == 42
    assert len(lazy_cache_dict._resolved) == 1
    # Emulate some mess in data, contain some data into the `_resolved`
    lazy_cache_dict._resolved.add("biz")
    assert len(lazy_cache_dict) == 3
    assert len(lazy_cache_dict._resolved) == 2
    # And finally cleanup everything
    lazy_cache_dict.clear()
    assert len(lazy_cache_dict) == 0
    assert not lazy_cache_dict._raw_dict
    assert not lazy_cache_dict._resolved

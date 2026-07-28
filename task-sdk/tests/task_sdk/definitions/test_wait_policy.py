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

from airflow.sdk.definitions.partition_mappers.wait_policy import MinimumCount, WaitForAll


class TestSdkWaitForAll:
    def test_repr(self):
        assert repr(WaitForAll()) == "WaitForAll()"

    def test_eq(self):
        assert WaitForAll() == WaitForAll()

    def test_neq_other_policy(self):
        assert WaitForAll() != MinimumCount(1)

    def test_hash_consistent(self):
        assert hash(WaitForAll()) == hash(WaitForAll())


class TestSdkMinimumCount:
    def test_stores_n(self):
        assert MinimumCount(5).n == 5

    def test_eq_same_n(self):
        assert MinimumCount(5) == MinimumCount(5)

    def test_neq_different_n(self):
        assert MinimumCount(5) != MinimumCount(6)

    def test_repr(self):
        assert repr(MinimumCount(5)) == "MinimumCount(n=5)"

    def test_hash_consistent(self):
        assert hash(MinimumCount(5)) == hash(MinimumCount(5))

    def test_zero_rejected(self):
        with pytest.raises(ValueError, match="MinimumCount\\(0\\) is degenerate"):
            MinimumCount(0)

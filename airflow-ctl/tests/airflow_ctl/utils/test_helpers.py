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

from airflowctl.utils.helpers import partition


def test_partition_splits_false_and_true_entries():
    false_entries, true_entries = partition(lambda value: value % 2 == 0, [0, 1, 2, 3, 4])

    assert list(false_entries) == [1, 3]
    assert list(true_entries) == [0, 2, 4]


def test_partition_handles_empty_iterable():
    false_entries, true_entries = partition(bool, [])

    assert list(false_entries) == []
    assert list(true_entries) == []


def test_partition_works_with_one_shot_iterators():
    source = (value for value in range(4))

    false_entries, true_entries = partition(lambda value: value >= 2, source)

    # Consume the true group first; tee must still let both sides see the full input.
    assert list(true_entries) == [2, 3]
    assert list(false_entries) == [0, 1]

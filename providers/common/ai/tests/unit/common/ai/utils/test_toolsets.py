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

from pydantic_ai.toolsets.combined import CombinedToolset
from pydantic_ai.toolsets.function import FunctionToolset

from airflow.providers.common.ai.utils.toolsets import find_toolset, iter_toolsets


class _Marker(FunctionToolset):
    """A toolset type worth finding."""


class TestIterToolsets:
    def test_a_plain_toolset_yields_itself(self):
        ts = FunctionToolset()
        assert list(iter_toolsets(ts)) == [ts]

    def test_wrappers_are_unwrapped_outermost_first(self):
        inner = FunctionToolset()
        prefixed = inner.prefixed("a")
        filtered = prefixed.filtered(lambda ctx, tool: True)

        assert list(iter_toolsets(filtered)) == [filtered, prefixed, inner]

    def test_a_combined_toolset_yields_its_members_in_order(self):
        a, b = FunctionToolset(), FunctionToolset()
        combined = CombinedToolset([a, b])

        assert list(iter_toolsets(combined)) == [combined, a, b]

    def test_wrappers_inside_combinations_inside_wrappers_are_all_reached(self):
        leaf = _Marker()
        composed = CombinedToolset([FunctionToolset(), leaf.prefixed("x")]).prefixed("y")

        assert leaf in list(iter_toolsets(composed))


class TestFindToolset:
    def test_returns_the_nested_instance(self):
        leaf = _Marker()
        toolsets = [FunctionToolset(), CombinedToolset([leaf.prefixed("x")])]

        assert find_toolset(toolsets, _Marker) is leaf

    def test_returns_none_when_absent(self):
        assert find_toolset([FunctionToolset().prefixed("x")], _Marker) is None

    def test_empty_input(self):
        assert find_toolset([], _Marker) is None

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

from ci.prek.check_order_dockerfile_extras import get_replaced_content


class TestGetReplacedContent:
    def test_replaces_between_markers(self):
        content = [
            "before\n",
            "# START\n",
            "old_item_1\n",
            "old_item_2\n",
            "# END\n",
            "after\n",
        ]
        result = get_replaced_content(
            content,
            ["new_a", "new_b"],
            "# START",
            "# END",
            prefix='"',
            suffix='",',
            add_empty_lines=False,
        )
        assert result == [
            "before\n",
            "# START\n",
            '"new_a",\n',
            '"new_b",\n',
            "# END\n",
            "after\n",
        ]

    def test_replaces_with_empty_lines(self):
        content = [
            "before\n",
            ".. START\n",
            "old\n",
            ".. END\n",
            "after\n",
        ]
        result = get_replaced_content(
            content,
            ["item1", "item2"],
            ".. START",
            ".. END",
            prefix="* ",
            suffix="",
            add_empty_lines=True,
        )
        assert result == [
            "before\n",
            ".. START\n",
            "\n",
            "* item1\n",
            "* item2\n",
            "\n",
            ".. END\n",
            "after\n",
        ]

    def test_preserves_content_outside_markers(self):
        content = [
            "line1\n",
            "line2\n",
            "# START\n",
            "old\n",
            "# END\n",
            "line3\n",
            "line4\n",
        ]
        result = get_replaced_content(
            content, ["new"], "# START", "# END", prefix="", suffix="", add_empty_lines=False
        )
        assert result[0] == "line1\n"
        assert result[1] == "line2\n"
        assert result[-2] == "line3\n"
        assert result[-1] == "line4\n"

    def test_empty_extras_list(self):
        content = [
            "# START\n",
            "old\n",
            "# END\n",
        ]
        result = get_replaced_content(
            content, [], "# START", "# END", prefix="", suffix="", add_empty_lines=False
        )
        assert result == ["# START\n", "# END\n"]

    def test_no_markers_returns_content_unchanged(self):
        content = ["line1\n", "line2\n", "line3\n"]
        result = get_replaced_content(
            content,
            ["new"],
            "# NONEXISTENT START",
            "# NONEXISTENT END",
            prefix="",
            suffix="",
            add_empty_lines=False,
        )
        assert result == content

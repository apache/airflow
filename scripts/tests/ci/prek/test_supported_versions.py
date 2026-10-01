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

import textwrap

import pytest
from ci.prek import supported_versions as hook

README = textwrap.dedent(
    """\
    |            | Main version (dev)  | Stable version (3.3.0)   | Deprecate version (2.11.2)   |
    |------------|---------------------|--------------------------|------------------------------|
    files in the orphan `constraints-main` and `constraints-2-0` branches.

    pip install 'apache-airflow==3.3.0' \\
     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.10.txt"

    pip install 'apache-airflow[postgres,google]==3.3.0' \\
     --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.10.txt"
    """
)


class TestUpdateStableVersionInReadme:
    def test_updates_header_and_pins(self):
        result = hook.update_stable_version_in_readme(README, "3.3.2")

        assert result == README.replace("3.3.0", "3.3.2")

    def test_keeps_table_cell_width(self):
        result = hook.update_stable_version_in_readme(README, "3.3.10")

        header, separator = result.splitlines()[:2]
        assert "| Stable version (3.3.10)  |" in header
        assert len(header) == len(separator)

    def test_is_idempotent(self):
        once = hook.update_stable_version_in_readme(README, "3.3.2")

        assert hook.update_stable_version_in_readme(once, "3.3.2") == once

    @pytest.mark.parametrize(
        "removed",
        [
            pytest.param(["Stable version (3.3.0)"], id="header"),
            pytest.param(["'apache-airflow==3.3.0'", "'apache-airflow[postgres,google]==3.3.0'"], id="pip"),
            pytest.param(["/apache/airflow/constraints-3.3.0/"], id="constraints"),
        ],
    )
    def test_raises_when_a_pin_site_disappears(self, removed):
        text = README
        for snippet in removed:
            text = text.replace(snippet, "")

        with pytest.raises(RuntimeError, match="not found in README.md"):
            hook.update_stable_version_in_readme(text, "3.3.2")

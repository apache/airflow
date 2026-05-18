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

from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Template

TEMPLATE_PATH = Path(__file__).parents[1] / "src" / "airflow_breeze" / "provider_issue_TEMPLATE.md.jinja2"


@pytest.mark.parametrize(("is_new", "has_marker"), [(True, True), (False, False)])
def test_provider_issue_template_marks_new_provider(is_new: bool, has_marker: bool):
    provider_info = SimpleNamespace(
        version="0.1.0",
        suffix="",
        pypi_package_name="apache-airflow-providers-vespa",
        pr_list=[],
        is_new=is_new,
    )
    template = Template(TEMPLATE_PATH.read_text())

    rendered = template.render(
        providers={"vespa": provider_info},
        linked_issues={},
        date="2026-04-24",
    )

    assert (":tada: New provider" in rendered) is has_marker

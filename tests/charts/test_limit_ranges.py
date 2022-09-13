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

import unittest

import jmespath

from tests.charts.helm_template_generator import render_chart


class LimitRangesTest(unittest.TestCase):
    def test_limit_ranges_template(self):
        docs = render_chart(
            values={"limits": [{"max": {"cpu": "500m"}, "min": {"min": "200m"}, "type": "Container"}]},
            show_only=["templates/limitrange.yaml"],
        )
        assert "LimitRange" == jmespath.search("kind", docs[0])
        assert "500m" == jmespath.search("spec.limits[0].max.cpu", docs[0])

    def test_limit_ranges_are_not_added_by_default(self):
        docs = render_chart(show_only=["templates/limitrange.yaml"])
        assert docs == []

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

import json
from pathlib import Path

import yaml
from jsonschema import validate

CHART_DIR = Path(__file__).resolve().parents[3] / "chart"


class TestChartQuality:
    def test_values_validate_schema(self):
        values = yaml.safe_load((CHART_DIR / "values.yaml").read_text())
        schema = json.loads((CHART_DIR / "values.schema.json").read_text())

        # Add extra restrictions just for the tests to make sure
        # we don't forget to update the schema if we add a new property
        schema["additionalProperties"] = False
        schema["minProperties"] = len(schema["properties"].keys())

        # shouldn't raise
        validate(instance=values, schema=schema)

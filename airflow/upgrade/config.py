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

from __future__ import absolute_import
import yaml
from jsonschema import validate

from airflow.utils.module_loading import import_string

SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "ignored_rules": {"type": ["array", "null"], "items": {"type": "string"}},
        "custom_rules": {"type": ["array", "null"], "items": {"type": "string"}},
    },
    "additionalProperties": False,
}


class UpgradeConfig(object):
    def __init__(self, raw_config):
        self._raw_config = raw_config

    def get_ignored_rules(self):
        return self._raw_config.get("ignored_rules") or []

    def get_custom_rules(self):
        custom_rules = self._raw_config.get("custom_rules") or []
        return [import_string(r)() for r in custom_rules]

    @classmethod
    def read(cls, path):
        with open(path) as f:
            raw_config = yaml.safe_load(f)
        validate(raw_config, schema=SCHEMA)
        return cls(raw_config)

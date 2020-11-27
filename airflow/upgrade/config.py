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

from airflow.utils.module_loading import import_string


class UpgradeConfig(object):
    def __init__(self, raw_config):
        self._raw_config = raw_config

    def remove_ignored_rules(self, rules):
        rules_to_ignore = self._raw_config.get("ignored_rules")
        if not rules_to_ignore:
            return rules
        return [r for r in rules if r.__class__.__name__ not in rules_to_ignore]

    def register_custom_rules(self, rules):
        custom_rules = self._raw_config.get("custom_rules")
        if not custom_rules:
            return rules
        for custom_rule in custom_rules:
            rule = import_string(custom_rule)
            rules.append(rule())
        return rules

    @classmethod
    def read(cls, path):
        with open(path) as f:
            raw_config = yaml.safe_load(f)
        return cls(raw_config)

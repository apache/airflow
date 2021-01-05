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

from airflow.upgrade.rules import get_rules
from airflow.upgrade.rules.conn_type_is_not_nullable import ConnTypeIsNotNullableRule
from airflow.upgrade.rules.base_rule import BaseRule


class TestBaseRule:
    def test_rules_are_registered(self):
        rule_classes = get_rules()
        assert BaseRule not in rule_classes
        assert ConnTypeIsNotNullableRule in rule_classes

    def test_rules_are_ordered(self):
        rule_classes = get_rules()
        from airflow.upgrade.rules.aaa_airflow_version_check import VersionCheckRule
        from airflow.upgrade.rules.conn_id_is_unique import UniqueConnIdRule
        from airflow.upgrade.rules.no_additional_args_in_operators import (
            NoAdditionalArgsInOperatorsRule,
        )

        # Version check should still be first
        assert rule_classes[0] == VersionCheckRule
        # The former rule is defined in a file alphabetically before the latter rule,
        # but it should appear later in the list since the classes are sorted alphabetically
        unique_rule_index = rule_classes.index(UniqueConnIdRule)
        no_addtl_args_rule_index = rule_classes.index(NoAdditionalArgsInOperatorsRule)
        assert unique_rule_index > no_addtl_args_rule_index, "Rules are not alphabetical by class name"

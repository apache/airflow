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
from typing import List

from airflow.upgrade.formatters import BaseFormatter
from airflow.upgrade.problem import RuleStatus
from airflow.upgrade.rules.base_rule import BaseRule, RULES

ALL_RULES = [cls() for cls in RULES]  # type: List[BaseRule]


def check_upgrade(formatter):
    # type: (BaseFormatter) -> List[RuleStatus]
    formatter.start_checking(ALL_RULES)
    all_rule_statuses = []  # List[RuleStatus]
    for rule in ALL_RULES:
        rule_status = RuleStatus.from_rule(rule)
        all_rule_statuses.append(rule_status)
        formatter.on_next_rule_status(rule_status)
    formatter.end_checking(all_rule_statuses)
    return all_rule_statuses

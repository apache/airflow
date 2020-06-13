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

from typing import List

from airflow.upgrade.formatters.base_formatter import BaseFormatter
from airflow.upgrade.problem import Problem
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.upgrade.rules.conn_type_is_not_nullable import ConnTypeIsNotNullableRule

# TODO: Add auto-discoverable for rules
ALL_RULES: List[BaseRule] = [
    ConnTypeIsNotNullableRule()
]


def check_upgrade(formatter: BaseFormatter):
    formatter.start_checking()
    all_problems: List[Problem] = []
    for rule in ALL_RULES:
        messages = rule.check()
        problems = [Problem(rule=rule, message=message) for message in messages]
        all_problems.extend(problems)
        formatter.on_next_rule_status(rule, problems)
    formatter.end_checking(all_problems)
    return all_problems

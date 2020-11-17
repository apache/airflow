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
import argparse
import sys
from typing import List

from airflow.upgrade.formatters import BaseFormatter
from airflow.upgrade.problem import RuleStatus
from airflow.upgrade.rules import get_rules
from airflow.upgrade.rules.base_rule import BaseRule

ALL_RULES = [cls() for cls in get_rules()]  # type: List[BaseRule]


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


def register_arguments(subparser):
    subparser.add_argument(
        "-s", "--save",
        help="Saves the result to the indicated file. The file format is determined by the file extension."
    )
    subparser.set_defaults(func=run)


def run(args):
    from airflow.upgrade.formatters import (ConsoleFormatter, JSONFormatter)
    if args.save:
        filename = args.save
        if not filename.lower().endswith(".json"):
            exit("Only JSON files are supported")
        formatter = JSONFormatter(args.save)
    else:
        formatter = ConsoleFormatter()
    all_problems = check_upgrade(formatter)
    if all_problems:
        sys.exit(1)


def __main__():
    parser = argparse.ArgumentParser()
    register_arguments(parser)
    args = parser.parse_args()
    args.func(args)

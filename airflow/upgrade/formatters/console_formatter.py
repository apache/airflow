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

import os
from itertools import groupby

from airflow.upgrade.formatters.base_formatter import BaseFormatter
from airflow.utils.cli import header


class ConsoleFormatter(BaseFormatter):
    def start_checking(self):
        print()
        header("STATUS", "=")
        print()

    def end_checking(self, all_problems):
        if all_problems:
            print()
            if len(all_problems) == 1:
                print(f"Found {len(all_problems)} problem.")
            else:
                print(f"Found {len(all_problems)} problems.")
            print()
            header("RECOMMENDATIONS", "=")
            print()

            self.display_recommendations(all_problems)
        else:
            print(
                "Not found any problems. World is beautiful. "
                "You can safely update Airflow to the new version."
            )

    @staticmethod
    def display_recommendations(all_problems):
        for rule, problems in groupby(all_problems, key=lambda d: d.rule):
            print(rule.title)
            print("-" * len(rule.title))
            print(rule.description)
            print("")
            print("Problems:")
            for problem_no, problem in enumerate(problems, 1):
                print(f'{problem_no:>3}.  {problem.message}')

    def on_next_rule_status(self, rule, problems):
        status = "SUCCESS" if problems else "FAIL"
        status_line_fmt = self.prepare_status_line_format()
        print(status_line_fmt.format(rule.title, status))

    @staticmethod
    def prepare_status_line_format():
        terminal_width, _ = os.get_terminal_size(0)

        return "{:.<" + str(terminal_width - 10) + "}{:.>10}"

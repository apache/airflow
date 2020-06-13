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

import json
import shlex

from airflow.upgrade.formatters.base_formatter import BaseFormatter


class JSONFormatter(BaseFormatter):
    def __init__(self, output_path: str):
        self.filename = output_path

    def start_checking(self):
        print("Start looking for problems.")

    def end_checking(self, all_problems):
        formatted_results = [
            {"rule": str(type(problem.rule).__name__), "message": str(problem.message)}
            for problem in all_problems
        ]
        with open(self.filename, "w") as output_file:
            json.dump(formatted_results, output_file, indent=2)
        print(f"Saved result to: {shlex.quote(self.filename)}")

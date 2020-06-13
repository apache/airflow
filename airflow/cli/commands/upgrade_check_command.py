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

import sys

from airflow.upgrade.checker import check_upgrade
from airflow.upgrade.formatters.console_formatter import ConsoleFormatter
from airflow.upgrade.formatters.json_formatter import JSONFormatter


def upgrade_check(args):
    if args.save:
        filename: str = args.save
        if not filename.lower().endswith(".json"):
            print("Only JSON files are supported", file=sys.stderr)
        formatter = JSONFormatter(args.save)
    else:
        formatter = ConsoleFormatter()
    all_problems = check_upgrade(formatter)
    if all_problems:
        exit(1)

#!/usr/bin/env python3

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

import itertools
import os
import re
import sys

from tabulate import tabulate

from airflow.stats import METRICS_LIST

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
METRICS_RST_PATH = os.path.join(ROOT_DIR, "docs", "logging-monitoring", "metrics.rst")

RST_HEADER_TEMPLATE = '.. START METRICS[TYPE={}] REFERENCE HERE\n'
RST_FOOTER_TEMPLATE = '.. END METRICS[TYPE={}] REFERENCE HERE\n'
TEMPLATES = [RST_HEADER_TEMPLATE, RST_FOOTER_TEMPLATE]

allowed_metric_types = sorted(metric.metric_type for metric in METRICS_LIST)

with open(METRICS_RST_PATH) as rst_file:
    content = rst_file.read()

for metric_type, template in itertools.product(allowed_metric_types, TEMPLATES):
    marker = template.format(metric_type)
    if marker not in content:
        print(f"Missing marker '{marker}' in file '{METRICS_RST_PATH}'", file=sys.stderr)
        sys.exit(1)

for metric_type in allowed_metric_types:
    table_data = [
        [f"``{metric.key}``", metric.description]
        for metric in sorted(METRICS_LIST, key=lambda d: d.key)
        if metric.metric_type == metric_type
    ]
    rst_table = (
        tabulate(
            table_data,
            headers=["Key", "Description"],
            tablefmt="rst",
        )
        + "\n"
    )

    header = RST_HEADER_TEMPLATE.format(metric_type)
    footer = RST_FOOTER_TEMPLATE.format(metric_type)
    expected_content = "\n".join(
        [
            header,
            rst_table,
            footer,
        ]
    )
    if expected_content in content:
        continue
    pattern = "^" + re.escape(header) + r".+?" + re.escape(footer)
    content, number_of_subs_made = re.subn(
        pattern=pattern, string=content, repl=expected_content, flags=re.MULTILINE | re.DOTALL
    )

    if number_of_subs_made != 1:
        print(f"The '{metric_type}' metric in file '{METRICS_RST_PATH}' failed to fix.", file=sys.stderr)
        sys.exit(1)

with open(METRICS_RST_PATH, "w") as rst_file:
    rst_file.write(content)

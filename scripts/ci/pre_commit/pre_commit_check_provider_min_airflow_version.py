#!/usr/bin/env python
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
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore

ROOT_DIR = Path(__file__).resolve().parents[3]

if __name__ == '__main__':
    provider_files_pattern = Path(ROOT_DIR).glob("airflow/providers/**/provider.yaml")
    all_provider_files = sorted(str(path) for path in provider_files_pattern)

    if len(sys.argv) > 1:
        paths = sorted(sys.argv[1:])
    else:
        paths = all_provider_files

    for path in paths:
        print(path)
        with open(path) as yaml_file:
            provider = yaml.load(yaml_file, SafeLoader)
            deps = provider['dependencies']
            try:
                airflow_dep = [x for x in deps if x.startswith('apache-airflow>=')][0]
            except IndexError:
                continue
            min_airflow_version = airflow_dep.split('>=')[1].split('.')
            min_airflow_version = tuple(map(int, min_airflow_version))
            init_mod = Path(path).parent / '__init__.py'
            old_content: str = init_mod.read_text()
            var_declaration = 'MIN_AIRFLOW_VERSION = '
            full_var_def = f"{var_declaration}{min_airflow_version}"
            if var_declaration in old_content:
                new_content = re.sub(f"{var_declaration}(.+)", lambda x: full_var_def, old_content)
            else:
                new_content = old_content + f"\n{full_var_def}\n"
            init_mod.write_text(new_content)

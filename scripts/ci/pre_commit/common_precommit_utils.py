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
from pathlib import Path
from typing import List

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()


def insert_documentation(file_path: Path, content: List[str], header: str, footer: str):
    text = file_path.read_text().splitlines(keepends=True)
    replacing = False
    result: List[str] = []
    for line in text:
        if line.startswith(header):
            replacing = True
            result.append(line)
            result.extend(content)
        if line.startswith(footer):
            replacing = False
        if not replacing:
            result.append(line)
    src = "".join(result)
    file_path.write_text(src)

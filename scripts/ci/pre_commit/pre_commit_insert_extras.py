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
import sys
from pathlib import Path
from textwrap import wrap
from typing import List

AIRFLOW_SOURCES_DIR = Path(__file__).parents[3].absolute()

sys.path.insert(0, str(AIRFLOW_SOURCES_DIR))
# flake8: noqa: F401

from setup import EXTRAS_REQUIREMENTS  # isort:skip

sys.path.append(str(AIRFLOW_SOURCES_DIR))

RST_HEADER = '  .. START EXTRAS HERE'
RST_FOOTER = '  .. END EXTRAS HERE'

INSTALL_HEADER = '# START EXTRAS HERE'
INSTALL_FOOTER = '# END EXTRAS HERE'

CONSTANTS_HEADER = '# START EXTRAS HERE'
CONSTANTS_FOOTER = '# END EXTRAS HERE'

DEFAULT_EXTRAS = (
    "amazon,async,celery,cncf.kubernetes,dask,docker,elasticsearch,ftp,google,"
    "google_auth,grpc,hashicorp,http,ldap,microsoft.azure,mysql,odbc,pandas,"
    "postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv"
)


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
    file_path.write_text("".join(result))


if __name__ == '__main__':
    install_file_path = AIRFLOW_SOURCES_DIR / 'INSTALL'
    contributing_file_path = AIRFLOW_SOURCES_DIR / 'CONTRIBUTING.rst'
    global_constants_file_path = (
        AIRFLOW_SOURCES_DIR / "dev" / "breeze" / "src" / "airflow_breeze" / "global_constants.py"
    )
    extras_list = wrap(", ".join(EXTRAS_REQUIREMENTS.keys()), 100)
    extras_list = [line + "\n" for line in extras_list]
    extras_code = [f"    {extra}\n" for extra in EXTRAS_REQUIREMENTS.keys()]
    insert_documentation(install_file_path, extras_list, INSTALL_HEADER, INSTALL_FOOTER)
    insert_documentation(contributing_file_path, extras_list, RST_HEADER, RST_FOOTER)

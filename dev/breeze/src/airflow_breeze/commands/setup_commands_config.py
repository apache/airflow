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
from typing import Dict, List, Union

SETUP_COMMANDS: Dict[str, Union[str, List[str]]] = {
    "name": "Setup",
    "commands": [
        "autocomplete",
        "self-upgrade",
        "cleanup",
        "config",
        "regenerate-command-images",
        "command-hash-export",
        "version",
    ],
}
SETUP_PARAMETERS: Dict[str, List[Dict[str, Union[str, List[str]]]]] = {
    "breeze setup self-upgrade": [
        {
            "name": "Self-upgrade flags",
            "options": [
                "--use-current-airflow-sources",
                "--force",
            ],
        }
    ],
    "breeze setup autocomplete": [
        {
            "name": "Setup autocomplete flags",
            "options": [
                "--force",
            ],
        },
    ],
    "breeze setup config": [
        {
            "name": "Config flags",
            "options": [
                "--python",
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--cheatsheet",
                "--asciiart",
                "--colour",
            ],
        },
    ],
}

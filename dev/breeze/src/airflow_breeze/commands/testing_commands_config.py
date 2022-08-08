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

TESTING_COMMANDS: Dict[str, Union[str, List[str]]] = {
    "name": "Testing",
    "commands": ["tests", "helm-tests", "docker-compose-tests"],
}
TESTING_PARAMETERS: Dict[str, List[Dict[str, Union[str, List[str]]]]] = {
    "breeze testing tests": [
        {
            "name": "Basic flag for tests command",
            "options": [
                "--integration",
                "--test-type",
                "--db-reset",
                "--backend",
                "--python",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
            ],
        },
        {
            "name": "Advanced flag for tests command",
            "options": [
                "--limit-progress-output",
                "--image-tag",
                "--mount-sources",
            ],
        },
    ],
    "breeze testing helm-tests": [
        {
            "name": "Advanced flag for helms-tests command",
            "options": [
                "--limit-progress-output",
                "--image-tag",
                "--mount-sources",
            ],
        },
    ],
    "breeze testing docker-compose-tests": [
        {
            "name": "Docker-compose tests flag",
            "options": [
                "--image-name",
                "--image-tag",
                "--python",
            ],
        }
    ],
}

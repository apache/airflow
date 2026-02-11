#
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


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-sail",
        "name": "Sail",
        "description": "`Sail <https://lakesail.com/>`__\n",
        "integrations": [
            {
                "integration-name": "Sail",
                "external-doc-url": "https://github.com/lakehq/sail",
                "tags": ["service"],
            }
        ],
        "operators": [
            {
                "integration-name": "Sail",
                "python-modules": [
                    "airflow.providers.sail.operators.sail",
                ],
            }
        ],
        "hooks": [
            {
                "integration-name": "Sail",
                "python-modules": [
                    "airflow.providers.sail.hooks.sail",
                ],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.sail.hooks.sail.SailHook",
                "connection-type": "sail",
            },
        ],
        "task-decorators": [
            {
                "class-name": "airflow.providers.sail.decorators.pysail.pysail_task",
                "name": "pysail",
            }
        ],
    }

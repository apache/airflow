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

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE OVERWRITTEN!
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-arangodb",
        "name": "ArangoDB",
        "description": "`ArangoDB <https://www.arangodb.com/>`__\n",
        "integrations": [
            {
                "integration-name": "ArangoDB",
                "external-doc-url": "https://www.arangodb.com/",
                "tags": ["software"],
            }
        ],
        "hooks": [
            {"integration-name": "ArangoDB", "python-modules": ["airflow.providers.arangodb.hooks.arangodb"]}
        ],
        "operators": [
            {
                "integration-name": "ArangoDB",
                "python-modules": ["airflow.providers.arangodb.operators.arangodb"],
            }
        ],
        "sensors": [
            {
                "integration-name": "ArangoDB",
                "python-modules": ["airflow.providers.arangodb.sensors.arangodb"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.arangodb.hooks.arangodb.ArangoDBHook",
                "connection-type": "arangodb",
            }
        ],
    }

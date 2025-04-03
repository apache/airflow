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
        "package-name": "apache-airflow-providers-exasol",
        "name": "Exasol",
        "description": "`Exasol <https://www.exasol.com/>`__\n",
        "state": "ready",
        "source-date-epoch": 1743477819,
        "versions": [
            "4.7.3",
            "4.7.2",
            "4.7.0",
            "4.6.1",
            "4.6.0",
            "4.5.3",
            "4.5.2",
            "4.5.1",
            "4.5.0",
            "4.4.3",
            "4.4.2",
            "4.4.1",
            "4.4.0",
            "4.3.0",
            "4.2.5",
            "4.2.4",
            "4.2.3",
            "4.2.2",
            "4.2.1",
            "4.2.0",
            "4.1.3",
            "4.1.2",
            "4.1.1",
            "4.1.0",
            "4.0.1",
            "4.0.0",
            "3.1.0",
            "3.0.0",
            "2.1.3",
            "2.1.2",
            "2.1.1",
            "2.1.0",
            "2.0.1",
            "2.0.0",
            "1.1.1",
            "1.1.0",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Exasol",
                "external-doc-url": "https://docs.exasol.com/home.htm",
                "logo": "/docs/integration-logos/Exasol.png",
                "tags": ["software"],
            }
        ],
        "operators": [
            {"integration-name": "Exasol", "python-modules": ["airflow.providers.exasol.operators.exasol"]}
        ],
        "hooks": [
            {"integration-name": "Exasol", "python-modules": ["airflow.providers.exasol.hooks.exasol"]}
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.exasol.hooks.exasol.ExasolHook",
                "connection-type": "exasol",
            }
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "pyexasol>=0.5.1",
            "pandas>=2.1.2,<2.2",
        ],
        "devel-dependencies": [],
    }

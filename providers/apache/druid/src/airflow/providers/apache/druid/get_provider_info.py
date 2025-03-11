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
        "package-name": "apache-airflow-providers-apache-druid",
        "name": "Apache Druid",
        "description": "`Apache Druid <https://druid.apache.org/>`__.\n",
        "state": "ready",
        "source-date-epoch": 1741507815,
        "versions": [
            "4.1.0",
            "4.0.0",
            "3.12.1",
            "3.12.0",
            "3.11.0",
            "3.10.2",
            "3.10.1",
            "3.10.0",
            "3.9.0",
            "3.8.1",
            "3.8.0",
            "3.7.0",
            "3.6.0",
            "3.5.0",
            "3.4.2",
            "3.4.1",
            "3.4.0",
            "3.3.1",
            "3.3.0",
            "3.2.1",
            "3.2.0",
            "3.1.0",
            "3.0.0",
            "2.3.3",
            "2.3.2",
            "2.3.1",
            "2.3.0",
            "2.2.0",
            "2.1.0",
            "2.0.2",
            "2.0.1",
            "2.0.0",
            "1.1.0",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Apache Druid",
                "external-doc-url": "https://druid.apache.org/",
                "logo": "/docs/integration-logos/druid-1.png",
                "how-to-guide": ["/docs/apache-airflow-providers-apache-druid/operators.rst"],
                "tags": ["apache"],
            }
        ],
        "operators": [
            {
                "integration-name": "Apache Druid",
                "python-modules": ["airflow.providers.apache.druid.operators.druid"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Apache Druid",
                "python-modules": ["airflow.providers.apache.druid.hooks.druid"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.apache.druid.hooks.druid.DruidDbApiHook",
                "connection-type": "druid",
            }
        ],
        "transfers": [
            {
                "source-integration-name": "Apache Hive",
                "target-integration-name": "Apache Druid",
                "python-module": "airflow.providers.apache.druid.transfers.hive_to_druid",
            }
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "pydruid>=0.4.1",
        ],
        "optional-dependencies": {"apache.hive": ["apache-airflow-providers-apache-hive"]},
        "devel-dependencies": [],
    }

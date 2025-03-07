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
        "package-name": "apache-airflow-providers-apache-pinot",
        "name": "Apache Pinot",
        "description": "`Apache Pinot <https://pinot.apache.org/>`__\n",
        "state": "ready",
        "source-date-epoch": 1741121835,
        "versions": [
            "4.6.0",
            "4.5.1",
            "4.5.0",
            "4.4.2",
            "4.4.1",
            "4.4.0",
            "4.3.1",
            "4.3.0",
            "4.2.0",
            "4.1.4",
            "4.1.3",
            "4.1.2",
            "4.1.1",
            "4.1.0",
            "4.0.1",
            "4.0.0",
            "3.2.1",
            "3.2.0",
            "3.1.0",
            "3.0.0",
            "2.0.4",
            "2.0.3",
            "2.0.2",
            "2.0.1",
            "2.0.0",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Apache Pinot",
                "external-doc-url": "https://pinot.apache.org/",
                "logo": "/docs/integration-logos/pinot.png",
                "tags": ["apache"],
                "how-to-guide": ["/docs/apache-airflow-providers-apache-pinot/operators.rst"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Apache Pinot",
                "python-modules": ["airflow.providers.apache.pinot.hooks.pinot"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.apache.pinot.hooks.pinot.PinotDbApiHook",
                "connection-type": "pinot",
            },
            {
                "hook-class-name": "airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook",
                "connection-type": "pinot_admin",
            },
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "pinotdb>=5.1.0",
        ],
        "devel-dependencies": [],
    }

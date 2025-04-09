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
        "package-name": "apache-airflow-providers-apache-livy",
        "name": "Apache Livy",
        "description": "`Apache Livy <https://livy.apache.org/>`__\n",
        "integrations": [
            {
                "integration-name": "Apache Livy",
                "external-doc-url": "https://livy.apache.org/",
                "how-to-guide": ["/docs/apache-airflow-providers-apache-livy/operators.rst"],
                "logo": "/docs/integration-logos/Livy.png",
                "tags": ["apache"],
            }
        ],
        "operators": [
            {
                "integration-name": "Apache Livy",
                "python-modules": ["airflow.providers.apache.livy.operators.livy"],
            }
        ],
        "sensors": [
            {
                "integration-name": "Apache Livy",
                "python-modules": ["airflow.providers.apache.livy.sensors.livy"],
            }
        ],
        "hooks": [
            {
                "integration-name": "Apache Livy",
                "python-modules": ["airflow.providers.apache.livy.hooks.livy"],
            }
        ],
        "triggers": [
            {
                "integration-name": "Apache Livy",
                "python-modules": ["airflow.providers.apache.livy.triggers.livy"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.apache.livy.hooks.livy.LivyHook",
                "connection-type": "livy",
            }
        ],
    }

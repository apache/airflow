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
        "package-name": "apache-airflow-providers-dbt-cloud",
        "name": "dbt Cloud",
        "description": "`dbt Cloud <https://www.getdbt.com/product/dbt-cloud/>`__\n",
        "state": "ready",
        "source-date-epoch": 1740734116,
        "versions": [
            "4.2.0",
            "4.0.0",
            "3.11.2",
            "3.11.1",
            "3.11.0",
            "3.10.1",
            "3.10.0",
            "3.9.0",
            "3.8.1",
            "3.8.0",
            "3.7.1",
            "3.7.0",
            "3.6.1",
            "3.6.0",
            "3.5.1",
            "3.5.0",
            "3.4.1",
            "3.4.0",
            "3.3.0",
            "3.2.3",
            "3.2.2",
            "3.2.1",
            "3.2.0",
            "3.1.1",
            "3.1.0",
            "3.0.0",
            "2.3.1",
            "2.3.0",
            "2.2.0",
            "2.1.0",
            "2.0.1",
            "2.0.0",
            "1.0.2",
            "1.0.1",
        ],
        "integrations": [
            {
                "integration-name": "dbt Cloud",
                "external-doc-url": "https://docs.getdbt.com/docs/dbt-cloud/cloud-overview",
                "logo": "/docs/integration-logos/dbt.png",
                "how-to-guide": ["/docs/apache-airflow-providers-dbt-cloud/operators.rst"],
                "tags": ["dbt"],
            }
        ],
        "operators": [
            {"integration-name": "dbt Cloud", "python-modules": ["airflow.providers.dbt.cloud.operators.dbt"]}
        ],
        "sensors": [
            {"integration-name": "dbt Cloud", "python-modules": ["airflow.providers.dbt.cloud.sensors.dbt"]}
        ],
        "hooks": [
            {"integration-name": "dbt Cloud", "python-modules": ["airflow.providers.dbt.cloud.hooks.dbt"]}
        ],
        "triggers": [
            {"integration-name": "dbt Cloud", "python-modules": ["airflow.providers.dbt.cloud.triggers.dbt"]}
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook",
                "connection-type": "dbt_cloud",
            }
        ],
        "extra-links": ["airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperatorLink"],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-http",
            "asgiref>=2.3.0",
            "aiohttp>=3.9.2",
        ],
        "optional-dependencies": {"openlineage": ["apache-airflow-providers-openlineage>=1.7.0"]},
    }

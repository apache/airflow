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
        "package-name": "apache-airflow-providers-snowflake",
        "name": "Snowflake",
        "description": "`Snowflake <https://www.snowflake.com/>`__\n",
        "state": "ready",
        "source-date-epoch": 1741509691,
        "versions": [
            "6.1.1",
            "6.1.0",
            "6.0.0",
            "5.8.1",
            "5.8.0",
            "5.7.1",
            "5.7.0",
            "5.6.1",
            "5.6.0",
            "5.5.2",
            "5.5.1",
            "5.5.0",
            "5.4.0",
            "5.3.1",
            "5.3.0",
            "5.2.1",
            "5.2.0",
            "5.1.2",
            "5.1.1",
            "5.1.0",
            "5.0.1",
            "5.0.0",
            "4.4.2",
            "4.4.1",
            "4.4.0",
            "4.3.1",
            "4.3.0",
            "4.2.0",
            "4.1.0",
            "4.0.5",
            "4.0.4",
            "4.0.3",
            "4.0.2",
            "4.0.1",
            "4.0.0",
            "3.3.0",
            "3.2.0",
            "3.1.0",
            "3.0.0",
            "2.7.0",
            "2.6.0",
            "2.5.2",
            "2.5.1",
            "2.5.0",
            "2.4.0",
            "2.3.1",
            "2.3.0",
            "2.2.0",
            "2.1.1",
            "2.1.0",
            "2.0.0",
            "1.3.0",
            "1.2.0",
            "1.1.1",
            "1.1.0",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Snowflake",
                "external-doc-url": "https://snowflake.com/",
                "how-to-guide": [
                    "/docs/apache-airflow-providers-snowflake/operators/snowflake.rst",
                    "/docs/apache-airflow-providers-snowflake/operators/snowpark.rst",
                ],
                "logo": "/docs/integration-logos/Snowflake.png",
                "tags": ["service"],
            }
        ],
        "operators": [
            {
                "integration-name": "Snowflake",
                "python-modules": [
                    "airflow.providers.snowflake.operators.snowflake",
                    "airflow.providers.snowflake.operators.snowpark",
                ],
            }
        ],
        "task-decorators": [
            {
                "class-name": "airflow.providers.snowflake.decorators.snowpark.snowpark_task",
                "name": "snowpark",
            }
        ],
        "hooks": [
            {
                "integration-name": "Snowflake",
                "python-modules": [
                    "airflow.providers.snowflake.hooks.snowflake",
                    "airflow.providers.snowflake.hooks.snowflake_sql_api",
                ],
            }
        ],
        "transfers": [
            {
                "source-integration-name": "Amazon Simple Storage Service (S3)",
                "target-integration-name": "Snowflake",
                "python-module": "airflow.providers.snowflake.transfers.copy_into_snowflake",
                "how-to-guide": "/docs/apache-airflow-providers-snowflake/operators/copy_into_snowflake.rst",
            },
            {
                "source-integration-name": "Google Cloud Storage (GCS)",
                "target-integration-name": "Snowflake",
                "python-module": "airflow.providers.snowflake.transfers.copy_into_snowflake",
            },
            {
                "source-integration-name": "Microsoft Azure Blob Storage",
                "target-integration-name": "Snowflake",
                "python-module": "airflow.providers.snowflake.transfers.copy_into_snowflake",
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook",
                "connection-type": "snowflake",
            }
        ],
        "triggers": [
            {
                "integration-name": "Snowflake",
                "python-modules": ["airflow.providers.snowflake.triggers.snowflake_trigger"],
            }
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-compat>=1.6.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "pandas>=2.1.2,<2.2",
            "pyarrow>=14.0.1",
            "snowflake-connector-python>=3.7.1",
            "snowflake-sqlalchemy>=1.4.0",
            "snowflake-snowpark-python>=1.17.0;python_version<'3.12'",
        ],
        "optional-dependencies": {"openlineage": ["apache-airflow-providers-openlineage"]},
        "devel-dependencies": [],
    }

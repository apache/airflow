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
        "package-name": "apache-airflow-providers-postgres",
        "name": "PostgreSQL",
        "description": "`PostgreSQL <https://www.postgresql.org/>`__\n",
        "state": "ready",
        "source-date-epoch": 1734536300,
        "versions": [
            "6.0.0",
            "5.14.0",
            "5.13.1",
            "5.13.0",
            "5.12.0",
            "5.11.3",
            "5.11.2",
            "5.11.1",
            "5.11.0",
            "5.10.2",
            "5.10.1",
            "5.10.0",
            "5.9.0",
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
            "5.2.2",
            "5.2.1",
            "5.2.0",
            "5.1.0",
            "5.0.0",
            "4.1.0",
            "4.0.1",
            "4.0.0",
            "3.0.0",
            "2.4.0",
            "2.3.0",
            "2.2.0",
            "2.1.0",
            "2.0.0",
            "1.0.2",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "PostgreSQL",
                "external-doc-url": "https://www.postgresql.org/",
                "how-to-guide": ["/docs/apache-airflow-providers-postgres/operators.rst"],
                "logo": "/docs/integration-logos/Postgres.png",
                "tags": ["software"],
            }
        ],
        "dialects": [
            {
                "dialect-type": "postgresql",
                "dialect-class-name": "airflow.providers.postgres.dialects.postgres.PostgresDialect",
            }
        ],
        "hooks": [
            {
                "integration-name": "PostgreSQL",
                "python-modules": ["airflow.providers.postgres.hooks.postgres"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.postgres.hooks.postgres.PostgresHook",
                "connection-type": "postgres",
            }
        ],
        "asset-uris": [
            {
                "schemes": ["postgres", "postgresql"],
                "handler": "airflow.providers.postgres.assets.postgres.sanitize_uri",
            }
        ],
        "dataset-uris": [
            {
                "schemes": ["postgres", "postgresql"],
                "handler": "airflow.providers.postgres.assets.postgres.sanitize_uri",
            }
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "psycopg2-binary>=2.9.7",
            "asyncpg>=0.30.0",
        ],
        "optional-dependencies": {
            "amazon": ["apache-airflow-providers-amazon>=2.6.0"],
            "openlineage": ["apache-airflow-providers-openlineage"],
        },
    }

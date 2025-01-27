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
        "package-name": "apache-airflow-providers-teradata",
        "name": "Teradata",
        "description": "`Teradata <https://www.teradata.com/>`__\n",
        "state": "ready",
        "source-date-epoch": 1734537340,
        "versions": [
            "3.0.0",
            "2.6.1",
            "2.6.0",
            "2.5.0",
            "2.4.0",
            "2.3.0",
            "2.2.0",
            "2.1.1",
            "2.1.0",
            "2.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Teradata",
                "external-doc-url": "https://www.teradata.com/",
                "how-to-guide": [
                    "/docs/apache-airflow-providers-teradata/operators/teradata.rst",
                    "/docs/apache-airflow-providers-teradata/operators/compute_cluster.rst",
                ],
                "logo": "/docs/integration-logos/Teradata.png",
                "tags": ["software"],
            }
        ],
        "operators": [
            {
                "integration-name": "Teradata",
                "python-modules": [
                    "airflow.providers.teradata.operators.teradata",
                    "airflow.providers.teradata.operators.teradata_compute_cluster",
                ],
            }
        ],
        "hooks": [
            {"integration-name": "Teradata", "python-modules": ["airflow.providers.teradata.hooks.teradata"]}
        ],
        "transfers": [
            {
                "source-integration-name": "Teradata",
                "target-integration-name": "Teradata",
                "python-module": "airflow.providers.teradata.transfers.teradata_to_teradata",
                "how-to-guide": "/docs/apache-airflow-providers-teradata/operators/teradata_to_teradata.rst",
            },
            {
                "source-integration-name": "Microsoft Azure Blob Storage",
                "target-integration-name": "Teradata",
                "python-module": "airflow.providers.teradata.transfers.azure_blob_to_teradata",
                "how-to-guide": "/docs/apache-airflow-providers-teradata/operators/azure_blob_to_teradata.rst",
            },
            {
                "source-integration-name": "Amazon Simple Storage Service (S3)",
                "target-integration-name": "Teradata",
                "python-module": "airflow.providers.teradata.transfers.s3_to_teradata",
                "how-to-guide": "/docs/apache-airflow-providers-teradata/operators/s3_to_teradata.rst",
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.teradata.hooks.teradata.TeradataHook",
                "connection-type": "teradata",
            }
        ],
        "triggers": [
            {
                "integration-name": "Teradata",
                "python-modules": ["airflow.providers.teradata.triggers.teradata_compute_cluster"],
            }
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "apache-airflow-providers-common-sql>=1.20.0",
            "teradatasqlalchemy>=17.20.0.0",
            "teradatasql>=17.20.0.28",
        ],
        "optional-dependencies": {
            "microsoft.azure": ["apache-airflow-providers-microsoft-azure"],
            "amazon": ["apache-airflow-providers-amazon"],
        },
    }

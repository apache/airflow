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
        "package-name": "apache-airflow-providers-apache-hive",
        "name": "Apache Hive",
        "description": "`Apache Hive <https://hive.apache.org/>`__\n",
        "integrations": [
            {
                "integration-name": "Apache Hive",
                "external-doc-url": "https://hive.apache.org/",
                "how-to-guide": ["/docs/apache-airflow-providers-apache-hive/operators.rst"],
                "logo": "/docs/integration-logos/hive.png",
                "tags": ["apache"],
            }
        ],
        "operators": [
            {
                "integration-name": "Apache Hive",
                "python-modules": [
                    "airflow.providers.apache.hive.operators.hive",
                    "airflow.providers.apache.hive.operators.hive_stats",
                ],
            }
        ],
        "sensors": [
            {
                "integration-name": "Apache Hive",
                "python-modules": [
                    "airflow.providers.apache.hive.sensors.hive_partition",
                    "airflow.providers.apache.hive.sensors.metastore_partition",
                    "airflow.providers.apache.hive.sensors.named_hive_partition",
                ],
            }
        ],
        "hooks": [
            {
                "integration-name": "Apache Hive",
                "python-modules": ["airflow.providers.apache.hive.hooks.hive"],
            }
        ],
        "transfers": [
            {
                "source-integration-name": "Vertica",
                "target-integration-name": "Apache Hive",
                "python-module": "airflow.providers.apache.hive.transfers.vertica_to_hive",
            },
            {
                "source-integration-name": "Apache Hive",
                "target-integration-name": "MySQL",
                "python-module": "airflow.providers.apache.hive.transfers.hive_to_mysql",
            },
            {
                "source-integration-name": "Apache Hive",
                "target-integration-name": "Samba",
                "python-module": "airflow.providers.apache.hive.transfers.hive_to_samba",
            },
            {
                "source-integration-name": "Amazon Simple Storage Service (S3)",
                "target-integration-name": "Apache Hive",
                "python-module": "airflow.providers.apache.hive.transfers.s3_to_hive",
            },
            {
                "source-integration-name": "MySQL",
                "target-integration-name": "Apache Hive",
                "python-module": "airflow.providers.apache.hive.transfers.mysql_to_hive",
            },
            {
                "source-integration-name": "Microsoft SQL Server (MSSQL)",
                "target-integration-name": "Apache Hive",
                "python-module": "airflow.providers.apache.hive.transfers.mssql_to_hive",
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.apache.hive.hooks.hive.HiveCliHook",
                "connection-type": "hive_cli",
            },
            {
                "hook-class-name": "airflow.providers.apache.hive.hooks.hive.HiveServer2Hook",
                "connection-type": "hiveserver2",
            },
            {
                "hook-class-name": "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook",
                "connection-type": "hive_metastore",
            },
        ],
        "plugins": [
            {"name": "hive", "plugin-class": "airflow.providers.apache.hive.plugins.hive.HivePlugin"}
        ],
        "config": {
            "hive": {
                "description": None,
                "options": {
                    "default_hive_mapred_queue": {
                        "description": "Default mapreduce queue for HiveOperator tasks\n",
                        "version_added": None,
                        "type": "string",
                        "example": None,
                        "default": "",
                    },
                    "mapred_job_name_template": {
                        "description": "Template for mapred_job_name in HiveOperator, supports the following named parameters\nhostname, dag_id, task_id, execution_date\n",
                        "version_added": None,
                        "type": "string",
                        "example": None,
                        "default": None,
                    },
                },
            }
        },
    }

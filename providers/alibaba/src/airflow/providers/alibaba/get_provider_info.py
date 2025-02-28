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
        "package-name": "apache-airflow-providers-alibaba",
        "name": "Alibaba",
        "description": "Alibaba Cloud integration (including `Alibaba Cloud <https://www.alibabacloud.com/>`__).\n",
        "state": "ready",
        "source-date-epoch": 1740734069,
        "versions": [
            "3.0.0",
            "2.9.1",
            "2.9.0",
            "2.8.1",
            "2.8.0",
            "2.7.3",
            "2.7.2",
            "2.7.1",
            "2.7.0",
            "2.6.0",
            "2.5.3",
            "2.5.2",
            "2.5.1",
            "2.5.0",
            "2.4.1",
            "2.4.0",
            "2.3.0",
            "2.2.0",
            "2.1.0",
            "2.0.1",
            "2.0.0",
            "1.1.1",
            "1.1.0",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Alibaba Cloud OSS",
                "external-doc-url": "https://www.alibabacloud.com/help/product/31815.htm",
                "logo": "/docs/integration-logos/alibabacloud-oss.png",
                "how-to-guide": ["/docs/apache-airflow-providers-alibaba/operators/oss.rst"],
                "tags": ["alibaba"],
            },
            {
                "integration-name": "Alibaba Cloud AnalyticDB Spark",
                "external-doc-url": "https://www.alibabacloud.com/help/en/analyticdb-for-mysql/latest/spark-developerment",
                "how-to-guide": ["/docs/apache-airflow-providers-alibaba/operators/analyticdb_spark.rst"],
                "tags": ["alibaba"],
            },
        ],
        "operators": [
            {
                "integration-name": "Alibaba Cloud OSS",
                "python-modules": ["airflow.providers.alibaba.cloud.operators.oss"],
            },
            {
                "integration-name": "Alibaba Cloud AnalyticDB Spark",
                "python-modules": ["airflow.providers.alibaba.cloud.operators.analyticdb_spark"],
            },
        ],
        "sensors": [
            {
                "integration-name": "Alibaba Cloud OSS",
                "python-modules": ["airflow.providers.alibaba.cloud.sensors.oss_key"],
            },
            {
                "integration-name": "Alibaba Cloud AnalyticDB Spark",
                "python-modules": ["airflow.providers.alibaba.cloud.sensors.analyticdb_spark"],
            },
        ],
        "hooks": [
            {
                "integration-name": "Alibaba Cloud OSS",
                "python-modules": ["airflow.providers.alibaba.cloud.hooks.oss"],
            },
            {
                "integration-name": "Alibaba Cloud AnalyticDB Spark",
                "python-modules": ["airflow.providers.alibaba.cloud.hooks.analyticdb_spark"],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.alibaba.cloud.hooks.oss.OSSHook",
                "connection-type": "oss",
            },
            {
                "hook-class-name": "airflow.providers.alibaba.cloud.hooks.analyticdb_spark.AnalyticDBSparkHook",
                "connection-type": "adb_spark",
            },
        ],
        "logging": ["airflow.providers.alibaba.cloud.log.oss_task_handler.OSSTaskHandler"],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "oss2>=2.14.0",
            "alibabacloud_adb20211201>=1.0.0",
            "alibabacloud_tea_openapi>=0.3.7",
        ],
    }

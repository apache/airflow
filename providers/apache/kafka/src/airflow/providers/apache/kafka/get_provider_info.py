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
        "package-name": "apache-airflow-providers-apache-kafka",
        "name": "Apache Kafka",
        "state": "ready",
        "source-date-epoch": 1742979371,
        "description": "`Apache Kafka  <https://kafka.apache.org/>`__\n",
        "versions": [
            "1.8.0",
            "1.7.0",
            "1.6.1",
            "1.6.0",
            "1.5.0",
            "1.4.1",
            "1.4.0",
            "1.3.1",
            "1.3.0",
            "1.2.0",
            "1.1.2",
            "1.1.1",
            "1.1.0",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Apache Kafka",
                "external-doc-url": "https://kafka.apache.org/",
                "logo": "/docs/integration-logos/kafka.svg",
                "tags": ["apache"],
            }
        ],
        "operators": [
            {
                "integration-name": "Apache Kafka",
                "python-modules": [
                    "airflow.providers.apache.kafka.operators.consume",
                    "airflow.providers.apache.kafka.operators.produce",
                ],
            }
        ],
        "hooks": [
            {
                "integration-name": "Apache Kafka",
                "python-modules": [
                    "airflow.providers.apache.kafka.hooks.base",
                    "airflow.providers.apache.kafka.hooks.client",
                    "airflow.providers.apache.kafka.hooks.consume",
                    "airflow.providers.apache.kafka.hooks.produce",
                ],
            }
        ],
        "sensors": [
            {
                "integration-name": "Apache Kafka",
                "python-modules": ["airflow.providers.apache.kafka.sensors.kafka"],
            }
        ],
        "triggers": [
            {
                "integration-name": "Apache Kafka",
                "python-modules": ["airflow.providers.apache.kafka.triggers.await_message"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.apache.kafka.hooks.base.KafkaBaseHook",
                "connection-type": "kafka",
            }
        ],
        "dependencies": ["apache-airflow>=2.9.0", "asgiref>=2.3.0", "confluent-kafka>=2.3.0"],
        "optional-dependencies": {"google": ["apache-airflow-providers-google"]},
        "devel-dependencies": [],
    }

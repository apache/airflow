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
        "package-name": "apache-airflow-providers-influxdb",
        "name": "Influxdb",
        "description": "`InfluxDB <https://www.influxdata.com/>`__\n",
        "state": "ready",
        "source-date-epoch": 1743477836,
        "versions": [
            "2.8.3",
            "2.8.2",
            "2.8.0",
            "2.7.1",
            "2.7.0",
            "2.6.0",
            "2.5.1",
            "2.5.0",
            "2.4.1",
            "2.4.0",
            "2.3.0",
            "2.2.3",
            "2.2.2",
            "2.2.1",
            "2.2.0",
            "2.1.0",
            "2.0.0",
            "1.1.3",
            "1.1.2",
            "1.1.1",
            "1.1.0",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Influxdb",
                "external-doc-url": "https://www.influxdata.com/",
                "tags": ["software"],
            }
        ],
        "hooks": [
            {"integration-name": "Influxdb", "python-modules": ["airflow.providers.influxdb.hooks.influxdb"]}
        ],
        "operators": [
            {
                "integration-name": "Influxdb",
                "python-modules": ["airflow.providers.influxdb.operators.influxdb"],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.influxdb.hooks.influxdb.InfluxDBHook",
                "connection-type": "influxdb",
            }
        ],
        "dependencies": ["apache-airflow>=2.9.0", "influxdb-client>=1.19.0", "requests>=2.31.0,<3"],
        "devel-dependencies": [],
    }

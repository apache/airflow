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
        "package-name": "apache-airflow-providers-mongo",
        "name": "MongoDB",
        "description": "`MongoDB <https://www.mongodb.com/>`__\n",
        "state": "ready",
        "source-date-epoch": 1734535493,
        "versions": [
            "5.0.0",
            "4.2.2",
            "4.2.1",
            "4.2.0",
            "4.1.2",
            "4.1.1",
            "4.1.0",
            "4.0.0",
            "3.6.0",
            "3.5.0",
            "3.4.0",
            "3.3.0",
            "3.2.2",
            "3.2.1",
            "3.2.0",
            "3.1.1",
            "3.1.0",
            "3.0.0",
            "2.3.3",
            "2.3.2",
            "2.3.1",
            "2.3.0",
            "2.2.0",
            "2.1.0",
            "2.0.0",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "MongoDB",
                "external-doc-url": "https://www.mongodb.com/",
                "logo": "/docs/integration-logos/MongoDB.png",
                "tags": ["software"],
            }
        ],
        "sensors": [
            {"integration-name": "MongoDB", "python-modules": ["airflow.providers.mongo.sensors.mongo"]}
        ],
        "hooks": [{"integration-name": "MongoDB", "python-modules": ["airflow.providers.mongo.hooks.mongo"]}],
        "connection-types": [
            {"hook-class-name": "airflow.providers.mongo.hooks.mongo.MongoHook", "connection-type": "mongo"}
        ],
        "dependencies": ["apache-airflow>=2.9.0", "dnspython>=1.13.0", "pymongo>=4.0.0,!=4.11"],
        "devel-dependencies": ["mongomock>=4.0.0"],
    }

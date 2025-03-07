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
        "package-name": "apache-airflow-providers-docker",
        "name": "Docker",
        "description": "`Docker <https://www.docker.com/>`__\n",
        "state": "ready",
        "source-date-epoch": 1741121866,
        "versions": [
            "4.2.0",
            "4.0.0",
            "3.14.1",
            "3.14.0",
            "3.13.0",
            "3.12.3",
            "3.12.2",
            "3.12.1",
            "3.12.0",
            "3.11.0",
            "3.10.0",
            "3.9.2",
            "3.9.1",
            "3.9.0",
            "3.8.2",
            "3.8.1",
            "3.8.0",
            "3.7.5",
            "3.7.4",
            "3.7.3",
            "3.7.2",
            "3.7.1",
            "3.7.0",
            "3.6.0",
            "3.5.1",
            "3.5.0",
            "3.4.0",
            "3.3.0",
            "3.2.0",
            "3.1.0",
            "3.0.0",
            "2.7.0",
            "2.6.0",
            "2.5.2",
            "2.5.1",
            "2.5.0",
            "2.4.1",
            "2.4.0",
            "2.3.0",
            "2.2.0",
            "2.1.1",
            "2.1.0",
            "2.0.0",
            "1.2.0",
            "1.1.0",
            "1.0.2",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Docker",
                "external-doc-url": "https://docs.docker.com/",
                "logo": "/docs/integration-logos/Docker.png",
                "tags": ["software"],
            },
            {
                "integration-name": "Docker Swarm",
                "external-doc-url": "https://docs.docker.com/engine/swarm/",
                "logo": "/docs/integration-logos/Docker-Swarm.png",
                "tags": ["software"],
            },
        ],
        "operators": [
            {"integration-name": "Docker", "python-modules": ["airflow.providers.docker.operators.docker"]},
            {
                "integration-name": "Docker Swarm",
                "python-modules": ["airflow.providers.docker.operators.docker_swarm"],
            },
        ],
        "hooks": [
            {"integration-name": "Docker", "python-modules": ["airflow.providers.docker.hooks.docker"]}
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.docker.hooks.docker.DockerHook",
                "connection-type": "docker",
            }
        ],
        "task-decorators": [
            {"class-name": "airflow.providers.docker.decorators.docker.docker_task", "name": "docker"}
        ],
        "dependencies": ["apache-airflow>=2.9.0", "docker>=7.1.0", "python-dotenv>=0.21.0"],
        "optional-dependencies": {"common.compat": ["apache-airflow-providers-common-compat"]},
        "devel-dependencies": [],
    }

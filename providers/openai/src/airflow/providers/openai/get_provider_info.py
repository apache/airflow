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
        "package-name": "apache-airflow-providers-openai",
        "name": "OpenAI",
        "description": "`OpenAI <https://platform.openai.com/docs/introduction>`__\n",
        "state": "ready",
        "source-date-epoch": 1742980979,
        "versions": [
            "1.5.3",
            "1.5.2",
            "1.5.1",
            "1.5.0",
            "1.4.0",
            "1.3.0",
            "1.2.2",
            "1.2.1",
            "1.2.0",
            "1.1.0",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "OpenAI",
                "external-doc-url": "https://platform.openai.com/docs/introduction",
                "how-to-guide": ["/docs/apache-airflow-providers-openai/operators/openai.rst"],
                "tags": ["software"],
            }
        ],
        "hooks": [
            {"integration-name": "OpenAI", "python-modules": ["airflow.providers.openai.hooks.openai"]}
        ],
        "operators": [
            {"integration-name": "OpenAI", "python-modules": ["airflow.providers.openai.operators.openai"]}
        ],
        "triggers": [
            {"integration-name": "OpenAI", "python-modules": ["airflow.providers.openai.triggers.openai"]}
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.openai.hooks.openai.OpenAIHook",
                "connection-type": "openai",
            }
        ],
        "dependencies": ["apache-airflow>=2.9.0", "openai[datalib]>=1.66.0"],
        "devel-dependencies": [],
    }

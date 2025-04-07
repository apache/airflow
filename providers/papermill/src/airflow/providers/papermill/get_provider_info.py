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
        "package-name": "apache-airflow-providers-papermill",
        "name": "Papermill",
        "description": "`Papermill <https://github.com/nteract/papermill>`__\n",
        "state": "ready",
        "source-date-epoch": 1743836583,
        "versions": [
            "3.10.0",
            "3.9.2",
            "3.9.1",
            "3.9.0",
            "3.8.2",
            "3.8.1",
            "3.8.0",
            "3.7.2",
            "3.7.1",
            "3.7.0",
            "3.6.2",
            "3.6.1",
            "3.6.0",
            "3.5.0",
            "3.4.0",
            "3.2.1",
            "3.2.0",
            "3.1.1",
            "3.1.0",
            "3.0.0",
            "2.2.3",
            "2.2.2",
            "2.2.1",
            "2.2.0",
            "2.1.0",
            "2.0.1",
            "2.0.0",
            "1.0.2",
            "1.0.1",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Papermill",
                "external-doc-url": "https://github.com/nteract/papermill",
                "how-to-guide": ["/docs/apache-airflow-providers-papermill/operators.rst"],
                "logo": "/docs/integration-logos/Papermill.png",
                "tags": ["software"],
            }
        ],
        "operators": [
            {
                "integration-name": "Papermill",
                "python-modules": ["airflow.providers.papermill.operators.papermill"],
            }
        ],
        "hooks": [
            {"integration-name": "Papermill", "python-modules": ["airflow.providers.papermill.hooks.kernel"]}
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.papermill.hooks.kernel.KernelHook",
                "connection-type": "jupyter_kernel",
            }
        ],
        "dependencies": [
            "apache-airflow>=2.9.0",
            "papermill[all]>=2.6.0",
            "scrapbook[all]>=0.5.0",
            "ipykernel>=6.29.4",
            "pandas>=2.1.2,<2.2",
            "nbconvert>=7.16.1",
        ],
        "optional-dependencies": {"common.compat": ["apache-airflow-providers-common-compat"]},
        "devel-dependencies": [],
    }

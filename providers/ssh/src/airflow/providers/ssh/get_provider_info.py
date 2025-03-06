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
        "package-name": "apache-airflow-providers-ssh",
        "name": "SSH",
        "description": "`Secure Shell (SSH) <https://tools.ietf.org/html/rfc4251>`__\n",
        "state": "ready",
        "source-date-epoch": 1741121951,
        "versions": [
            "4.0.0",
            "3.14.0",
            "3.13.1",
            "3.13.0",
            "3.12.0",
            "3.11.2",
            "3.11.1",
            "3.11.0",
            "3.10.1",
            "3.10.0",
            "3.9.0",
            "3.8.1",
            "3.8.0",
            "3.7.3",
            "3.7.2",
            "3.7.1",
            "3.7.0",
            "3.6.0",
            "3.5.0",
            "3.4.0",
            "3.3.0",
            "3.2.0",
            "3.1.0",
            "3.0.0",
            "2.4.4",
            "2.4.3",
            "2.4.2",
            "2.4.1",
            "2.4.0",
            "2.3.0",
            "2.2.0",
            "2.1.1",
            "2.1.0",
            "2.0.0",
            "1.3.0",
            "1.2.0",
            "1.1.0",
            "1.0.0",
        ],
        "integrations": [
            {
                "integration-name": "Secure Shell (SSH)",
                "external-doc-url": "https://tools.ietf.org/html/rfc4251",
                "logo": "/docs/integration-logos/SSH.png",
                "tags": ["protocol"],
            }
        ],
        "operators": [
            {
                "integration-name": "Secure Shell (SSH)",
                "python-modules": ["airflow.providers.ssh.operators.ssh"],
            }
        ],
        "hooks": [
            {"integration-name": "Secure Shell (SSH)", "python-modules": ["airflow.providers.ssh.hooks.ssh"]}
        ],
        "connection-types": [
            {"hook-class-name": "airflow.providers.ssh.hooks.ssh.SSHHook", "connection-type": "ssh"}
        ],
        "dependencies": ["apache-airflow>=2.9.0", "paramiko>=2.9.0", "sshtunnel>=0.3.2"],
        "devel-dependencies": [],
    }

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
"""Config sub-commands using API client instead of direct database access."""

from __future__ import annotations

from airflow.exceptions import AirflowConfigException
from airflowctl.api.client import Client, provide_api_client


@provide_api_client
def show_config(args):
    """Show current application configuration."""
    pass


@provide_api_client
def get_value(args, cli_api_client: Client):
    """Get one value from configuration."""
    try:
        response = cli_api_client.configs.get(section=args.section, option=args.option)
        if response and "value" in response:
            print(response["value"])
    except AirflowConfigException:
        pass


@provide_api_client
def lint_config(args) -> None:
    """Lint the airflow.cfg file for removed, or renamed configurations."""
    pass


@provide_api_client
def update_config(args) -> None:
    """Update the airflow.cfg file to migrate configuration changes from Airflow 2.x to Airflow 3."""
    pass

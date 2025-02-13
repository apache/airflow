#
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

from __future__ import annotations

import os
import sys

import rich

from airflow.cli.api.cli_api_client import Credentials
from airflow.utils import cli as cli_utils


@cli_utils.action_cli
def login(args) -> None:
    """Login to a provider."""
    if not args.api_token and not os.environ.get("APACHE_AIRFLOW_CLI_TOKEN"):
        # Exit
        rich.print("[red]No token found.")
        rich.print(
            "[green]Please pass: [blue]--api-token or set APACHE_AIRFLOW_CLI_TOKEN environment variable to login."
        )
        sys.exit(1)
    Credentials(
        api_url=args.api_url,
        api_token=args.api_token or os.getenv("APACHE_AIRFLOW_CLI_TOKEN"),
        api_environment=args.env,
    ).save()

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

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, Credentials, provide_api_client
from airflowctl.api.datamodels.auth_generated import LoginBody


@provide_api_client(kind=ClientKind.AUTH)
def login(args, api_client=NEW_API_CLIENT) -> None:
    """Login to a provider."""
    success_message = "[green]Login successful! Welcome to airflowctl![/green]"
    # Check is username and password are passed
    if args.username and args.password:
        # Generate empty credentials with the api_url and env
        credentials = Credentials(
            api_url=args.api_url,
            api_token="",
            api_environment=args.env,
            client_kind=ClientKind.AUTH,
        )
        # After logging in, the token will be saved in the credentials file
        try:
            credentials.save()
            api_client.refresh_base_url(base_url=args.api_url, kind=ClientKind.AUTH)
            login_response = api_client.login.login_with_username_and_password(
                LoginBody(
                    username=args.username,
                    password=args.password,
                )
            )
            credentials.api_token = login_response.access_token
            credentials.save()
            rich.print(success_message)
            return
        except Exception as e:
            rich.print(f"[red]Login failed: {e}")
            sys.exit(1)

    # Check if token is passed or environment variable is set
    if not (token := args.api_token or os.environ.get("AIRFLOW_CLI_TOKEN")):
        # Exit
        rich.print("[red]No token found.")
        rich.print(
            "[green]Please pass:[/green] [blue]--api-token[/blue] or set "
            "[blue]AIRFLOW_CLI_TOKEN[/blue] environment variable to login."
            "[blue] Alternatively, you can use --username and --password to login.[/blue]"
        )
        sys.exit(1)

    Credentials(
        api_url=args.api_url,
        api_token=token,
        api_environment=args.env,
    ).save()
    rich.print(success_message)

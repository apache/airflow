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

import glob
import json
import os
import sys

import keyring
import rich
from keyring.errors import NoKeyringError

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, Credentials, provide_api_client
from airflowctl.api.datamodels.auth_generated import LoginBody
from airflowctl.ctl.console_formatting import AirflowConsole


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


def list_envs(args) -> None:
    """List all CLI environments that the user has logged into."""
    # Get AIRFLOW_HOME
    airflow_home = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))

    # Check if directory exists
    if not os.path.isdir(airflow_home):
        rich.print(f"[yellow]No AIRFLOW_HOME directory found at {airflow_home}[/yellow]")
        AirflowConsole().print_as(data=[], output=args.output)
        return

    # Find all .json files
    config_files = glob.glob(os.path.join(airflow_home, "*.json"))

    environments = []

    for config_path in config_files:
        filename = os.path.basename(config_path)

        # Skip non-environment config files
        if filename.startswith("debug_creds_") or filename.endswith("_generated.json"):
            continue

        env_name = filename.replace(".json", "")

        # Try to read config file
        api_url = None
        config_status = "ok"

        try:
            with open(config_path) as f:
                config = json.load(f)
                api_url = config.get("api_url", "unknown")
        except (OSError, json.JSONDecodeError, KeyError) as e:
            config_status = f"config error: {str(e)[:50]}"
            api_url = "error reading config"

        # Try to get token from keyring
        token_status = "not authenticated"

        try:
            if os.getenv("AIRFLOW_CLI_DEBUG_MODE") == "true":
                # Check debug credentials file
                debug_path = os.path.join(airflow_home, f"debug_creds_{env_name}.json")
                if os.path.exists(debug_path):
                    with open(debug_path) as f:
                        debug_creds = json.load(f)
                        if f"api_token_{env_name}" in debug_creds:
                            token_status = "authenticated"
            else:
                # Check keyring
                token = keyring.get_password("airflowctl", f"api_token_{env_name}")
                if token:
                    token_status = "authenticated"
        except NoKeyringError:
            token_status = "keyring unavailable"
        except ValueError:
            # Incorrect keyring password
            token_status = "keyring error"
        except Exception as e:
            token_status = f"error: {str(e)[:30]}"

        # If config is corrupted, override token status
        if config_status != "ok":
            token_status = config_status

        environments.append(
            {
                "environment": env_name,
                "api_url": api_url,
                "status": token_status,
            }
        )

    # Sort by environment name
    environments.sort(key=lambda x: x.get("environment", ""))

    # Display results
    if not environments:
        rich.print(f"[yellow]No environments found in {airflow_home}[/yellow]")

    AirflowConsole().print_as(data=environments, output=args.output)

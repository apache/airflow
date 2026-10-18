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

import getpass
import glob
import json
import os
import secrets
import sys
import webbrowser
from datetime import UTC, datetime, timedelta

import keyring
import rich
from keyring.errors import NoKeyringError

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, Credentials, provide_api_client
from airflowctl.api.datamodels.auth_generated import LoginBody
from airflowctl.api.datamodels.generated import CliTokenBody
from airflowctl.ctl.console_formatting import AirflowConsole
from airflowctl.utils.browser_login import (
    BROWSER_LOGIN_TIMEOUT_SECONDS,
    MIN_SERVER_VERSION,
    LoopbackCallbackServer,
    build_authorize_url,
    generate_pkce_pair,
    get_confirmation_code,
    is_browser_login_supported,
    is_ssh_session,
)


@provide_api_client(kind=ClientKind.AUTH)
def login(args, api_client=NEW_API_CLIENT) -> None:
    """Login to a provider."""
    success_message = "[green]Login successful! Welcome to airflowctl![/green]"

    if args.web:
        expires_at = _login_with_browser(args, api_client)
        rich.print(success_message)
        rich.print(
            f"The token expires at {expires_at.astimezone():%Y-%m-%d %H:%M %Z}. "
            "Run this command again to log in after that."
        )
        return

    username = args.username
    password = args.password
    token = args.api_token or os.environ.get("AIRFLOW_CLI_TOKEN")

    # If credentials are incomplete, prompt interactively on a real terminal
    if not token and not (username and password):
        if not sys.stdin.isatty():
            rich.print("[red]No credentials provided.[/red]")
            rich.print(
                "[green]Please pass:[/green] [blue]--api-token[/blue] or set "
                "[blue]AIRFLOW_CLI_TOKEN[/blue] environment variable to login."
                "[blue] Alternatively, you can use --username and --password to login.[/blue]"
            )
            sys.exit(1)
        rich.print(f"[blue]Credentials for[/blue] [bold]{args.env}[/bold] [blue]({args.api_url})[/blue]")
        if not username:
            username = input(f"[{args.env}] Username: ")
        if not password:
            password = getpass.getpass(f"[{args.env}] Password: ")

    # Username + password login (from args or interactively prompted)
    if username and password:
        if args.skip_keyring:
            rich.print("[red]The --skip-keyring is not compatible with username and password login.")
            sys.exit(1)
        credentials = Credentials(
            api_url=args.api_url,
            api_token="",
            api_environment=args.env,
            client_kind=ClientKind.AUTH,
        )
        # After logging in, the token will be saved in the credentials file
        try:
            api_client.refresh_base_url(base_url=args.api_url, kind=ClientKind.AUTH)
            login_response = api_client.login.login_with_username_and_password(
                LoginBody(
                    username=username,
                    password=password,
                )
            )
            credentials.api_token = login_response.access_token
            credentials.save()
            rich.print(success_message)
            return
        except Exception as e:
            rich.print(f"[red]Login failed: {e}")
            sys.exit(1)

    # Token-based login
    if not token:
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
    ).save(args.skip_keyring)
    rich.print(success_message)


def _login_with_browser(args, api_client) -> datetime:
    """
    Log in by approving the request in the Airflow UI, then save the resulting token.

    Returns when the token expires. Exits with an explanation whenever the browser flow cannot work.
    """
    if args.api_token or args.username or args.password or args.skip_keyring:
        rich.print(
            "[red]--web cannot be combined with --api-token, --username, --password or --skip-keyring.[/red]"
        )
        sys.exit(1)
    if is_ssh_session():
        rich.print("[red]--web opens a browser on this machine, which is not possible over SSH.[/red]")
        _print_generate_token_fallback()
        sys.exit(1)

    api_client.refresh_base_url(base_url=args.api_url, kind=ClientKind.NO_AUTH)
    try:
        server_version = api_client.version.get().version
    except Exception as e:
        rich.print(f"[red]Could not reach the Airflow API at {args.api_url}: {e}[/red]")
        sys.exit(1)
    if not is_browser_login_supported(server_version):
        minimum = ".".join(str(part) for part in MIN_SERVER_VERSION)
        rich.print(
            f"[red]The Airflow API server runs {server_version}; logging in through the browser "
            f"needs Airflow {minimum} or later.[/red]"
        )
        _print_generate_token_fallback()
        sys.exit(1)

    code_verifier, code_challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(32)
    with LoopbackCallbackServer(expected_state=state) as server:
        authorize_url = build_authorize_url(
            args.api_url, port=server.port, state=state, code_challenge=code_challenge
        )
        rich.print(f"[blue]Opening[/blue] {authorize_url} [blue]in your browser.[/blue]")
        if not webbrowser.open(authorize_url):
            rich.print("[red]No browser could be opened on this machine.[/red]")
            _print_generate_token_fallback()
            sys.exit(1)
        rich.print(f"Confirmation code: [bold]{get_confirmation_code(code_challenge)}[/bold]")
        rich.print("Waiting for you to approve the login in the browser (Ctrl-C to cancel)...")
        try:
            result = server.wait_for_callback(timeout=BROWSER_LOGIN_TIMEOUT_SECONDS)
        except KeyboardInterrupt:
            rich.print("[red]Login cancelled.[/red]")
            sys.exit(1)
        port = server.port

    if result is None:
        rich.print(
            f"[red]The browser did not respond within {BROWSER_LOGIN_TIMEOUT_SECONDS // 60} minutes. "
            "Run the command again when you are ready to approve the login.[/red]"
        )
        sys.exit(1)
    if result.code is None:
        if result.error == "access_denied":
            rich.print("[red]Login denied in the browser.[/red]")
        else:
            rich.print(f"[red]Login failed in the browser: {result.error}[/red]")
        sys.exit(1)

    try:
        token_response = api_client.login.login_with_authorization_code(
            CliTokenBody(code=result.code, code_verifier=code_verifier, port=port)
        )
    except Exception as e:
        rich.print(f"[red]Login failed: {e}[/red]")
        sys.exit(1)

    Credentials(api_url=args.api_url, api_token=token_response.access_token, api_environment=args.env).save()
    return datetime.now(tz=UTC) + timedelta(seconds=token_response.expires_in_seconds)


def _print_generate_token_fallback() -> None:
    rich.print(
        "[green]Instead:[/green] open the Airflow UI, pick [bold]Generate Token[/bold] and "
        "[bold]CLI Token[/bold] in the user menu (Airflow 3.2.0 or later), then run "
        "[blue]airflowctl auth login --api-token <token>[/blue]."
    )


@provide_api_client(kind=ClientKind.AUTH)
def get_token(args, api_client=NEW_API_CLIENT) -> None:
    """Generate and print a JWT token for the given credentials to stdout."""
    username = args.username or input("Username: ")
    password = args.password or getpass.getpass("Password: ")

    try:
        api_client.refresh_base_url(base_url=args.api_url, kind=ClientKind.AUTH)
        login_response = api_client.login.login_with_username_and_password(
            LoginBody(username=username, password=password)
        )
        print(login_response.access_token)
    except Exception as e:
        rich.print(f"[red]Token generation failed: {e}[/red]")
        sys.exit(1)


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

        env_name, _ = os.path.splitext(filename)

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
                        if Credentials.token_key_for_environment(env_name) in debug_creds:
                            token_status = "authenticated"
            else:
                # Check keyring
                token = keyring.get_password("airflowctl", Credentials.token_key_for_environment(env_name))
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

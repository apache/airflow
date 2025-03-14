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
"""Providers sub-commands."""

from __future__ import annotations

import re

from airflow.cli.simple_table import AirflowConsole
from airflow.providers_manager import ProvidersManager
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

ERROR_IMPORTING_HOOK = "Error when importing hook!"


def _remove_rst_syntax(value: str) -> str:
    return re.sub("[`_<>]", "", value.strip(" \n."))


@suppress_logs_and_warning
@providers_configuration_loaded
def provider_get(args):
    """Get a provider info."""
    providers = ProvidersManager().providers
    if args.provider_name in providers:
        provider_version = providers[args.provider_name].version
        provider_info = providers[args.provider_name].data
        if args.full:
            provider_info["description"] = _remove_rst_syntax(provider_info["description"])
            AirflowConsole().print_as(
                data=[provider_info],
                output=args.output,
            )
        else:
            AirflowConsole().print_as(
                data=[{"Provider": args.provider_name, "Version": provider_version}], output=args.output
            )
    else:
        raise SystemExit(f"No such provider installed: {args.provider_name}")


@suppress_logs_and_warning
@providers_configuration_loaded
def providers_list(args):
    """List all providers at the command line."""
    AirflowConsole().print_as(
        data=list(ProvidersManager().providers.values()),
        output=args.output,
        mapper=lambda x: {
            "package_name": x.data["package-name"],
            "description": _remove_rst_syntax(x.data["description"]),
            "version": x.version,
        },
    )

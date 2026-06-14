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
"""Providers commands."""

from __future__ import annotations

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def get(args, api_client=NEW_API_CLIENT) -> None:
    """Get information about a single installed provider."""
    # No single-provider API endpoint exists, so filter the providers collection by name.
    providers = {provider.package_name: provider for provider in api_client.providers.list().providers}
    provider = providers.get(args.provider_name)
    if provider is None:
        raise SystemExit(f"No such provider installed: {args.provider_name}")

    if args.full:
        data = provider.model_dump(mode="json")
    else:
        data = {"package_name": provider.package_name, "version": provider.version}
    AirflowConsole().print_as(data=[data], output=args.output)

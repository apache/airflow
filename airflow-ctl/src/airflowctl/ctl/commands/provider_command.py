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
"""Command functions for installed Airflow providers."""

from __future__ import annotations

from typing import Any

from airflowctl.api.client import NEW_API_CLIENT, Client, ClientKind, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def get_provider(args, api_client: Client = NEW_API_CLIENT) -> dict[str, Any]:
    """Get information for an installed provider."""
    response = api_client.providers.get(provider_name=args.provider_name)
    if args.full:
        result = response.provider_info
    else:
        result = {"Provider": response.package_name, "Version": response.version}

    AirflowConsole().print_as(data=[result], output=args.output)
    return result

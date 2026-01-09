#!/usr/bin/env python
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

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from in_container_utils import console, generate_openapi_file, validate_openapi_file

from airflow.providers.edge3 import __file__ as EDGE_PATH
from airflow.providers.edge3.worker_api.app import create_edge_worker_api_app
from airflow.providers.fab.auth_manager.api_fastapi import __file__ as FAB_AUTHMGR_API_PATH
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from airflow.providers.keycloak.auth_manager import __file__ as KEYCLOAK_AUTHMGR_PATH
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers_manager import ProvidersManager

if TYPE_CHECKING:
    from fastapi import FastAPI


class ProviderDef(NamedTuple):
    openapi_spec_file: Path
    app: FastAPI | None
    prefix: str


sys.path.insert(0, str(Path(__file__).parent.resolve()))
ProvidersManager().initialize_providers_configuration()

PROVIDERS_DEFS = {
    "fab": ProviderDef(
        openapi_spec_file=Path(FAB_AUTHMGR_API_PATH).parent
        / "openapi"
        / "v2-fab-auth-manager-generated.yaml",
        app=FabAuthManager().get_fastapi_app(),
        prefix="/auth",
    ),
    "edge": ProviderDef(
        openapi_spec_file=Path(EDGE_PATH).parent / "worker_api" / "v2-edge-generated.yaml",
        app=create_edge_worker_api_app(),
        prefix="/edge_worker",
    ),
    "keycloak": ProviderDef(
        openapi_spec_file=Path(KEYCLOAK_AUTHMGR_PATH).parent
        / "openapi"
        / "v2-keycloak-auth-manager-generated.yaml",
        app=KeycloakAuthManager().get_fastapi_app(),
        prefix="/auth",
    ),
}


# Generate FAB auth manager openapi spec
def generate_openapi_specs(provider_name: str):
    provider_def = PROVIDERS_DEFS.get(provider_name)
    if provider_def is None:
        console.print(f"[red]Provider '{provider_name}' not found. Skipping OpenAPI spec generation.[/]")
        sys.exit(1)
    app = provider_def.app
    openapi_spec_file = provider_def.openapi_spec_file

    if app:
        generate_openapi_file(app=app, file_path=openapi_spec_file, prefix=provider_def.prefix)
        validate_openapi_file(openapi_spec_file)
    else:
        console.print(
            f"[red]Provider '{provider_name}' has no FastAPI app. Skipping OpenAPI spec generation.[/]"
        )
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate openapi-spec for the specified provider.")
    parser.add_argument(
        "provider",
        type=str,
        help="The name of the provider whose openapi-spec should be compiled.",
        choices=list(PROVIDERS_DEFS.keys()),
    )
    args = parser.parse_args()
    generate_openapi_specs(args.provider)

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
"""User sub-commands."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import boto3

from airflow.configuration import conf
from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.amazon.aws.auth_manager.constants import CONF_REGION_NAME_KEY, CONF_SECTION_NAME
from airflow.utils import cli as cli_utils

try:
    from airflow.utils.providers_configuration_loader import providers_configuration_loaded
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import avp_commands. This feature is only available in Airflow "
        "version >= 2.8.0 where Auth Managers are introduced."
    )

if TYPE_CHECKING:
    from botocore.client import BaseClient

log = logging.getLogger(__name__)


@cli_utils.action_cli
@providers_configuration_loaded
def init_avp(args):
    """Initialize Amazon Verified Permissions resources."""
    client = _get_client()

    # Create the policy store if needed
    policy_store_id, is_new_policy_store = _create_policy_store(client, args)

    if not is_new_policy_store:
        print(
            f"Since an existing policy store with description '{args.policy_store_description}' has been found in Amazon Verified Permissions, "
            "the CLI made no changes to this policy store for security reasons. "
            "Any modification to this policy store must be done manually.",
        )
    else:
        # Set the schema
        _set_schema(client, policy_store_id, args)

    if not args.dry_run:
        print(
            "Please set configs below in Airflow configuration."
        )
        print(f"AIRFLOW__AWS_AUTH_MANAGER__AVP_POLICY_STORE_ID={policy_store_id}")


@cli_utils.action_cli
@providers_configuration_loaded
def update_schema(args):
    """Update Amazon Verified Permissions policy store schema."""
    client = _get_client()
    _set_schema(client, args.policy_store_id, args)


def _get_client():
    """Return Amazon Verified Permissions client."""
    region_name = conf.get(CONF_SECTION_NAME, CONF_REGION_NAME_KEY)
    return boto3.client("verifiedpermissions", region_name=region_name)


def _create_policy_store(client: BaseClient, args) -> tuple[str | None, bool]:
    """
    Create if needed the policy store.

    This function returns two elements:
    - the policy store ID
    - whether the policy store ID returned refers to a newly created policy store.
    """
    paginator = client.get_paginator("list_policy_stores")
    pages = paginator.paginate()
    policy_stores = [application for page in pages for application in page["policyStores"]]
    existing_policy_stores = [
        policy_store
        for policy_store in policy_stores
        if policy_store.get("description") == args.policy_store_description
    ]

    if args.verbose:
        log.debug("Policy stores found: %s", policy_stores)
        log.debug("Existing policy stores found: %s", existing_policy_stores)

    if len(existing_policy_stores) > 0:
        print(
            f"There is already a policy store with description '{args.policy_store_description}' in Amazon Verified Permissions: '{existing_policy_stores[0]['policyStoreId']}'."
        )
        return existing_policy_stores[0]["policyStoreId"], False
    else:
        print(f"No policy store with description '{args.policy_store_description}' found, creating one.")
        if args.dry_run:
            print(
                f"Dry run, not creating the policy store with description '{args.policy_store_description}'."
            )
            return None, True

        response = client.create_policy_store(
            validationSettings={
                "mode": "STRICT",
            },
            description=args.policy_store_description,
        )
        if args.verbose:
            log.debug("Response from create_policy_store: %s", response)

        print(f"Policy store created: '{response['policyStoreId']}'")

        return response["policyStoreId"], True


def _set_schema(client: BaseClient, policy_store_id: str, args) -> None:
    """Set the policy store schema."""
    if args.dry_run:
        print(f"Dry run, not updating the schema of the policy store with ID '{policy_store_id}'.")
        return

    schema_path = Path(__file__).parents[1] / "avp" / "schema.json"
    with open(schema_path) as schema_file:
        response = client.put_schema(
            policyStoreId=policy_store_id,
            definition={
                "cedarJson": schema_file.read(),
            },
        )

        if args.verbose:
            log.debug("Response from put_schema: %s", response)

    print("Policy store schema updated.")

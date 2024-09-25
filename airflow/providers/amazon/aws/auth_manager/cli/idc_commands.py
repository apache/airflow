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

import logging
import sys
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

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
def init_idc(args):
    """Initialize AWS IAM Identity Center resources."""
    client = _get_client()

    # Create the instance if needed
    instance_arn = _create_instance(client, args)

    # Create the application if needed
    _create_application(client, instance_arn, args)

    if not args.dry_run:
        print("AWS IAM Identity Center resources created successfully.")


def _get_client():
    """Return AWS IAM Identity Center client."""
    region_name = conf.get(CONF_SECTION_NAME, CONF_REGION_NAME_KEY)
    return boto3.client("sso-admin", region_name=region_name)


def _create_instance(client: BaseClient, args) -> str | None:
    """Create if needed AWS IAM Identity Center instance."""
    instances = client.list_instances()

    if args.verbose:
        log.debug("Instances found: %s", instances)

    if len(instances["Instances"]) > 0:
        print(
            f"There is already an instance configured in AWS IAM Identity Center: '{instances['Instances'][0]['InstanceArn']}'. "
            "No need to create a new one."
        )
        return instances["Instances"][0]["InstanceArn"]
    else:
        print("No instance configured in AWS IAM Identity Center, creating one.")
        if args.dry_run:
            print("Dry run, not creating the instance.")
            return None

        response = client.create_instance(Name=args.instance_name)
        if args.verbose:
            log.debug("Response from create_instance: %s", response)

        print(f"Instance created: '{response['InstanceArn']}'")

        return response["InstanceArn"]


def _create_application(client: BaseClient, instance_arn: str | None, args) -> str | None:
    """Create if needed AWS IAM identity Center application."""
    paginator = client.get_paginator("list_applications")
    pages = paginator.paginate(InstanceArn=instance_arn or "")
    applications = [application for page in pages for application in page["Applications"]]
    existing_applications = [
        application for application in applications if application["Name"] == args.application_name
    ]

    if args.verbose:
        log.debug("Applications found: %s", applications)
        log.debug("Existing applications found: %s", existing_applications)

    if len(existing_applications) > 0:
        print(
            f"There is already an application named '{args.application_name}' in AWS IAM Identity Center: '{existing_applications[0]['ApplicationArn']}'. "
            "Using this application."
        )
        return existing_applications[0]["ApplicationArn"]
    else:
        print(f"No application named {args.application_name} found, creating one.")
        if args.dry_run:
            print("Dry run, not creating the application.")
            return None

        try:
            response = client.create_application(
                ApplicationProviderArn="arn:aws:sso::aws:applicationProvider/custom-saml",
                Description="Application automatically created through the Airflow CLI. This application is used to access Airflow environment.",
                InstanceArn=instance_arn,
                Name=args.application_name,
                PortalOptions={
                    "SignInOptions": {
                        "Origin": "IDENTITY_CENTER",
                    },
                    "Visibility": "ENABLED",
                },
                Status="ENABLED",
            )
            if args.verbose:
                log.debug("Response from create_application: %s", response)
        except ClientError as e:
            # This is needed because as of today, the create_application in AWS Identity Center does not support SAML application
            # Remove this part when it is supported
            if "is not supported for this action" in e.response["Error"]["Message"]:
                print(
                    "*************************************************************************\n"
                    "*                            ACTION REQUIRED                            *\n"
                    "* Creation of SAML applications is only supported in AWS console today. *\n"
                    "* Please create the application through the console.                    *\n"
                    "*************************************************************************\n"
                )
            sys.exit(1)

        print(f"Application created: '{response['ApplicationArn']}'")

        return response["ApplicationArn"]

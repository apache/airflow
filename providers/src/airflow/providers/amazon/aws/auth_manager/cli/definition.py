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

from airflow.cli.cli_config import (
    ActionCommand,
    Arg,
    lazy_load_command,
)

############
# # ARGS # #
############

ARG_VERBOSE = Arg(
    ("-v", "--verbose"), help="Make logging output more verbose", action="store_true"
)

ARG_DRY_RUN = Arg(
    ("--dry-run",),
    help="Perform a dry run",
    action="store_true",
)

# AWS IAM Identity Center
ARG_INSTANCE_NAME = Arg(
    ("--instance-name",), help="Instance name in Identity Center", default="Airflow"
)

ARG_APPLICATION_NAME = Arg(
    ("--application-name",), help="Application name in Identity Center", default="Airflow"
)


# Amazon Verified Permissions
ARG_POLICY_STORE_DESCRIPTION = Arg(
    ("--policy-store-description",), help="Policy store description", default="Airflow"
)
ARG_POLICY_STORE_ID = Arg(("--policy-store-id",), help="Policy store ID")


################
# # COMMANDS # #
################

AWS_AUTH_MANAGER_COMMANDS = (
    ActionCommand(
        name="init-avp",
        help="Initialize Amazon Verified resources to be used by AWS manager",
        func=lazy_load_command(
            "airflow.providers.amazon.aws.auth_manager.cli.avp_commands.init_avp"
        ),
        args=(ARG_POLICY_STORE_DESCRIPTION, ARG_DRY_RUN, ARG_VERBOSE),
    ),
    ActionCommand(
        name="update-avp-schema",
        help="Update Amazon Verified permissions policy store schema to the latest version in 'airflow/providers/amazon/aws/auth_manager/avp/schema.json'",
        func=lazy_load_command(
            "airflow.providers.amazon.aws.auth_manager.cli.avp_commands.update_schema"
        ),
        args=(ARG_POLICY_STORE_ID, ARG_DRY_RUN, ARG_VERBOSE),
    ),
)

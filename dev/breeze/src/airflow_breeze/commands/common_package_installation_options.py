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

import click

from airflow_breeze.branch_defaults import DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import ALLOWED_CONSTRAINTS_MODES_CI, ALLOWED_CONSTRAINTS_MODES_PROD
from airflow_breeze.utils.custom_param_types import BetterChoice

option_airflow_constraints_reference = click.option(
    "--airflow-constraints-reference",
    default=DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH,
    help="Constraint reference to use for airflow installation (used in calculated constraints URL).",
    envvar="AIRFLOW_CONSTRAINTS_REFERENCE",
)
option_airflow_constraints_location = click.option(
    "--airflow-constraints-location",
    type=str,
    help="Location of airflow constraints to use (remote URL or local context file).",
    envvar="AIRFLOW_CONSTRAINTS_LOCATION",
)
option_airflow_constraints_mode_update = click.option(
    "--airflow-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_CI),
    required=False,
    help="Limit constraint update to only selected constraint mode - if selected.",
)
option_airflow_constraints_mode_prod = click.option(
    "--airflow-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_PROD),
    default=ALLOWED_CONSTRAINTS_MODES_PROD[0],
    show_default=True,
    help="Mode of constraints for Airflow for PROD image building.",
)
option_airflow_constraints_mode_ci = click.option(
    "--airflow-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_CI),
    default=ALLOWED_CONSTRAINTS_MODES_CI[0],
    show_default=True,
    help="Mode of constraints for Airflow for CI image building.",
)
option_airflow_skip_constraints = click.option(
    "--airflow-skip-constraints",
    is_flag=True,
    help="Do not use constraints when installing airflow.",
    envvar="AIRFLOW_SKIP_CONSTRAINTS",
)
option_install_airflow_with_constraints = click.option(
    "--install-airflow-with-constraints/--no-install-airflow-with-constraints",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="INSTALL_AIRFLOW_WITH_CONSTRAINTS",
    help="Install airflow in a separate step, with constraints determined from package or airflow version.",
)
option_install_selected_providers = click.option(
    "--install-selected-providers",
    help="Comma-separated list of providers selected to be installed (implies --use-packages-from-dist).",
    envvar="INSTALL_SELECTED_PROVIDERS",
    default="",
)
option_providers_constraints_reference = click.option(
    "--providers-constraints-reference",
    help="Constraint reference to use for providers installation (used in calculated constraints URL). "
    "Can be 'default' in which case the default constraints-reference is used.",
    envvar="PROVIDERS_CONSTRAINTS_REFERENCE",
)
option_providers_constraints_location = click.option(
    "--providers-constraints-location",
    type=str,
    help="Location of providers constraints to use (remote URL or local context file).",
    envvar="PROVIDERS_CONSTRAINTS_LOCATION",
)
option_providers_constraints_mode_prod = click.option(
    "--providers-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_PROD),
    default=ALLOWED_CONSTRAINTS_MODES_PROD[0],
    show_default=True,
    help="Mode of constraints for Providers for PROD image building.",
)
option_providers_constraints_mode_ci = click.option(
    "--providers-constraints-mode",
    type=BetterChoice(ALLOWED_CONSTRAINTS_MODES_CI),
    default=ALLOWED_CONSTRAINTS_MODES_CI[0],
    show_default=True,
    help="Mode of constraints for Providers for CI image building.",
)
option_providers_skip_constraints = click.option(
    "--providers-skip-constraints",
    is_flag=True,
    help="Do not use constraints when installing providers.",
    envvar="PROVIDERS_SKIP_CONSTRAINTS",
)
option_use_packages_from_dist = click.option(
    "--use-packages-from-dist",
    is_flag=True,
    help="Install all found packages (--package-format determines type) from 'dist' folder "
    "when entering breeze.",
    envvar="USE_PACKAGES_FROM_DIST",
)

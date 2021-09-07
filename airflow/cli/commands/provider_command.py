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
"""Providers sub-commands"""
import re

from airflow.cli.simple_table import AirflowConsole
from airflow.providers_manager import ProvidersManager
from airflow.utils.cli import suppress_logs_and_warning

ERROR_IMPORTING_HOOK = "Error when importing hook!"


def _remove_rst_syntax(value: str) -> str:
    return re.sub("[`_<>]", "", value.strip(" \n."))


@suppress_logs_and_warning
def provider_get(args):
    """Get a provider info."""
    providers = ProvidersManager().providers
    if args.provider_name in providers:
        provider_version = providers[args.provider_name].version
        provider_info = providers[args.provider_name].provider_info
        if args.full:
            provider_info["description"] = _remove_rst_syntax(provider_info["description"])
            AirflowConsole().print_as(
                data=[provider_info],
                output=args.output,
            )
        else:
            print(f"Provider: {args.provider_name}")
            print(f"Version: {provider_version}")
    else:
        raise SystemExit(f"No such provider installed: {args.provider_name}")


@suppress_logs_and_warning
def providers_list(args):
    """Lists all providers at the command line"""
    AirflowConsole().print_as(
        data=list(ProvidersManager().providers.values()),
        output=args.output,
        mapper=lambda x: {
            "package_name": x[1]["package-name"],
            "description": _remove_rst_syntax(x[1]["description"]),
            "version": x[0],
        },
    )


@suppress_logs_and_warning
def hooks_list(args):
    """Lists all hooks at the command line"""
    AirflowConsole().print_as(
        data=list(ProvidersManager().hooks.items()),
        output=args.output,
        mapper=lambda x: {
            "connection_type": x[0],
            "class": x[1].hook_class_name if x[1] else ERROR_IMPORTING_HOOK,
            "conn_id_attribute_name": x[1].connection_id_attribute_name if x[1] else ERROR_IMPORTING_HOOK,
            'package_name': x[1].package_name if x[1] else ERROR_IMPORTING_HOOK,
            'hook_name': x[1].hook_name if x[1] else ERROR_IMPORTING_HOOK,
        },
    )


@suppress_logs_and_warning
def connection_form_widget_list(args):
    """Lists all custom connection form fields at the command line"""
    AirflowConsole().print_as(
        data=list(ProvidersManager().connection_form_widgets.items()),
        output=args.output,
        mapper=lambda x: {
            "connection_parameter_name": x[0],
            "class": x[1].hook_class_name,
            'package_name': x[1].package_name,
            'field_type': x[1].field.field_class.__name__,
        },
    )


@suppress_logs_and_warning
def connection_field_behaviours(args):
    """Lists field behaviours"""
    AirflowConsole().print_as(
        data=list(ProvidersManager().field_behaviours.keys()),
        output=args.output,
        mapper=lambda x: {
            "field_behaviours": x,
        },
    )


@suppress_logs_and_warning
def extra_links_list(args):
    """Lists all extra links at the command line"""
    AirflowConsole().print_as(
        data=ProvidersManager().extra_links_class_names,
        output=args.output,
        mapper=lambda x: {
            "extra_link_class_name": x,
        },
    )


@suppress_logs_and_warning
def logging_list(args):
    """Lists all log task handlers at the command line"""
    AirflowConsole().print_as(
        data=list(ProvidersManager().logging_class_names),
        output=args.output,
        mapper=lambda x: {
            "logging_class_name": x,
        },
    )


@suppress_logs_and_warning
def secrets_backends_list(args):
    """Lists all secrets backends at the command line"""
    AirflowConsole().print_as(
        data=list(ProvidersManager().secrets_backend_class_names),
        output=args.output,
        mapper=lambda x: {
            "secrets_backend_class_name": x,
        },
    )


@suppress_logs_and_warning
def auth_backend_list(args):
    """Lists all API auth backend modules at the command line"""
    AirflowConsole().print_as(
        data=list(ProvidersManager().auth_backend_module_names),
        output=args.output,
        mapper=lambda x: {
            "api_auth_backand_module": x,
        },
    )

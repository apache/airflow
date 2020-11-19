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
from typing import Dict, List

import pygments
import yaml
from pygments.lexers.data import YamlLexer
from tabulate import tabulate

from airflow.utils.cli import should_use_colors
from airflow.utils.code_utils import get_terminal_formatter


def _tabulate_providers(providers: List[Dict], tablefmt: str):
    tabulate_data = [
        {
            'Provider name': provider['package-name'],
            'Description': provider['description'],
            'Version': provider['versions'][0],
            'Package': provider['provider-package'],
        }
        for provider in providers
    ]

    msg = tabulate(tabulate_data, tablefmt=tablefmt, headers='keys')
    return msg


def provider_get(args):
    """Get a provider info."""
    from airflow.providers_manager import get_providers_manager

    providers = get_providers_manager().providers
    if args.provider_name in providers:
        yaml_content = yaml.dump(providers[args.provider_name])
        if should_use_colors(args):
            yaml_content = pygments.highlight(
                code=yaml_content, formatter=get_terminal_formatter(), lexer=YamlLexer()
            )
        print(yaml_content)
    else:
        raise SystemExit(f"No such provider installed: {args.provider_name}")


def providers_list(args):
    """Lists all providers at the command line"""
    from airflow.providers_manager import get_providers_manager

    msg = _tabulate_providers(get_providers_manager().providers.values(), args.output)
    print(msg)

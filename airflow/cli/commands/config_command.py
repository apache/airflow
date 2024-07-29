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
"""Config sub-commands."""

from __future__ import annotations

from io import StringIO

import pygments
from pygments.lexers.configs import IniLexer

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.cli import should_use_colors
from airflow.utils.code_utils import get_terminal_formatter
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@providers_configuration_loaded
def show_config(args):
    """Show current application configuration."""
    with StringIO() as output:
        conf.write(
            output,
            section=args.section,
            include_examples=args.include_examples or args.defaults,
            include_descriptions=args.include_descriptions or args.defaults,
            include_sources=args.include_sources and not args.defaults,
            include_env_vars=args.include_env_vars or args.defaults,
            include_providers=not args.exclude_providers,
            comment_out_everything=args.comment_out_everything or args.defaults,
            only_defaults=args.defaults,
        )
        code = output.getvalue()
    if should_use_colors(args):
        code = pygments.highlight(code=code, formatter=get_terminal_formatter(), lexer=IniLexer())
    print(code)


@providers_configuration_loaded
def get_value(args):
    """Get one value from configuration."""
    # while this will make get_value quite a bit slower we must initialize configuration
    # for providers because we do not know what sections and options will be available after
    # providers are initialized. Theoretically Providers might add new sections and options
    # but also override defaults for existing options, so without loading all providers we
    # cannot be sure what is the final value of the option.
    try:
        value = conf.get(args.section, args.option)
        print(value)
    except AirflowConfigException:
        pass

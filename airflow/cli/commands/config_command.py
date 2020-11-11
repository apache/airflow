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
"""Config sub-commands"""
import io
import sys

import pygments
from pygments.lexers.configs import IniLexer

from airflow.configuration import conf
from airflow.utils.cli import should_use_colors
from airflow.utils.code_utils import get_terminal_formatter


def show_config(args):
    """Show current application configuration"""
    with io.StringIO() as output:
        conf.write(output)
        code = output.getvalue()
        if should_use_colors(args):
            code = pygments.highlight(code=code, formatter=get_terminal_formatter(), lexer=IniLexer())
        print(code)


def get_value(args):
    """Get one value from configuration"""
    if not conf.has_section(args.section):
        print(f'The section [{args.section}] is not found in config.', file=sys.stderr)
        sys.exit(1)

    if not conf.has_option(args.section, args.option):
        print(f'The option [{args.section}/{args.option}] is not found in config.', file=sys.stderr)
        sys.exit(1)

    value = conf.get(args.section, args.option)
    print(value)

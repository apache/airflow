#
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
#
"""Roles sub-commands"""

from airflow.cli.simple_table import AirflowConsole
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.www.app import cached_app


@suppress_logs_and_warning()
def roles_list(args):
    """Lists all existing roles"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    roles = appbuilder.sm.get_all_roles()
    AirflowConsole().print_as(
        data=sorted([r.name for r in roles]), output=args.output, mapper=lambda x: {"name": x}
    )


@cli_utils.action_logging
@suppress_logs_and_warning()
def roles_create(args):
    """Creates new empty role in DB"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    for role_name in args.role:
        appbuilder.sm.add_role(role_name)
    print(f"Added {len(args.role)} role(s)")

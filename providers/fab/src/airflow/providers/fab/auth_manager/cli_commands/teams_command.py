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
"""Roles sub-commands."""

from __future__ import annotations

import itertools
import json
import os
from argparse import Namespace
from collections import defaultdict
from typing import TYPE_CHECKING

from airflow.cli.simple_table import AirflowConsole
from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.models import Team, User


@suppress_logs_and_warning
@providers_configuration_loaded
def teams_list(args):
    """List all existing teams."""
    with get_application_builder() as appbuilder:
        teams = appbuilder.sm.get_all_teams()

    if not args.members:
        AirflowConsole().print_as(
            data=sorted(t.name for t in teams), output=args.output, mapper=lambda x: {"name": x}
        )
        return

    members_map: dict[str, list[str]] = defaultdict(list)
    for team in teams:
        members_map[team.name] = [m.name for m in team.members]

    AirflowConsole().print_as(
        data=sorted(members_map),
        output=args.output,
        mapper=lambda x: {"name": x, "members": ",".join(sorted(members_map[x]))},
    )


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def teams_create(args):
    """Create new empty team in DB."""
    with get_application_builder() as appbuilder:
        for team_name in args.team:
            appbuilder.sm.add_team(team_name)
    print(f"Added {len(args.team)} role(s)")


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def teams_delete(args):
    """Delete team in DB."""
    with get_application_builder() as appbuilder:
        for team_name in args.team:
            team = appbuilder.sm.find_team(team_name)
            if not team:
                print(f"Role named '{team_name}' does not exist")
                exit(1)
        for team_name in args.team:
            appbuilder.sm.delete_role(team_name)
    print(f"Deleted {len(args.team)} role(s)")


def __teams_add_or_remove_members(args):
    with get_application_builder() as appbuilder:
        is_add: bool = args.subcommand.startswith("add")

        team_map: dict[str, Team] = {}
        member_map: dict[str, set[str]] = defaultdict(set)
        asm = appbuilder.sm
        for name in args.team:
            team: Team | None = asm.find_team(name)
            if not team:
                print(f"Role named '{name}' does not exist")
                exit(1)

            team_map[name] = team
            for member in team.members:
                member_map[name].add(member.name)

        for name in args.member:
            user: User | None = asm.find_user(name)
            if not user:
                print(f"User named '{name}' does not exist")
                exit(1)

        member_count = 0
        for team_name, member_name in itertools.product(args.team, args.member or [None]):
            if is_add and member_name not in member_map[team_name]:
                user: User | None = asm.find_user(member_name)
                asm.add_user_to_team(team_map[team_name], user)
                print(f"Added {member_name} to team {team_name}")
                member_count += 1
            elif not is_add and member_name in member_map:
                for _member_name in member_map[team_name] if member_name is None else [member_name]:
                    user: User | None = asm.find_user(_member_name)
                    asm.remove_user_from_team(team_map[team_name], user)
                    print(f"Deleted {member_name} from team {team_name}")
                    member_count += 1

        print(f"{'Added' if is_add else 'Deleted'} {member_count} member(s)")


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def teams_add_members(args):
    """Add members to team in DB."""
    __teams_add_or_remove_members(args)


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def teams_del_members(args):
    """Delete members from team in DB."""
    __teams_add_or_remove_members(args)


@suppress_logs_and_warning
@providers_configuration_loaded
def teams_export(args):
    """Export all the teams from the database to a file including members."""
    with get_application_builder() as appbuilder:
        exporting_teams = appbuilder.sm.get_all_teams()
    filename = os.path.expanduser(args.file)

    member_map: dict[str, list[str]] = defaultdict(list)
    for team in exporting_teams:
        for member in team.members:
            member_map[team.name].append(member.name)
    export_data = [
        {"name": team, "members": ",".join(sorted(members))} for team, members in member_map.items()
    ]
    kwargs = {} if not args.pretty else {"sort_keys": False, "indent": 4}
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(export_data, f, **kwargs)
    print(f"{len(exporting_teams)} teams with {len(export_data)} members successfully exported to {filename}")


@cli_utils.action_cli
@suppress_logs_and_warning
def teams_import(args):
    """
    Import all the teams into the db from the given json file including their members.

    Note, if a team already exists in the db, it is not overwritten, even when the members change.
    """
    json_file = args.file
    try:
        with open(json_file) as f:
            team_list = json.load(f)
    except FileNotFoundError:
        print(f"File '{json_file}' does not exist")
        exit(1)
    except ValueError as e:
        print(f"File '{json_file}' is not a valid JSON file. Error: {e}")
        exit(1)

    with get_application_builder() as appbuilder:
        existing_teams = [team.name for team in appbuilder.sm.get_all_teams()]
        teams_to_import = [team_dict for team_dict in team_list if team_dict["name"] not in existing_teams]
        for team_dict in teams_to_import:
            if team_dict["name"] not in appbuilder.sm.get_all_roles():
                appbuilder.sm.add_team(team_dict["name"])
                if team_dict.get("members"):
                    team_args = Namespace(
                        subcommand="add-members",
                        team=[team_dict["name"]],
                        members=team_dict["members"],
                    )
                __teams_add_or_remove_members(team_args)
        print("teams successfully imported")


@suppress_logs_and_warning
@providers_configuration_loaded
def teams_sync(args):
    with get_application_builder() as appbuilder:
        print("Syncing teams from Airflow into FAB")
        appbuilder.sm.sync_teams()
    print("Teams successfully synced")

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
"""
Edge3 provider commands exposed through ``airflowctl``.

Discovered via the ``ctl:`` field in ``providers/edge3/provider.yaml``, which
the breeze pyproject toolchain folds into the runtime ``get_provider_info()``
payload. ``airflowctl`` then imports each entry and calls it to build the
command tree at parser-construction time.

Scope is intentionally narrow: only routes under
``airflow.providers.edge3.worker_api.routes`` (excluding ``ui.py``, which is
not a public surface) are exposed here. Most of those routes are
worker-protocol endpoints with worker-side JWT auth, not suitable for an
admin/operator client — so for now only ``health`` lands. As more
admin-facing public routes are added in v3+, they can plug in here without
rewiring discovery.

Existing operator commands like ``list-workers`` / ``remove-remote-edge-worker``
remain in the core ``airflow edge`` CLI (``airflow.providers.edge3.cli.definition``)
because they currently rely on direct DB access, which is not part of the
public API contract. Per AIP-94 (see
``contributing-docs/27_cli_implementation_guide.rst``), those will move here
once equivalent public routes exist.
"""

from __future__ import annotations

import argparse

from airflowctl.ctl.cli_config import (
    ARG_OUTPUT,
    ActionCommand,
    CLICommand,
    GroupCommand,
    lazy_load_command,
)

EDGE_COMMANDS = (
    ActionCommand(
        name="health",
        help="Probe the Edge worker-api (GET /edge_worker/v1/health).",
        func=lazy_load_command("airflow.providers.edge3.ctl.health_command.health"),
        args=(ARG_OUTPUT,),
    ),
)


def get_edge_airflowctl_commands() -> list[CLICommand]:
    """
    Return the edge3 provider's airflowctl commands.

    Referenced from ``providers/edge3/provider.yaml`` under the ``ctl:`` field.
    """
    return [
        GroupCommand(
            name="edge",
            help="Inspect the Edge worker-api via the public worker_api routes.",
            subcommands=EDGE_COMMANDS,
        ),
    ]


def get_parser() -> argparse.ArgumentParser:
    """
    Build a parser scoped to edge3's airflowctl commands for documentation rendering.

    Used by ``sphinx-argparse`` from ``providers/edge3/docs/airflowctl-ref.rst``;
    not used at runtime.

    :meta private:
    """
    from airflowctl.ctl.cli_config import GroupCommandParser
    from airflowctl.ctl.cli_parser import (
        AirflowHelpFormatter,
        DefaultHelpParser,
        _add_command,
    )

    parser = DefaultHelpParser(prog="airflowctl", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for command in get_edge_airflowctl_commands():
        _add_command(
            subparsers,
            GroupCommandParser.from_group_command(command) if isinstance(command, GroupCommand) else command,
        )
    return parser

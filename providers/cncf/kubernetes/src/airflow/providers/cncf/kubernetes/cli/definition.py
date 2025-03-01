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
# The CLI definition should import as less as possible from other modules to reduce the overhead of importing the module.
from __future__ import annotations

import argparse

from airflow.configuration import conf

try:
    from airflow.cli.cli_config import ARG_LOGICAL_DATE
except ImportError:  # 2.x compatibility.
    from airflow.cli.cli_config import (  # type: ignore[attr-defined, no-redef]
        ARG_EXECUTION_DATE as ARG_LOGICAL_DATE,
    )
from airflow.cli.cli_config import (
    ARG_DAG_ID,
    ARG_OUTPUT_PATH,
    ARG_SUBDIR,
    ARG_VERBOSE,
    ActionCommand,
    Arg,
    GroupCommand,
    lazy_load_command,
    positive_int,
)

# CLI Args
ARG_NAMESPACE = Arg(
    ("--namespace",),
    default=conf.get("kubernetes_executor", "namespace"),
    help="Kubernetes Namespace. Default value is `[kubernetes] namespace` in configuration.",
)

ARG_MIN_PENDING_MINUTES = Arg(
    ("--min-pending-minutes",),
    default=30,
    type=positive_int(allow_zero=False),
    help=(
        "Pending pods created before the time interval are to be cleaned up, "
        "measured in minutes. Default value is 30(m). The minimum value is 5(m)."
    ),
)

# CLI Commands
KUBERNETES_COMMANDS = (
    ActionCommand(
        name="cleanup-pods",
        help=(
            "Clean up Kubernetes pods "
            "(created by KubernetesExecutor/KubernetesPodOperator) "
            "in evicted/failed/succeeded/pending states"
        ),
        func=lazy_load_command("airflow.providers.cncf.kubernetes.cli.kubernetes_command.cleanup_pods"),
        args=(ARG_NAMESPACE, ARG_MIN_PENDING_MINUTES, ARG_VERBOSE),
    ),
    ActionCommand(
        name="generate-dag-yaml",
        help="Generate YAML files for all tasks in DAG. Useful for debugging tasks without "
        "launching into a cluster",
        func=lazy_load_command("airflow.providers.cncf.kubernetes.cli.kubernetes_command.generate_pod_yaml"),
        args=(ARG_DAG_ID, ARG_LOGICAL_DATE, ARG_SUBDIR, ARG_OUTPUT_PATH, ARG_VERBOSE),
    ),
)

# CLI Groups
KUBERNETES_GROUP_COMMANDS: list[GroupCommand] = [
    GroupCommand(
        name="kubernetes",
        help="Tools to help run the KubernetesExecutor",
        subcommands=KUBERNETES_COMMANDS,
    )
]


def _get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    from airflow.cli.cli_config import DefaultHelpParser
    from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in KUBERNETES_GROUP_COMMANDS:
        _add_command(subparsers, group_command)
    return parser

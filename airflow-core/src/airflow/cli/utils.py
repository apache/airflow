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

import logging
import os
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

# Placeholder for masking sensitive values in CLI output
SENSITIVE_PLACEHOLDER = "***"

if TYPE_CHECKING:
    import datetime
    from collections.abc import Collection
    from io import IOBase, TextIOWrapper

    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun

F = TypeVar("F", bound=Callable[..., object])


def deprecated_for_airflowctl(replacement: str) -> Callable[[F], F]:
    """
    Mark an ``airflow`` CLI command as deprecated in favour of an ``airflowctl`` equivalent.

    The command keeps its existing implementation and stays in the ``airflow`` CLI as a supported
    entry point, so it emits **no user-facing deprecation warning** at runtime. The intent is to
    point future development at ``airflowctl``: the equivalent ``airflowctl`` command is recorded
    for maintainers only, on the ``_migrated_to_airflowctl`` attribute (the migration registry test
    in ``test_command_deprecations.py`` reads it). The decorator at the command's definition site is
    the developer-facing trace -- it is source-only and never rendered to users.

    See ``contributing-docs/27_cli_implementation_guide.rst`` for the CLI / ``airflowctl``
    development guidance.

    :param replacement: The equivalent ``airflowctl`` command, e.g. ``airflowctl dags trigger``.
    """

    def decorator(func: F) -> F:
        func._migrated_to_airflowctl = replacement  # type: ignore[attr-defined]
        return func

    return decorator


class CliConflictError(Exception):
    """Error for when CLI commands are defined twice by different sources."""

    pass


def is_stdout(fileio: IOBase) -> bool:
    """
    Check whether a file IO is stdout.

    The intended use case for this helper is to check whether an argument parsed
    with argparse.FileType points to stdout (by setting the path to ``-``). This
    is why there is no equivalent for stderr; argparse does not allow using it.

    """
    return fileio is sys.stdout


def redirect_stdout_log_handlers_to_stderr() -> None:
    """
    Redirect any root-logger ``StreamHandler`` writing to stdout so it writes to stderr.

    Called from the CLI entrypoint for commands that emit structured output on
    stdout (``-o json|yaml|plain|table``), so log lines do not corrupt that
    output. ``FileHandler`` is a ``StreamHandler`` subclass; the identity check
    against ``sys.stdout`` correctly skips it.
    """
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream is sys.stdout:
            handler.setStream(sys.stderr)


def print_export_output(command_type: str, exported_items: Collection, file: TextIOWrapper):
    if is_stdout(file):
        print(f"\n{len(exported_items)} {command_type} successfully exported.", file=sys.stderr)
    else:
        print(f"{len(exported_items)} {command_type} successfully exported to {file.name}.")


def get_hidden_entries_warning(entity_name: str, env_prefix: str) -> str | None:
    """
    Build a warning about entries this listing cannot show.

    Connections and Variables can be defined in three places: the metadata database, environment
    variables, and an optional secrets backend. The database is checked last, so an environment
    variable or a secrets backend entry silently takes precedence over a database row with the same
    ID. Commands that only enumerate database rows (like ``connections list`` / ``variables list``)
    should surface that gap explicitly instead of presenting the database rows as the full picture.

    :param entity_name: Human-readable plural noun to use in the message, e.g. ``"connections"``.
    :param env_prefix: Environment variable prefix used for this entity, e.g. ``AIRFLOW_CONN_``.
    :return: A warning message, or ``None`` if neither hiding source appears to be in use.
    """
    from airflow.configuration import conf

    has_env_vars = any(key.startswith(env_prefix) for key in os.environ)
    # Only check whether a custom backend is *configured*, without instantiating it (which could
    # have side effects, e.g. opening a network connection to a Vault/AWS/GCP secrets service).
    has_secrets_backend = bool(conf.get("secrets", "backend", fallback=None))

    if not has_env_vars and not has_secrets_backend:
        return None

    sources = []
    if has_env_vars:
        sources.append(f"`{env_prefix}*` environment variables")
    if has_secrets_backend:
        sources.append("the configured secrets backend")

    return (
        f"This list only includes {entity_name} stored in the metadata database. "
        f"{' and '.join(sources)} may also define {entity_name} -- including ones that override a "
        "database entry with the same ID -- that will not appear here."
    )


def fetch_dag_run_from_run_id_or_logical_date_string(
    *,
    dag_id: str,
    value: str,
    session: Session,
) -> tuple[DagRun | None, datetime.datetime | None]:
    """
    Try to find a DAG run with a given string value.

    The string value may be a run ID, or a logical date in string form. We first
    try to use it as a run_id; if a run is found, it is returned as-is.

    Otherwise, the string value is parsed into a datetime. If that works, it is
    used to find a DAG run.

    The return value is a two-tuple. The first item is the found DAG run (or
    *None* if one cannot be found). The second is the parsed logical date. This
    second value can be used to create a new run by the calling function when
    one cannot be found here.
    """
    from pendulum.parsing.exceptions import ParserError
    from sqlalchemy import select

    from airflow._shared.timezones import timezone
    from airflow.models.dagrun import DagRun

    if dag_run := session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == value)):
        return dag_run, dag_run.logical_date
    try:
        logical_date = timezone.parse(value)
    except (ParserError, TypeError):
        return None, None
    dag_run = session.scalar(
        select(DagRun)
        .where(DagRun.dag_id == dag_id, DagRun.logical_date == logical_date)
        .order_by(DagRun.id.desc())
        .limit(1)
    )
    return dag_run, logical_date

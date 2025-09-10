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

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime
    from collections.abc import Collection
    from io import IOBase, TextIOWrapper

    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun


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


def print_export_output(command_type: str, exported_items: Collection, file: TextIOWrapper):
    if is_stdout(file):
        print(f"\n{len(exported_items)} {command_type} successfully exported.", file=sys.stderr)
    else:
        print(f"{len(exported_items)} {command_type} successfully exported to {file.name}.")


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

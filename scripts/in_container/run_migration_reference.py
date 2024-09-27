#!/usr/bin/env python3
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
"""
Module to update db migration information in Airflow
"""

from __future__ import annotations

import os
import re
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

import re2
from alembic.script import ScriptDirectory
from rich.console import Console
from tabulate import tabulate

from airflow import __version__ as airflow_version
from airflow.providers.fab import __version__ as fab_version
from airflow.utils.db import _get_alembic_config

if TYPE_CHECKING:
    from alembic.script import Script

console = Console(width=400, color_system="standard")

airflow_version = re.match(r"(\d+\.\d+\.\d+).*", airflow_version).group(1)  # type: ignore
fab_version = re.match(r"(\d+\.\d+\.\d+).*", fab_version).group(1)  # type: ignore
project_root = Path(__file__).parents[2].resolve()


def replace_text_between(file: Path, start: str, end: str, replacement_text: str):
    original_text = file.read_text()
    leading_text = original_text.split(start)[0]
    trailing_text = original_text.split(end)[1]
    file.write_text(leading_text + start + replacement_text + end + trailing_text)


def wrap_backticks(val):
    def _wrap_backticks(x):
        return f"``{x}``"

    return ",\n".join(map(_wrap_backticks, val)) if isinstance(val, (tuple, list)) else _wrap_backticks(val)


def update_doc(file, data, app):
    replace_text_between(
        file=file,
        start=" .. Beginning of auto-generated table\n",
        end=" .. End of auto-generated table\n",
        replacement_text="\n"
        + tabulate(
            headers={
                "revision": "Revision ID",
                "down_revision": "Revises ID",
                "version": f"{app.title()} Version",
                "description": "Description",
            },
            tabular_data=data,
            tablefmt="grid",
            stralign="left",
            disable_numparse=True,
        )
        + "\n\n",
    )


def has_version(content, app):
    if app == "airflow":
        return re.search(r"^airflow_version\s*=.*", content, flags=re.MULTILINE) is not None
    return re.search(r"^fab_version\s*=.*", content, flags=re.MULTILINE) is not None


def insert_version(old_content, file, app):
    if app == "airflow":
        new_content = re.sub(
            r"(^depends_on.*)",
            lambda x: f'{x.group(1)}\nairflow_version = "{airflow_version}"',
            old_content,
            flags=re.MULTILINE,
        )
    else:
        new_content = re.sub(
            r"(^depends_on.*)",
            lambda x: f'{x.group(1)}\nfab_version = "{fab_version}"',
            old_content,
            flags=re.MULTILINE,
        )
    file.write_text(new_content)


def revision_suffix(rev: Script):
    if rev.is_head:
        return " (head)"
    if rev.is_base:
        return " (base)"
    if rev.is_merge_point:
        return " (merge_point)"
    if rev.is_branch_point:
        return " (branch_point)"
    return ""


def ensure_version(revisions: Iterable[Script], app):
    for rev in revisions:
        if TYPE_CHECKING:  # For mypy
            assert rev.module.__file__ is not None
        file = Path(rev.module.__file__)
        content = file.read_text()
        if not has_version(content, app=app):
            insert_version(content, file, app=app)


def get_revisions(app="airflow") -> Iterable[Script]:
    if app == "airflow":
        config = _get_alembic_config()
        script = ScriptDirectory.from_config(config)
        yield from script.walk_revisions()
    else:
        from airflow.providers.fab.auth_manager.models.db import FABDBManager

        script = FABDBManager(session="").get_script_object()
        yield from script.walk_revisions()


def update_docs(revisions: Iterable[Script], app="airflow"):
    doc_data = []
    for rev in revisions:
        app_revision = rev.module.airflow_version if app == "airflow" else rev.module.fab_version
        doc_data.append(
            dict(
                revision=wrap_backticks(rev.revision) + revision_suffix(rev),
                down_revision=wrap_backticks(rev.down_revision),
                version=wrap_backticks(app_revision),  # type: ignore
                description="\n".join(textwrap.wrap(rev.doc, width=60)),
            )
        )
    if app == "fab":
        filepath = project_root / "docs" / "apache-airflow-providers-fab" / "migrations-ref.rst"
    else:
        filepath = project_root / "docs" / "apache-airflow" / "migrations-ref.rst"

    update_doc(
        file=filepath,
        data=doc_data,
        app=app,
    )


def ensure_mod_prefix(mod_name, idx, version):
    parts = [f"{idx + 1:04}", *version]
    match = re.match(r"([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)_(.+)", mod_name)
    if match:
        # previously standardized file, rebuild the name
        parts.append(match.group(5))
    else:
        # new migration file, standard format
        match = re.match(r"([a-z0-9]+)_(.+)", mod_name)
        if match:
            parts.append(match.group(2))
    return "_".join(parts)


def ensure_filenames_are_sorted(revisions, app):
    renames = []
    is_branched = False
    unmerged_heads = []
    for idx, rev in enumerate(revisions):
        mod_path = Path(rev.module.__file__)
        if app == "airflow":
            version = rev.module.airflow_version.split(".")[0:3]  # only first 3 tokens
        else:
            version = rev.module.fab_version.split(".")[0:3]  # only first 3 tokens
        correct_mod_basename = ensure_mod_prefix(mod_path.name, idx, version)
        if mod_path.name != correct_mod_basename:
            renames.append((mod_path, Path(mod_path.parent, correct_mod_basename)))
        if is_branched and rev.is_merge_point:
            is_branched = False
        if rev.is_branch_point:
            is_branched = True
        elif rev.is_head:
            unmerged_heads.append(rev.revision)
    if is_branched:
        head_prefixes = [x[0:4] for x in unmerged_heads]
        alembic_command = (
            "alembic merge -m 'merge heads " + ", ".join(head_prefixes) + "' " + " ".join(unmerged_heads)
        )
        raise SystemExit(
            "You have multiple alembic heads; please merge them with by running `alembic merge` command under "
            f'"airflow" directory (where alembic.ini located) and re-run pre-commit. '
            f"It should fail once more before succeeding.\nhint: `{alembic_command}`"
        )
    for old, new in renames:
        os.rename(old, new)


def correct_mismatching_revision_nums(revisions: Iterable[Script]):
    revision_pattern = r'revision = "([a-fA-F0-9]+)"'
    down_revision_pattern = r'down_revision = "([a-fA-F0-9]+)"'
    revision_id_pattern = r"Revision ID: ([a-fA-F0-9]+)"
    revises_id_pattern = r"Revises: ([a-fA-F0-9]+)"
    for rev in revisions:
        if TYPE_CHECKING:  # For mypy
            assert rev.module.__file__ is not None
        file = Path(rev.module.__file__)
        content = file.read_text()
        revision_match = re2.search(
            revision_pattern,
            content,
        )
        revision_id_match = re2.search(revision_id_pattern, content)
        new_content = content.replace(revision_id_match.group(1), revision_match.group(1), 1)
        down_revision_match = re2.search(down_revision_pattern, new_content)
        revises_id_match = re2.search(revises_id_pattern, new_content)
        if down_revision_match:
            new_content = new_content.replace(revises_id_match.group(1), down_revision_match.group(1), 1)
        file.write_text(new_content)


if __name__ == "__main__":
    apps = ["airflow", "fab"]
    for app in apps:
        console.print(f"[bright_blue]Updating migration reference for {app}")
        revisions = list(reversed(list(get_revisions(app))))
        console.print(f"[bright_blue]Making sure {app} version updated")
        ensure_version(revisions=revisions, app=app)
        console.print("[bright_blue]Making sure there's no mismatching revision numbers")
        correct_mismatching_revision_nums(revisions=revisions)
        revisions = list(reversed(list(get_revisions(app=app))))
        console.print("[bright_blue]Making sure filenames are sorted")
        ensure_filenames_are_sorted(revisions=revisions, app=app)
        revisions = list(get_revisions(app=app))
        console.print("[bright_blue]Updating documentation")
        update_docs(revisions=revisions, app=app)
        console.print("[green]Migrations OK")

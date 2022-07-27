#!/usr/bin/env python
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
Module to check pre-commit hook names for length
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported

from collections import defaultdict  # noqa: E402
from functools import lru_cache  # noqa: E402
from typing import Any, Dict, List, Tuple  # noqa: E402

import yaml  # noqa: E402
from common_precommit_utils import insert_documentation  # noqa: E402
from rich.console import Console  # noqa: E402
from tabulate import tabulate  # noqa: E402

console = Console(width=400, color_system="standard")

AIRFLOW_SOURCES_PATH = Path(__file__).parents[3].resolve()
AIRFLOW_BREEZE_SOURCES_PATH = AIRFLOW_SOURCES_PATH / "dev" / "breeze"
PRE_COMMIT_IDS_PATH = AIRFLOW_BREEZE_SOURCES_PATH / "src" / "airflow_breeze" / "pre_commit_ids.py"
PRE_COMMIT_YAML_FILE = AIRFLOW_SOURCES_PATH / ".pre-commit-config.yaml"


def get_errors_and_hooks(content: Any, max_length: int) -> Tuple[List[str], Dict[str, List[str]], List[str]]:
    errors = []
    hooks: Dict[str, List[str]] = defaultdict(list)
    needs_image = False
    image_hooks = []
    for repo in content['repos']:
        for hook in repo['hooks']:
            if 'id' in hook:
                hook_id = hook['id']
            else:
                errors.append(f"The id is missing in {hook}")
                continue
            if hook_id == 'run-mypy':
                needs_image = True
            if 'name' not in hook:
                errors.append(
                    f"Name is missing in hook `{hook_id}` in {PRE_COMMIT_YAML_FILE}. Please add it!"
                )
                continue
            name = hook['name']
            if len(name) > max_length:
                errors.append(
                    f"Name is too long for hook `{hook_id}` in {PRE_COMMIT_YAML_FILE}. Please shorten it!"
                )
                continue
            hooks[hook_id].append(name)
            if needs_image:
                image_hooks.append(hook_id)
    return errors, hooks, image_hooks


def render_template(
    searchpath: Path,
    template_name: str,
    context: Dict[str, Any],
    extension: str,
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on its name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param searchpath: Path to search images in
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param extension: Target file extension
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=searchpath)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE{extension}.jinja2")
    content: str = template.render(context)
    return content


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(AIRFLOW_BREEZE_SOURCES_PATH / "pyproject.toml")

    target_versions = set(
        target_version_option_callback(None, None, tuple(config.get('target_version', ()))),
    )

    return Mode(
        target_versions=target_versions,
        line_length=bool(config.get('line_length', Mode.line_length)),
        is_pyi=bool(config.get('is_pyi', Mode.is_pyi)),
        string_normalization=not bool(config.get('skip_string_normalization', not Mode.string_normalization)),
        preview=bool(config.get('preview', Mode.preview)),
    )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


def prepare_pre_commit_ids_py_file(pre_commit_ids):
    PRE_COMMIT_IDS_PATH.write_text(
        black_format(
            render_template(
                searchpath=AIRFLOW_BREEZE_SOURCES_PATH / "src" / "airflow_breeze",
                template_name="pre_commit_ids",
                context={"PRE_COMMIT_IDS": pre_commit_ids},
                extension='.py',
                autoescape=False,
                keep_trailing_newline=True,
            )
        )
    )


def update_static_checks_array(hooks: Dict[str, List[str]], image_hooks: List[str]):
    rows = []
    hook_ids = list(hooks.keys())
    hook_ids.sort()
    for hook_id in hook_ids:
        hook_description = hooks[hook_id]
        formatted_hook_description = (
            hook_description[0] if len(hook_description) == 1 else "* " + "\n* ".join(hook_description)
        )
        rows.append((hook_id, formatted_hook_description, " * " if hook_id in image_hooks else "  "))
    formatted_table = "\n" + tabulate(rows, tablefmt="grid", headers=("ID", "Description", 'Image')) + "\n\n"
    insert_documentation(
        file_path=AIRFLOW_SOURCES_PATH / "STATIC_CODE_CHECKS.rst",
        content=formatted_table.splitlines(keepends=True),
        header="  .. BEGIN AUTO-GENERATED STATIC CHECK LIST",
        footer="  .. END AUTO-GENERATED STATIC CHECK LIST",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-length', help="Max length for hook names")
    args = parser.parse_args()
    max_length = int(args.max_length) or 70
    content = yaml.safe_load(PRE_COMMIT_YAML_FILE.read_text())
    errors, hooks, image_hooks = get_errors_and_hooks(content, max_length)
    if errors:
        for error in errors:
            console.print(f"* [red]{error}[/]")
        sys.exit(1)
    ids = list(hooks.keys())
    ids.append("all")
    ids.sort()
    prepare_pre_commit_ids_py_file(ids)
    update_static_checks_array(hooks, image_hooks)


if __name__ == '__main__':
    main()

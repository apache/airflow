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
Module to check pre-commit hook names for length
"""
import argparse
import sys

import yaml

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-length', help="Max length for hook names")
    args = parser.parse_args()
    max_length = int(args.max_length) or 70

    retval = 0

    with open('.pre-commit-config.yaml', 'rb') as f:
        content = yaml.load(f, SafeLoader)
        errors, ids = get_errors_and_ids(content, max_length)
        ids.append("all")
        ids.sort()
        prepare_pre_commit_ids_py_file(ids)
    if errors:
        retval = 1
        print(f"found pre-commit hook names with length exceeding {max_length} characters")
        print("move add details in description if necessary")
    for hook_name in errors:
        print(f"    * '{hook_name}': length {len(hook_name)}")
    return retval


def get_errors_and_ids(content, max_length):
    errors = []
    ids = set()
    for repo in content['repos']:
        for hook in repo['hooks']:
            if 'id' in hook:
                id = hook['id']
                ids.add(id)
            if 'name' not in hook:
                continue
            name = hook['name']
            if len(name) > max_length:
                errors.append(name)
    return errors, list(ids)


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


# def get_precommit_jinja_context(pre_commit_ids: List) -> Dict[str, List]:
#     context: Dict[str, List] = {"PRE_COMMIT_IDS": pre_commit_ids}
#     return context


def get_target_precommit_parent_dir():
    airflow_source = Path.cwd()
    pre_commit_ids_file = Path(airflow_source) / "dev" / "breeze" / "src" / "airflow_breeze"
    return pre_commit_ids_file


def get_breeze_pyproject_toml_dir():
    airflow_source = Path.cwd()
    breeze_pyproject_toml_file = Path(airflow_source) / "dev" / "breeze"
    return breeze_pyproject_toml_file


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(Path(get_breeze_pyproject_toml_dir(), "pyproject.toml"))

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
    pre_commit_ids_template_name = "pre_commit_ids"
    pre_commit_ids_file_path = Path(get_target_precommit_parent_dir(), "pre_commit_ids.py")

    get_precommit_id_content = render_template(
        searchpath=get_target_precommit_parent_dir(),
        template_name=pre_commit_ids_template_name,
        context={"PRE_COMMIT_IDS": pre_commit_ids},
        extension='.py',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(pre_commit_ids_file_path, "wt") as pre_commit_ids_file:
        pre_commit_ids_file.write(black_format(get_precommit_id_content))


if __name__ == '__main__':
    sys.exit(main())

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

import hashlib
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import jinja2

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()
AIRFLOW_BREEZE_SOURCES_PATH = AIRFLOW_SOURCES_ROOT / "dev" / "breeze"


def filter_out_providers_on_non_main_branch(files: list[str]) -> list[str]:
    """When running build on non-main branch do not take providers into account"""
    default_branch = os.environ.get("DEFAULT_BRANCH")
    if not default_branch or default_branch == "main":
        return files
    return [file for file in files if not file.startswith(f"airflow{os.sep}providers")]


def insert_documentation(file_path: Path, content: list[str], header: str, footer: str):
    text = file_path.read_text().splitlines(keepends=True)
    replacing = False
    result: list[str] = []
    for line in text:
        if line.strip().startswith(header.strip()):
            replacing = True
            result.append(line)
            result.extend(content)
        if line.strip().startswith(footer.strip()):
            replacing = False
        if not replacing:
            result.append(line)
    src = "".join(result)
    file_path.write_text(src)


def get_directory_hash(directory: Path, skip_path_regexp: str | None = None) -> str:
    files = [file for file in directory.rglob("*")]
    files.sort()
    if skip_path_regexp:
        matcher = re.compile(skip_path_regexp)
        files = [file for file in files if not matcher.match(os.fspath(file.resolve()))]
    sha = hashlib.sha256()
    for file in files:
        if file.is_file() and not file.name.startswith("."):
            sha.update(file.read_bytes())
    return sha.hexdigest()


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(AIRFLOW_BREEZE_SOURCES_PATH / "pyproject.toml")

    target_versions = set(
        target_version_option_callback(None, None, tuple(config.get("target_version", ()))),
    )

    return Mode(
        target_versions=target_versions,
        line_length=config.get("line_length", Mode.line_length),
        is_pyi=bool(config.get("is_pyi", Mode.is_pyi)),
        string_normalization=not bool(config.get("skip_string_normalization", not Mode.string_normalization)),
        preview=bool(config.get("preview", Mode.preview)),
    )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


def render_template_from_file(
    searchpath: Path,
    template_name: str,
    context: dict[str, Any],
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
    template_loader = jinja2.FileSystemLoader(searchpath=searchpath)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE{extension}.jinja2")
    return template.render(context)


def render_template_from_string(
    template_string: str,
    context: dict[str, Any] | None = None,
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template from string.
    :param template_string: template string
    :param context: Jinja2 context
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """

    template_env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    ).from_string(template_string)
    if context is None:
        context = {}
    return template_env.render(context)

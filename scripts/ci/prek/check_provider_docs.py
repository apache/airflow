#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "jinja2>=3.1.2",
#   "pyyaml>=6.0.3",
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common utils are importable

from common_prek_utils import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
    get_all_provider_info_dicts,
)

sys.path.insert(0, str(AIRFLOW_CORE_SOURCES_PATH))  # make sure setup is imported from Airflow

console = Console(color_system="standard", width=200)

AIRFLOW_PROVIDERS_IMPORT_PREFIX = "airflow.providers."

warnings: list[str] = []
errors: list[str] = []

suspended_paths: list[str] = []

ALL_DEPENDENCIES: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))


LICENCE_CONTENT_RST = """
 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
"""

SECURITY_CONTENT_RST = """
.. include:: /devel-common/src/sphinx_exts/includes/security.rst
"""

INSTALLING_PROVIDERS_FROM_SOURCES_CONTENT_RST = """
.. include:: .. include:: /../../../devel-common/src/sphinx_exts/includes/installing-providers-from-sources.rst
"""


COMMIT_CONTENT_RST = """
 .. THIS FILE IS UPDATED AUTOMATICALLY_AT_RELEASE_TIME
"""

failed: list[bool] = []


def process_content_to_write(content: str) -> str:
    """Allow content to be defined with leading empty lines and strip/add EOL"""
    if not content:
        return content
    content_lines = content.splitlines()
    if content_lines and content_lines[0] == "":
        content_lines = content_lines[1:]
    content_to_write = "\n".join(content_lines) + "\n"
    return content_to_write


def get_provider_doc_folder(provider_id: str) -> Path:
    return AIRFLOW_PROVIDERS_ROOT_PATH.joinpath(*provider_id.split(".")) / "docs"


def check_provider_doc_exists_and_in_index(
    *,
    provider_id: str,
    index_link: str,
    file_name: str,
    generated_content: str = "",
    missing_ok: bool = False,
    check_content: bool = True,
):
    console.print(f"   [bright_blue]Checking [/]: {file_name} ", end="")
    fail = False
    provider_docs_file = get_provider_doc_folder(provider_id)
    file_path = provider_docs_file / file_name
    index_file = provider_docs_file / "index.rst"
    content_to_write = process_content_to_write(generated_content)
    regenerate_file = False
    if file_path.exists():
        if check_content and not generated_content:
            if file_path.read_text() != content_to_write:
                console.print("[yellow]Generating[/]")
                console.print()
                console.print(f"[yellow]Content of the file will be regenerated: [/]{file_path}")
                console.print()
                regenerate_file = True
    else:
        if missing_ok:
            # Fine - we do not check anything else
            console.print("[green]OK[/]")
            failed.append(False)
            return
        if not generated_content:
            console.print("[yellow]Generating[/]")
            console.print()
            console.print(f"[yellow]Missing file: [/]{file_path}")
            console.print("[bright_blue]Please create the file looking at other providers as example [/]")
            console.print()
        else:
            regenerate_file = True
    if regenerate_file:
        fail = True
        file_path.write_text(content_to_write)
        console.print()
        console.print(f"[yellow]Content updated in file: [/]{file_path}")
        console.print()
    if index_link not in index_file.read_text():
        console.print("[red]NOK[/]")
        fail = True
        console.print()
        console.print(
            f"[red]ERROR! Missing index link![/]\n"
            f"The index file: {index_file} should have this link:\n"
            f"{index_link}\n\n"
            f"[bright_blue]Please add the entry in the index!"
        )
        console.print()
    if not fail:
        console.print("[green]OK[/]")
    failed.append(fail)


def check_documentation_link_exists(link: str, docs_file: Path):
    console.print(f"   [bright_blue]Checking [/]: {docs_file} for link: {link} ", end="")
    if link not in docs_file.read_text():
        console.print("[red]NOK[/]")
        console.print()
        console.print(
            f"[red]ERROR! The {docs_file} does not contain:\n:[/]{link}\n[bright_blue]Please add it!"
        )
        console.print()
        failed.append(True)
    console.print("[green]OK[/]")
    failed.append(False)


def has_executor_package_defined(provider_id: str) -> bool:
    provider_distribution_path = (
        AIRFLOW_PROVIDERS_ROOT_PATH.joinpath(*provider_id.split(".")) / "src"
    ).joinpath(*provider_id.split("."))
    for executors_folder in provider_distribution_path.rglob("executors"):
        if executors_folder.is_dir() and (executors_folder / "__init__.py").is_file():
            return True
    return False


def run_all_checks():
    all_providers = get_all_provider_info_dicts()
    status: list[bool] = []

    for provider_id, provider_info in all_providers.items():
        console.print(f"[bright_blue]Checking provider: {provider_id}[/]")
        provider_docs_folder = get_provider_doc_folder(provider_id)
        if not provider_docs_folder.exists():
            provider_docs_folder.mkdir(parents=True)

        check_provider_doc_exists_and_in_index(
            provider_id=provider_id,
            index_link="Detailed list of commits <commits>",
            file_name="commits.rst",
            generated_content=LICENCE_CONTENT_RST + COMMIT_CONTENT_RST,
            # Only create commit content if it is missing, otherwise leave what is there
            check_content=False,
        )
        check_provider_doc_exists_and_in_index(
            provider_id=provider_id,
            index_link="Security <security>",
            file_name="security.rst",
            generated_content=LICENCE_CONTENT_RST + SECURITY_CONTENT_RST,
        )
        check_provider_doc_exists_and_in_index(
            provider_id=provider_id,
            index_link="Installing from sources <installing-providers-from-sources>",
            file_name="installing-providers-from-sources.rst",
            generated_content=LICENCE_CONTENT_RST + INSTALLING_PROVIDERS_FROM_SOURCES_CONTENT_RST,
        )
        if has_executor_package_defined(provider_id) and not provider_info.get("executors"):
            provider_yaml = AIRFLOW_PROVIDERS_ROOT_PATH.joinpath(*provider_id.split(".")) / "provider.yaml"
            console.print()
            console.print(
                f"[red]ERROR! The {provider_id} provider has executor package but "
                f"does not have `executors` defined in {provider_yaml}."
            )
            console.print(f"\nPlease add executor class to `executors` array in {provider_yaml}")
            status.append(False)

        if provider_info.get("executors"):
            check_provider_doc_exists_and_in_index(
                provider_id=provider_id,
                index_link="CLI <cli-ref>",
                file_name="cli-ref.rst",
                missing_ok=True,
                check_content=False,
            )
            if (get_provider_doc_folder(provider_id) / "cli-ref.rst").exists():
                check_documentation_link_exists(
                    link=f"and related CLI commands: :doc:`apache-airflow-providers-{provider_id.replace('.', '-')}:cli-ref`",
                    docs_file=AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "cli-and-env-variables-ref.rst",
                )
    print(failed)
    if any(failed):
        sys.exit(1)


if __name__ == "__main__":
    run_all_checks()

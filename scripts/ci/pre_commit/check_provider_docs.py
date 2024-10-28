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
from __future__ import annotations

import os
import sys
from collections import defaultdict
from pathlib import Path

import yaml
from jinja2 import BaseLoader, Environment
from rich.console import Console

console = Console(color_system="standard", width=200)

AIRFLOW_PROVIDERS_IMPORT_PREFIX = "airflow.providers."

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()

AIRFLOW_PROVIDERS_DIR = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"
AIRFLOW_DOC_FILES = AIRFLOW_SOURCES_ROOT / "docs"
AIRFLOW_DOC_AIRFLOW_BASE_FOLDER = AIRFLOW_DOC_FILES / "apache-airflow"

sys.path.insert(0, str(AIRFLOW_SOURCES_ROOT))  # make sure setup is imported from Airflow

warnings: list[str] = []
errors: list[str] = []

suspended_paths: list[str] = []

ALL_DEPENDENCIES: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

ALL_PROVIDERS: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict())

# Allow AST to parse the files.
sys.path.append(str(AIRFLOW_SOURCES_ROOT))

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

CONFIGURATION_CONTENT_RST = """
.. include:: ../exts/includes/providers-configurations-ref.rst
"""

SECURITY_CONTENT_RST = """
.. include:: ../exts/includes/security.rst
"""

INSTALLING_PROVIDERS_FROM_SOURCES_CONTENT_RST = """
.. include:: ../exts/includes/installing-providers-from-sources.rst
"""

CHANGELOG_CONTENT_RST = """
.. include:: ../../airflow/providers/{{provider_id | replace ('.', '/')}}/CHANGELOG.rst
"""

COMMIT_CONTENT_RST = """
 .. THIS FILE IS UPDATED AUTOMATICALLY_AT_RELEASE_TIME
"""


def find_all_providers():
    for provider_file in AIRFLOW_PROVIDERS_DIR.rglob("provider.yaml"):
        provider_name = str(
            provider_file.parent.relative_to(AIRFLOW_PROVIDERS_DIR)
        ).replace(os.sep, ".")
        provider_info = yaml.safe_load(provider_file.read_text())
        if provider_info["state"] != "suspended":
            ALL_PROVIDERS[provider_name] = provider_info


find_all_providers()

fail_pre_commit = False


def process_content_to_write(content: str) -> str:
    """Allow content to be defined with leading empty lines and strip/add EOL"""
    if not content:
        return content
    content_lines = content.splitlines()
    if content_lines and content_lines[0] == "":
        content_lines = content_lines[1:]
    content_to_write = "\n".join(content_lines) + "\n"
    return content_to_write


def check_provider_doc_exists_and_in_index(
    *,
    provider_id: str,
    index_link: str,
    file_name: str,
    generated_content: str = "",
    missing_ok: bool = False,
    check_content: bool = True,
) -> None:
    global fail_pre_commit
    provider_docs_file = get_provider_doc_folder(provider_id)
    file_path = provider_docs_file / file_name
    index_file = provider_docs_file / "index.rst"
    content_to_write = process_content_to_write(generated_content)
    regenerate_file = False
    if file_path.exists():
        if check_content and not generated_content:
            if file_path.read_text() != content_to_write:
                console.print()
                console.print(
                    f"[yellow]Content of the file will be regenerated: [/]{file_path}"
                )
                console.print()
                regenerate_file = True
    else:
        if missing_ok:
            # Fine - we do not check anything else
            return
        if not generated_content:
            console.print()
            console.print(f"[yellow]Missing file: [/]{file_path}")
            console.print(
                "[bright_blue]Please create the file looking at other providers as example [/]"
            )
            console.print()
        else:
            regenerate_file = True
    if regenerate_file:
        fail_pre_commit = True
        file_path.write_text(content_to_write)
        console.print()
        console.print(f"[yellow]Content updated in file: [/]{file_path}")
        console.print()
    if index_link not in index_file.read_text():
        fail_pre_commit = True
        console.print()
        console.print(
            f"[red]ERROR! Missing index link![/]\n"
            f"The index file: {index_file} should have this link:\n"
            f"{index_link}\n\n"
            f"[bright_blue]Please add the entry in the index!"
        )
        console.print()


def check_documentation_link_exists(link: str, doc_file_name: str):
    global fail_pre_commit
    docs_file = AIRFLOW_DOC_AIRFLOW_BASE_FOLDER / doc_file_name
    if link not in docs_file.read_text():
        fail_pre_commit = True
        console.print()
        console.print(
            f"[red]ERROR! The {docs_file} does not contain:\n:[/]{link}\n[bright_blue]Please add it!"
        )
        console.print()


def get_provider_doc_folder(provider_id: str) -> Path:
    return AIRFLOW_DOC_FILES / f"apache-airflow-providers-{provider_id.replace('.', '-')}"


def has_executor_package_defined(provider_id: str):
    provider_sources = AIRFLOW_PROVIDERS_DIR / provider_id.replace(".", "/")
    for executors_folder in provider_sources.rglob("executors"):
        if executors_folder.is_dir() and (executors_folder / "__init__.py").is_file():
            return True
    return False


JINJA_LOADER = Environment(loader=BaseLoader())

for provider_id, provider_info in ALL_PROVIDERS.items():
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
        generated_content=LICENCE_CONTENT_RST
        + INSTALLING_PROVIDERS_FROM_SOURCES_CONTENT_RST,
    )

    check_provider_doc_exists_and_in_index(
        provider_id=provider_id,
        index_link="Changelog <changelog>",
        file_name="changelog.rst",
        generated_content=LICENCE_CONTENT_RST
        + JINJA_LOADER.from_string(CHANGELOG_CONTENT_RST).render(provider_id=provider_id),
    )

    if has_executor_package_defined(provider_id) and not provider_info.get("executors"):
        provider_yaml = (
            AIRFLOW_PROVIDERS_DIR / provider_id.replace(".", "/") / "provider.yaml"
        )
        console.print()
        console.print(
            f"[red]ERROR! The {provider_id} provider has executor package but "
            f"does not have `executors` defined in {provider_yaml}."
        )
        console.print(
            f"\nPlease add executor class to `executors` array in {provider_yaml}"
        )
        fail_pre_commit = True

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
                link=f"and related CLI commands: :doc:`{get_provider_doc_folder(provider_id).name}:cli-ref`",
                doc_file_name="cli-and-env-variables-ref.rst",
            )
    if provider_info.get("config"):
        check_provider_doc_exists_and_in_index(
            provider_id=provider_id,
            index_link="Configuration <configurations-ref>",
            file_name="configurations-ref.rst",
            generated_content=LICENCE_CONTENT_RST + CONFIGURATION_CONTENT_RST,
        )
if fail_pre_commit:
    sys.exit(1)

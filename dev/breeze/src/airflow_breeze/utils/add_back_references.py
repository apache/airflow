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
import re
import tempfile
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from airflow_breeze.utils.console import get_console

airflow_redirects_link = (
    "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/redirects.txt"
)
helm_redirects_link = "https://raw.githubusercontent.com/apache/airflow/main/docs/helm-chart/redirects.txt"


def download_file(url):
    try:
        temp_dir = Path(tempfile.mkdtemp(prefix="temp_dir", suffix=""))
        file_name = temp_dir / "redirects.txt"
        filedata = urlopen(url)
        data = filedata.read()
        file_name.write_bytes(data)
        return True, file_name
    except URLError as e:
        if e.reason == "Not Found":
            get_console().print(f"[blue]The {url} does not exist. Skipping.")
        else:
            get_console().print(f"[yellow]Could not download file {url}: {e}")
        return False, "no-file"


def construct_old_to_new_tuple_mapping(file_name: Path) -> list[tuple[str, str]]:
    old_to_new_tuples: list[tuple[str, str]] = []
    with file_name.open() as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                old_path, new_path = line.split(" ")
                old_path = old_path.replace(".rst", ".html")
                new_path = new_path.replace(".rst", ".html")
                old_to_new_tuples.append((old_path, new_path))
    return old_to_new_tuples


def get_redirect_content(url: str):
    return f'<html><head><meta http-equiv="refresh" content="0; url={url}"/></head></html>'


def get_github_redirects_url(provider_name: str):
    return f"https://raw.githubusercontent.com/apache/airflow/main/docs/{provider_name}/redirects.txt"


def crete_redirect_html_if_not_exist(path: Path, content: str):
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        get_console().print(f"[green]Created back reference redirect: {path}")
    else:
        get_console().print(f"Skipping file:{path}, redirects already exist")


def create_back_reference_html(back_ref_url: str, target_path: Path):
    content = get_redirect_content(back_ref_url)

    version_match = re.compile(r"[0-9]+.[0-9]+.[0-9]+")
    target_path_as_posix = target_path.as_posix()
    if "/stable/" in target_path_as_posix:
        prefix, postfix = target_path_as_posix.split("/stable/")
        base_folder = Path(prefix)
        for folder in base_folder.iterdir():
            if folder.is_dir() and version_match.match(folder.name):
                crete_redirect_html_if_not_exist(folder / postfix, content)
    else:
        crete_redirect_html_if_not_exist(Path(target_path), content)


def generate_back_references(link: str, base_path: Path):
    if not base_path.exists():
        get_console().print("[blue]The provider is not yet released.Skipping.")
        return
    is_downloaded, file_name = download_file(link)
    if not is_downloaded:
        old_to_new: list[tuple[str, str]] = []
    else:
        get_console().print(f"Constructs old to new mapping from redirects.txt for {base_path}")
        old_to_new = construct_old_to_new_tuple_mapping(file_name)
    old_to_new.append(("index.html", "changelog.html"))
    old_to_new.append(("index.html", "security.html"))
    old_to_new.append(("security.html", "security/security-model.html"))

    for versioned_provider_path in (p for p in base_path.iterdir() if p.is_dir()):
        get_console().print(f"Processing {base_path}, version: {versioned_provider_path.name}")

        for old, new in old_to_new:
            # only if old file exists, add the back reference
            if (versioned_provider_path / old).exists():
                if "/" in new:
                    split_new_path, file_name = new.rsplit("/", 1)
                    dest_dir = versioned_provider_path / split_new_path
                else:
                    file_name = new
                    dest_dir = versioned_provider_path
                # finds relative path of old file with respect to new and handles case of different file
                # names also
                relative_path = os.path.relpath(old, new)
                # remove one directory level because file path was used above
                relative_path = relative_path.replace("../", "", 1)
                os.makedirs(dest_dir, exist_ok=True)
                dest_file_path = dest_dir / file_name
                create_back_reference_html(relative_path, dest_file_path)


def start_generating_back_references(airflow_site_directory: Path, short_provider_package_ids: list[str]):
    docs_archive_path = airflow_site_directory / "docs-archive"
    airflow_docs_path = docs_archive_path / "apache-airflow"
    helm_docs_path = docs_archive_path / "helm-chart"
    if "apache-airflow" in short_provider_package_ids:
        generate_back_references(airflow_redirects_link, airflow_docs_path)
        short_provider_package_ids.remove("apache-airflow")
    if "helm-chart" in short_provider_package_ids:
        generate_back_references(helm_redirects_link, helm_docs_path)
        short_provider_package_ids.remove("helm-chart")
    if "docker-stack" in short_provider_package_ids:
        get_console().print("[info]Skipping docker-stack package. No back-reference needed.")
        short_provider_package_ids.remove("docker-stack")
    if "apache-airflow-providers" in short_provider_package_ids:
        get_console().print("[info]Skipping apache-airflow-providers package. No back-reference needed.")
        short_provider_package_ids.remove("apache-airflow-providers")
    if short_provider_package_ids:
        all_providers = [
            f"apache-airflow-providers-{package.replace('.','-')}" for package in short_provider_package_ids
        ]
        for p in all_providers:
            get_console().print(f"Processing airflow provider: {p}")
            generate_back_references(get_github_redirects_url(p), docs_archive_path / p)

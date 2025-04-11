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

import concurrent
import concurrent.futures
import datetime
import itertools
import os
import shutil
import sys
import traceback
from collections.abc import Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile

import requests
import urllib3.exceptions
from requests.adapters import DEFAULT_POOLSIZE
from rich import print

from airflow.utils.helpers import partition
from sphinx_exts.docs_build.docs_builder import get_available_providers_distributions
from sphinx_exts.docs_build.third_party_inventories import THIRD_PARTY_INDEXES

AIRFLOW_ROOT_PATH = Path(os.path.abspath(__file__)).parents[4]
GENERATED_PATH = AIRFLOW_ROOT_PATH / "generated"
CACHE_PATH = GENERATED_PATH / "_inventory_cache"

S3_DOC_URL = "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com"
S3_DOC_URL_VERSIONED = S3_DOC_URL + "/docs/{package_name}/stable/objects.inv"
S3_DOC_URL_NON_VERSIONED = S3_DOC_URL + "/docs/{package_name}/objects.inv"


def _fetch_file(session: requests.Session, package_name: str, url: str, path: str) -> tuple[str, bool]:
    """
    Download a file, validate Sphinx Inventory headers and returns status information as a tuple with package
    name and success status(bool value).
    """
    try:
        response = session.get(url, allow_redirects=True, stream=True)
    except (requests.RequestException, urllib3.exceptions.HTTPError):
        print(f"{package_name}: Failed to fetch inventory: {url}")
        traceback.print_exc(file=sys.stderr)
        return package_name, False
    if not response.ok:
        print(f"{package_name}: Failed to fetch inventory: {url}")
        print(f"{package_name}: Failed with status: {response.status_code}", file=sys.stderr)
        return package_name, False

    if response.url != url:
        print(f"{package_name}: {url} redirected to {response.url}")

    with NamedTemporaryFile(suffix=package_name, mode="wb+") as tf:
        for chunk in response.iter_content(chunk_size=4096):
            tf.write(chunk)

        tf.flush()
        tf.seek(0, 0)

        line = tf.readline().decode()
        if not line.startswith("# Sphinx inventory version"):
            print(f"{package_name}: Response contain unexpected Sphinx Inventory header: {line!r}.")
            return package_name, False

        tf.seek(0, 0)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            shutil.copyfileobj(tf, f)

    print(f"{package_name}: Fetched inventory: {response.url}")
    return package_name, True


def _is_outdated(path: str):
    if not os.path.exists(path):
        return True
    delta = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(path))
    return delta > datetime.timedelta(hours=12)


def fetch_inventories(clean_build: bool) -> list[str]:
    """Fetch all inventories for Airflow documentation packages and store in cache."""
    if clean_build:
        shutil.rmtree(CACHE_PATH)
        print()
        print("[yellow]Inventory Cache cleaned!")
        print()
    CACHE_PATH.mkdir(exist_ok=True, parents=True)
    to_download: list[tuple[str, str, str]] = []

    for pkg_name in get_available_providers_distributions():
        to_download.append(
            (
                pkg_name,
                S3_DOC_URL_VERSIONED.format(package_name=pkg_name),
                (CACHE_PATH / pkg_name / "objects.inv").as_posix(),
            )
        )
    for pkg_name in ["apache-airflow", "helm-chart"]:
        to_download.append(
            (
                pkg_name,
                S3_DOC_URL_VERSIONED.format(package_name=pkg_name),
                (CACHE_PATH / pkg_name / "objects.inv").as_posix(),
            )
        )
    for pkg_name in ["apache-airflow-providers", "docker-stack"]:
        to_download.append(
            (
                pkg_name,
                S3_DOC_URL_NON_VERSIONED.format(package_name=pkg_name),
                (CACHE_PATH / pkg_name / "objects.inv").as_posix(),
            )
        )
    to_download.extend(
        (
            pkg_name,
            f"{doc_url}/objects.inv",
            (CACHE_PATH / pkg_name / "objects.inv").as_posix(),
        )
        for pkg_name, doc_url in THIRD_PARTY_INDEXES.items()
    )

    to_download = [(pkg_name, url, path) for pkg_name, url, path in to_download if _is_outdated(path)]
    if not to_download:
        print("Nothing to do")
        return []

    print(f"To download {len(to_download)} inventorie(s)")

    with requests.Session() as session, concurrent.futures.ThreadPoolExecutor(DEFAULT_POOLSIZE) as pool:
        download_results: Iterator[tuple[str, bool]] = pool.map(
            _fetch_file,
            itertools.repeat(session, len(to_download)),
            (pkg_name for pkg_name, _, _ in to_download),
            (url for _, url, _ in to_download),
            (path for _, _, path in to_download),
        )
    failed, success = partition(lambda d: d[1], download_results)
    failed, success = list(failed), list(success)
    print(f"Result: {len(success)} success, {len(failed)} failed")
    if failed:
        terminate = False
        print("Failed packages:")
        for pkg_no, (pkg_name, _) in enumerate(failed, start=1):
            print(f"{pkg_no}. {pkg_name}")
            if not terminate and not pkg_name.startswith("apache-airflow"):
                # For solve situation that newly created Community Provider doesn't upload inventory yet.
                # And we terminate execution only if any error happen during fetching
                # third party intersphinx inventories.
                terminate = True
        if terminate:
            print("Terminate execution.")
            raise SystemExit(1)

    return [pkg_name for pkg_name, status in failed]


if __name__ == "__main__":
    fetch_inventories(clean_build=len(sys.argv) > 1 and sys.argv[1] == "clean")

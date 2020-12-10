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

import concurrent
import concurrent.futures
import datetime
import os
import shutil

import requests
from requests.adapters import DEFAULT_POOLSIZE

from docs.exts.docs_build.docs_builder import (  # pylint: disable=no-name-in-module
    get_available_providers_packages,
)

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir, os.pardir, os.pardir))
DOCS_DIR = os.path.join(ROOT_DIR, 'docs')
CACHE_DIR = os.path.join(DOCS_DIR, '_inventory_cache')
EXPIRATION_DATE_PATH = os.path.join(DOCS_DIR, '_inventory_cache', "expiration-date")

S3_DOC_URL = "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com"
S3_DOC_URL_VERSIONED = S3_DOC_URL + "/docs/{package_name}/latest/objects.inv"
S3_DOC_URL_NON_VERSIONED = S3_DOC_URL + "/docs/{package_name}/objects.inv"


def _fetch_file(session: requests.Session, url: str, path: str):

    response = session.get(url, allow_redirects=True, stream=True)
    if not response.ok:
        print(f"Failed to fetch inventory: {url}")
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        response.raw.decode_content = True
        shutil.copyfileobj(response.raw, f)
    print(f"Fetched inventory: {url}")


def _is_outdated(path: str):
    delta = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(path))
    return delta < datetime.timedelta(hours=12)


def fetch_inventories():
    """Fetch all inventories for Airflow documentatio packages and store in cache."""
    os.makedirs(os.path.dirname(CACHE_DIR), exist_ok=True)
    to_download = []

    for pkg_name in get_available_providers_packages():
        to_download.append(
            (
                S3_DOC_URL_VERSIONED.format(package_name=pkg_name),
                f'{CACHE_DIR}/{pkg_name}/objects.inv',
            )
        )
    to_download.append(
        (
            S3_DOC_URL_VERSIONED.format(package_name='apache-airflow'),
            f'{CACHE_DIR}/apache-airflow/objects.inv',
        )
    )
    to_download.append(
        (
            S3_DOC_URL_NON_VERSIONED.format(package_name='apache-airflow-providers'),
            f'{CACHE_DIR}/apache-airflow-providers/objects.inv',
        )
    )
    to_download = [
        (url, path) for url, path in to_download if not (os.path.isfile(path) and _is_outdated(path))
    ]
    print(f"To download {len(to_download)} inventorie(s)")
    if not to_download:
        return
    with requests.Session() as session, concurrent.futures.ThreadPoolExecutor(DEFAULT_POOLSIZE) as pool:
        for url, path in to_download:
            pool.submit(_fetch_file, session=session, url=url, path=path)

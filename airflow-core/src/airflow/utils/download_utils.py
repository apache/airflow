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

import logging
import platform
from pathlib import Path

import requests

log = logging.getLogger(__name__)


def download_file_from_github(
    version: str, output_file: Path, github_token: str | None = None, timeout: int = 60
) -> bool:
    """
    Download a file from the GitHub repository of Mprocs using the GitHub API.

    In case of any error different from 404, it will exit the process with error code 1.

    :param version: version to download
    :param output_file: Path where the file should be downloaded
    :param github_token: GitHub token to use for authentication
    :param timeout: timeout in seconds for the download request, default is 60 seconds
    :return: whether the file was successfully downloaded (False if the file is missing)
    """
    import requests

    url = get_url_from_version(version=version)
    if not url:
        return False
    log.info("Downloading %s to %s", url, output_file.as_uri)
    headers = {"Accept": "application/octet-stream"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code == 403:
            log.error(
                "Access denied to %s. This may be caused by:\n"
                "   1. Network issues or VPN settings\n"
                "   2. GitHub API rate limiting\n"
                "   3. Invalid or missing GitHub token",
                url,
            )
            return False
        if response.status_code == 404:
            log.warning("The %s has not been found. Skipping", url)
            return False
        if response.status_code != 200:
            log.error("%s could not be downloaded. Status code %d", url, response.status_code)
            return False
        output_file.write_bytes(response.content)
    except requests.Timeout:
        log.error("The request to %s timed out after %d seconds.", url, timeout)
        return False
    log.info("Downloaded %s to %s", url, output_file.as_uri)
    return True


def get_url_from_version(version: str, github_token: str | None = None, timeout: int = 60) -> str:
    headers = {}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    url = f"https://api.github.com/repos/pvolok/mprocs/releases/tags/{version}"
    asset_url = ""
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code != 200:
            return asset_url
        response_json = response.json()
        asset_number = -1
        system = platform.uname().system.lower()
        architecture = platform.uname().machine.lower()
        for asset in response_json["assets"]:
            if system in asset["name"] and architecture in asset["name"]:
                asset_number = asset["id"]
                break
        if asset_number > 0:
            asset_url = f"https://api.github.com/repos/pvolok/mprocs/releases/assets/{asset_number}"
            return asset_url
    except Exception:
        log.error("Unable to find version %s of mprocs, failing", version)
        return asset_url

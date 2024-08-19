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


def strip_leading_zeros_from_version(version: str) -> str:
    """
    Strips leading zeros from version number.

    This converts 1974.04.03 to 1974.4.3 as the format with leading month and day zeros is not accepted
    by PIP versioning.

    :param version: version number in CALVER format (potentially with leading 0s in date and month)
    :return: string with leading 0s after dot replaced.
    """
    return ".".join(i.lstrip("0") or "0" for i in version.split("."))


def get_version_tag(version: str, provider_package_id: str, version_suffix: str = ""):
    if version_suffix is None:
        version_suffix = ""
    return f"providers-{provider_package_id.replace('.','-')}/{version}{version_suffix}"


def is_pre_release(version: str) -> bool:
    from packaging.version import Version

    return Version(version).is_prerelease

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


def remove_local_version_suffix(version_suffix: str) -> str:
    if "+" in version_suffix:
        return version_suffix.split("+")[0]
    return version_suffix


def is_local_package_version(version_suffix: str) -> bool:
    """
    Check if the given version suffix is a local version suffix. A local version suffix will contain a
    plus sign ('+'). This function does not guarantee that the version suffix is a valid local version suffix.

    Args:
        version_suffix (str): The version suffix to check.

    Returns:
        bool: True if the version suffix contains a '+', False otherwise. Please note this does not
        guarantee that the version suffix is a valid local version suffix.
    """
    if version_suffix and ("+" in version_suffix):
        return True
    return False

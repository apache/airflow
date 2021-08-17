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

import fnmatch
from typing import List, Optional


def process_package_filters(available_packages: List[str], package_filters: Optional[List[str]]):
    """Filters the package list against a set of filters.

    A packet is returned if it matches at least one filter. The function keeps the order of the packages.
    """
    if not package_filters:
        return available_packages

    invalid_filters = [
        f for f in package_filters if not any(fnmatch.fnmatch(p, f) for p in available_packages)
    ]
    if invalid_filters:
        raise SystemExit(
            f"Some filters did not find any package: {invalid_filters}, Please check if they are correct."
        )

    return [p for p in available_packages if any(fnmatch.fnmatch(p, f) for f in package_filters)]

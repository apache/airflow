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

import warnings

try:
    import importlib_metadata
except ImportError:
    from importlib import metadata as importlib_metadata  # type: ignore[no-redef]

import pytest


@pytest.fixture(scope="session")
def botocore_version():
    try:
        version = importlib_metadata.version("botocore")
    except importlib_metadata.PackageNotFoundError:
        warnings.warn("'botocore' package not found'", UserWarning)
        return None

    try:
        return tuple(map(int, version.split(".")[:3]))
    except Exception:
        warnings.warn(f"Unable to parse botocore {version!r}", UserWarning)
        return None


@pytest.fixture(autouse=True)
def filter_botocore_warnings(botocore_version):
    """Filter known botocore future warnings."""

    with warnings.catch_warnings():
        if botocore_version and botocore_version < (1, 29):
            # By default, for some clients botocore use deprecated endpoints `{region}.{service}.{dnsSuffix}`
            # In botocore 1.29 it will be replaced by `{service}.{region}.{dnsSuffix}`
            # and the warning should gone
            # See: https://github.com/boto/botocore/issues/2705
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                module="botocore.client",
                message="The .* client is currently using a deprecated endpoint.*",
            )
        yield

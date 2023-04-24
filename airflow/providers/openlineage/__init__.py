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

__all__ = ["version"]

try:
    import importlib_metadata as metadata
except ImportError:
    from importlib import metadata  # type: ignore[no-redef]

try:
    version = metadata.version("apache-airflow-providers-openlineage")
except metadata.PackageNotFoundError:
    import logging

    log = logging.getLogger(__name__)
    log.warning("Package metadata could not be found. Overriding it with version found in setup.py")
    # TODO: What should be a proper fallback?
    # If hardcoded version from provider version
    # there's no point to use metadata above
    version = "1.0.0.dev"

del metadata

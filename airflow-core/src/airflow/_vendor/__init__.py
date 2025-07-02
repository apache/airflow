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
from importlib.abc import MetaPathFinder
from importlib.util import spec_from_file_location
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import ModuleType


class _NonVendoredAirflowSharedFinder(MetaPathFinder):
    """
    A Python import PathFinder designed to load "airflow._vendor.airflow_shared" and it's submodules from
    the shared lib instead

    Only used during dev, not installed in production
    """

    dir: str

    def __init__(self, dir: str) -> None:
        self.dir = dir
        super().__init__()

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ):
        if fullname != f"{__name__}.airflow_shared":
            return

        return spec_from_file_location(fullname, os.path.join(self.dir, "airflow_shared", "__init__.py"))


def _install_loader():
    # Add a new module Finder to find the imports when the vendored sources aren't found. We only add it when
    # something that looks like the airflow repo layout is already in the sys path i.e. when we are running in
    # a repo checkout.
    #
    # We need to add this to the dir first as we now keep the `.pyi` files for the vendored libs, and
    for dir in sys.path:
        if dir.endswith("shared/core-and-tasksdk/src"):
            sys.meta_path.insert(0, _NonVendoredAirflowSharedFinder(dir))
            break


if __debug__:
    # i.e. we are not running with PYTHONOPTMIZE. This is almost never set, but if someone _does_ run it, we
    # def don't want to load code from the wrong place
    _install_loader()

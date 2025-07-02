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

# Please see airflow-core/src/_vendor/__init__.py for docs and and reasoning for this loader

class _NonVendoredAirflowSharedFinder(MetaPathFinder):

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
    for dir in sys.path:
        if dir.endswith("shared/core-and-tasksdk/src"):
            sys.meta_path.insert(0, _NonVendoredAirflowSharedFinder(dir))
            break


if __debug__:
    _install_loader()

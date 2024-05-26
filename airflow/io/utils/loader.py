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

import importlib
import marshal
import os.path
import sys
from importlib.abc import MetaPathFinder, SourceLoader
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import Any

from airflow.io.path import ObjectStoragePath


class FSSpecLoader(SourceLoader):
    """Create a FSSpecLoader that allows loading modules from remote locations and caches locally."""

    def __init__(self, base_uri: str | Path, cache_dir: str):
        self.base_uri = base_uri
        self.cache_dir = cache_dir

        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    def get_data(self, path):
        osp = ObjectStoragePath(path)
        return osp.read_bytes()

    def get_filename(self, fullname):
        return self._get_module_path(fullname)

    def create_module(self, spec) -> ModuleType | None:
        return None

    def exec_module(self, module):
        code = self.get_code(module.__name__)
        exec(code, module.__dict__)

    def get_code(self, fullname: str) -> Any:
        source_path = self._get_module_path(fullname)
        cache_path = self._get_cache_path(fullname)
        source_mtime = self._get_mtime(source_path)

        pyc_remote = self._get_remote_cache_path(source_path)
        if pyc_remote.exists():
            pyc_mtime = self._get_mtime(pyc_remote)
            if pyc_mtime >= source_mtime:
                with open(pyc_remote, "rb") as f:
                    code = marshal.load(f)
                return code

        if os.path.exists(cache_path):
            cache_mtime = os.path.getmtime(cache_path)
            if cache_mtime >= source_mtime:
                with open(cache_path, "rb") as f:
                    code = marshal.load(f)
                return code

        if not source_path.exists():
            raise ImportError(f"Cannot find module {fullname}")

        if self.is_package(fullname):
            init_file = source_path.joinpath("__init__.py")
            if not init_file.exists():
                raise ImportError(f"Cannot find module {fullname}")
            source_data = init_file.read_bytes()
        elif source_path.is_file():
            source_data = source_path.read_bytes()
        else:
            raise ImportError(f"Cannot find module {fullname}")

        code = self.source_to_code(source_data, source_path.path)
        with open(cache_path, "wb") as f:
            marshal.dump(code, f)
        return code

    def _get_cache_path(self, fullname: str) -> str:
        version_tag = f"cpython-{sys.version_info.major}{sys.version_info.minor}"

        if self.is_package(fullname):
            fullname = fullname + ".__init__"

        filename = fullname.replace(".", "_")

        return os.path.join(self.cache_dir, f"{filename}.{version_tag}.pyc")

    def _get_module_path(self, fullname) -> ObjectStoragePath:
        parts = fullname.split(".")
        path = self.base_uri
        for part in parts:
            path = os.path.join(path, part)
        osp = ObjectStoragePath(path).with_suffix(".py")
        if osp.exists():
            return osp
        osp = ObjectStoragePath(path)
        if osp.is_dir():
            return osp
        raise ImportError(f"Cannot find module {fullname}")

    def _get_remote_cache_path(self, source_path: ObjectStoragePath) -> ObjectStoragePath:
        version_tag = f"cpython-{sys.version_info.major}{sys.version_info.minor}"

        if source_path.is_dir():
            return source_path.joinpath(f"__init__.{version_tag}.pyc")

        return source_path.with_suffix(f".{version_tag}.pyc")

    def _get_mtime(self, path: ObjectStoragePath) -> int:
        if path.exists():
            return path.stat().st_mtime
        else:
            return 0

    def is_package(self, fullname):
        parts = fullname.split(".")
        osp = ObjectStoragePath(self.base_uri).joinpath(*parts)
        init_file = osp.joinpath("__init__.py")
        return osp.is_dir() and init_file.exists()


class PathLoader(importlib.abc.Loader):
    """A loader that loads a module from a pathlib.Path."""

    def __init__(self, fullname: str, path: Path):
        self.fullname = fullname
        self.path: Path = path

    def create_module(self, spec) -> ModuleType | None:
        # Use default module creation semantics
        return None

    def exec_module(self, module):
        code = self.path.read_text()
        module.__file__ = str(self.path)
        exec(code, module.__dict__)


class FSSpecFinder(MetaPathFinder):
    """A SpecFinder that allows loading modules from remote locations."""

    def __init__(self, base_uri: ObjectStoragePath | str, cache_dir: str):
        self.base_uri = base_uri
        self.cache_dir = cache_dir
        self.fs = ObjectStoragePath(base_uri).fs  # ensures eager initialization

    def find_spec(self, fullname, path, target=None):
        loader = FSSpecLoader(self.base_uri, self.cache_dir)
        try:
            return ModuleSpec(fullname, loader)
        except ImportError:
            return None

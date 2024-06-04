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

import functools
import itertools
import json
import os
import site
import sys
import warnings
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Generator

import pytest
from typing_extensions import Literal

WhenTypeDef = Literal["config", "collect", "runtest"]
TESTS_DIR = Path(__file__).parents[1].resolve()


@functools.lru_cache(maxsize=None)
def _sites_locations() -> tuple[str, ...]:
    return tuple([*site.getsitepackages(), site.getusersitepackages()])


@functools.lru_cache(maxsize=None)
def _resolve_warning_filepath(path: str, rootpath: str):
    if path.startswith(_sites_locations()):
        for site_loc in _sites_locations():
            if path.startswith(site_loc):
                return path[len(site_loc) :].lstrip(os.sep)
    elif path.startswith(rootpath):
        return path[len(rootpath) :].lstrip(os.sep)
    return path


@dataclass(frozen=True, unsafe_hash=True)
class CapturedWarning:
    category: str
    message: str
    filename: str
    lineno: int
    when: WhenTypeDef
    node_id: str | None = None
    param_id: str | None = None

    @classmethod
    def from_record(
        cls, warning_message: warnings.WarningMessage, root_path: Path, node_id: str | None, when: WhenTypeDef
    ) -> CapturedWarning:
        category = warning_message.category.__name__
        if (category_module := warning_message.category.__module__) != "builtins":
            category = f"{category_module}.{category}"
        param_id = None
        if node_id:
            # Remove parametrized part from the test node
            node_id, _, param_part = node_id.partition("[")
            if param_part:
                param_id = param_part[:-1] or None
        return cls(
            category=category,
            message=str(warning_message.message),
            node_id=node_id,
            param_id=param_id,
            when=when,
            filename=_resolve_warning_filepath(warning_message.filename, os.fspath(root_path)),
            lineno=warning_message.lineno,
        )

    @classmethod
    @contextmanager
    def capture_warnings(
        cls, when: WhenTypeDef, root_path: Path, node_id: str | None = None
    ) -> Generator[list[CapturedWarning], None, None]:
        captured_records: list[CapturedWarning] = []
        try:
            with warnings.catch_warnings(record=True) as records:
                if not sys.warnoptions:
                    warnings.filterwarnings("always", category=DeprecationWarning, append=True)
                    warnings.filterwarnings("always", category=PendingDeprecationWarning, append=True)
                yield captured_records
        finally:
            captured_records.extend(
                cls.from_record(rec, root_path=root_path, node_id=node_id, when=when) for rec in records
            )

    @property
    def uniq_key(self):
        return self.category, self.message, self.lineno, self.lineno

    @property
    def group(self) -> str:
        """
        Determine in which type of files warning raises.

        It depends on ``stacklevel`` in ``warnings.warn``, and if it is not set correctly,
        it might refer to another file.
        There is an assumption that airflow and all dependencies set it correct eventually.
        But we should not use it to filter it out, only for show in different groups.
        """
        if self.filename.startswith("airflow/"):
            if self.filename.startswith("airflow/providers/"):
                return "providers"
            return "airflow"
        elif self.filename.startswith("tests/"):
            return "tests"
        return "other"

    def dumps(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def loads(cls, obj: str) -> CapturedWarning:
        return cls(**json.loads(obj))

    def output(self, count: int) -> str:
        return json.dumps({**asdict(self), "group": self.group, "count": count})


class CaptureWarningsPlugin:
    """Internal plugin for capture warnings during the tests run."""

    node_key: str = "capture_warnings_node"

    def __init__(self, config: pytest.Config, output_path: str | None = None):
        output_path = output_path or os.environ.get("CAPTURE_WARNINGS_OUTPUT") or "warnings.txt"
        warning_output_path = Path(os.path.expandvars(os.path.expandvars(output_path)))
        if not warning_output_path.is_absolute():
            warning_output_path = TESTS_DIR.joinpath(output_path)

        self.warning_output_path = warning_output_path
        self.config = config
        self.root_path = config.rootpath
        self.is_worker_node = hasattr(config, "workerinput")
        self.captured_warnings: dict[CapturedWarning, int] = {}

    def add_captured_warnings(self, cap_warning: list[CapturedWarning]) -> None:
        for cw in cap_warning:
            if cw not in self.captured_warnings:
                self.captured_warnings[cw] = 1
            else:
                self.captured_warnings[cw] += 1

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_collection(self, session: pytest.Session):
        with CapturedWarning.capture_warnings("collect", self.root_path, None) as records:
            yield
        self.add_captured_warnings(records)

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_load_initial_conftests(self, early_config: pytest.Config):
        with CapturedWarning.capture_warnings("collect", self.root_path, None) as records:
            yield
        self.add_captured_warnings(records)

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_runtest_protocol(self, item: pytest.Item):
        with CapturedWarning.capture_warnings("runtest", self.root_path, item.nodeid) as records:
            yield
        self.add_captured_warnings(records)

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int):
        """Save warning captures in the session finish on xdist worker node"""
        with CapturedWarning.capture_warnings("config", self.root_path, None) as records:
            yield
        self.add_captured_warnings(records)
        if self.is_worker_node and self.captured_warnings and hasattr(self.config, "workeroutput"):
            self.config.workeroutput[self.node_key] = tuple(
                [(cw.dumps(), count) for cw, count in self.captured_warnings.items()]
            )

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodedown(self, node, error):
        """Get warning captures from the xdist worker node."""
        if not (workeroutput := getattr(node, "workeroutput", {})):
            return

        node_captured_warnings: tuple[tuple[str, int]] = workeroutput.get(self.node_key)
        if not node_captured_warnings:
            return

        for cw_ser, count in node_captured_warnings:
            if (cw := CapturedWarning.loads(cw_ser)) in self.captured_warnings:
                self.captured_warnings[cw] += count
            else:
                self.captured_warnings[cw] = count

    @staticmethod
    def sorted_groupby(it, grouping_key: Callable):
        """Helper for sort and group by."""
        for group, grouped_data in itertools.groupby(sorted(it, key=grouping_key), key=grouping_key):
            yield group, list(grouped_data)

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_terminal_summary(self, terminalreporter, exitstatus: int, config: pytest.Config):
        with CapturedWarning.capture_warnings("collect", self.root_path, None) as records:
            yield
        self.add_captured_warnings(records)

        if self.is_worker_node:  # No need to print/write file on worker node
            return

        if self.warning_output_path.exists():  # Cleanup file.
            self.warning_output_path.open("w").close()

        if not self.captured_warnings:
            return

        if not self.warning_output_path.parent.exists():
            self.warning_output_path.parent.mkdir(parents=True, exist_ok=True)

        terminalreporter.section(
            f"Warning summary. Total: {sum(self.captured_warnings.values()):,}, "
            f"Unique: {len({cw.uniq_key for cw in self.captured_warnings}):,}",
            yellow=True,
            bold=True,
        )
        for group, grouped_data in self.sorted_groupby(self.captured_warnings.items(), lambda x: x[0].group):
            color = {}
            if group in ("airflow", "providers"):
                color["red"] = True
            elif group == "tests":
                color["yellow"] = True
            else:
                color["white"] = True
            terminalreporter.write(group, bold=True, **color)
            terminalreporter.write(
                f": total {sum(item[1] for item in grouped_data):,}, "
                f"unique {len({item[0].uniq_key for item in grouped_data}):,}\n"
            )
            for when, when_data in self.sorted_groupby(grouped_data, lambda x: x[0].when):
                terminalreporter.write(
                    f"  {when}: total {sum(item[1] for item in when_data):,}, "
                    f"unique {len({item[0].uniq_key for item in when_data}):,}\n"
                )

        with self.warning_output_path.open("w") as fp:
            for cw, count in self.captured_warnings.items():
                fp.write(f"{cw.output(count)}\n")
        terminalreporter.write("Warnings saved into ")
        terminalreporter.write(os.fspath(self.warning_output_path), yellow=True)
        terminalreporter.write(" file.\n")

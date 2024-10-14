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
from collections.abc import Sequence
from pathlib import Path

import pytest
import yaml


class ForbiddenWarningsPlugin:
    """Internal plugin for restricting warnings during the tests run."""

    node_key: str = "forbidden_warnings_node"
    deprecations_ignore: Sequence[str | os.PathLike]

    def __init__(self, config: pytest.Config, forbidden_warnings: tuple[str, ...]):
        # Set by a pytest_configure hook in conftest
        deprecations_ignore = config.inicfg["airflow_deprecations_ignore"]
        if isinstance(deprecations_ignore, (str, os.PathLike)):
            self.deprecations_ignore = [deprecations_ignore]
        else:
            self.deprecations_ignore = deprecations_ignore

        excluded_cases = {
            # Skip: Integration and System Tests
            "tests/integration/",
            "tests/system/",
            "providers/tests/integration/",
            "providers/tests/system/",
            # Skip: DAGs for tests
            "tests/dags/",
            "tests/dags_corrupted/",
            "tests/dags_with_system_exit/",
        }
        for path in self.deprecations_ignore:
            path = Path(path).resolve()
            with path.open() as fp:
                excluded_cases.update(yaml.safe_load(fp))

        self.config = config
        self.forbidden_warnings = forbidden_warnings
        self.is_worker_node = hasattr(config, "workerinput")
        self.detected_cases: set[str] = set()
        self.excluded_cases: tuple[str, ...] = tuple(sorted(excluded_cases))

    @staticmethod
    def prune_params_node_id(node_id: str) -> str:
        """Remove parametrized parts from node id."""
        return node_id.partition("[")[0]

    def pytest_itemcollected(self, item: pytest.Item):
        if item.nodeid.startswith(self.excluded_cases):
            return
        for fw in self.forbidden_warnings:
            # Add marker at the beginning of the markers list. In this case, it does not conflict with
            # filterwarnings markers, which are set explicitly in the test suite.
            item.add_marker(pytest.mark.filterwarnings(f"error::{fw}"), append=False)
        item.add_marker(
            pytest.mark.filterwarnings(
                "ignore:Timer and timing metrics publish in seconds were deprecated. It is enabled by default from Airflow 3 onwards. Enable metrics consistency to publish all the timer and timing metrics in milliseconds.:DeprecationWarning"
            )
        )

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int):
        """Save set of test node ids in the session finish on xdist worker node"""
        yield
        if self.is_worker_node and self.detected_cases and hasattr(self.config, "workeroutput"):
            self.config.workeroutput[self.node_key] = frozenset(self.detected_cases)

    @pytest.hookimpl(optionalhook=True)
    def pytest_testnodedown(self, node, error):
        """Get a set of test node ids from the xdist worker node."""
        if not (workeroutput := getattr(node, "workeroutput", {})):
            return

        node_detected_cases: tuple[tuple[str, int]] = workeroutput.get(self.node_key)
        if not node_detected_cases:
            return

        self.detected_cases |= node_detected_cases

    def pytest_exception_interact(self, node: pytest.Item, call: pytest.CallInfo, report: pytest.TestReport):
        if not call.excinfo or call.when not in ["setup", "call", "teardown"]:
            # Skip analyze exception if there is no exception exists
            # or exception happens outside of tests or fixtures
            return

        exc = call.excinfo.type
        exception_qualname = exc.__name__
        if (exception_module := exc.__module__) != "builtins":
            exception_qualname = f"{exception_module}.{exception_qualname}"
        if exception_qualname in self.forbidden_warnings:
            self.detected_cases.add(node.nodeid)

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_terminal_summary(self, terminalreporter, exitstatus: int, config: pytest.Config):
        yield
        if not self.detected_cases or self.is_worker_node:  # No need to print report on worker node
            return

        total_cases = len(self.detected_cases)
        uniq_tests_cases = len(set(map(self.prune_params_node_id, self.detected_cases)))
        terminalreporter.section(f"{total_cases:,} prohibited warning(s) detected", red=True, bold=True)

        report_message = "By default selected warnings are prohibited during tests runs:\n * "
        report_message += "\n * ".join(self.forbidden_warnings)
        report_message += "\n\n"
        report_message += (
            "Please make sure that you follow Airflow Unit test developer guidelines:\n"
            "https://github.com/apache/airflow/blob/main/contributing-docs/testing/unit_tests.rst#handling-warnings"
        )
        if total_cases <= 20:
            # Print tests node ids only if there is a small amount of it,
            # otherwise it could turn into a mess in the terminal
            report_message += "\n\nWarnings detected in test case(s):\n - "
            report_message += "\n - ".join(sorted(self.detected_cases))
        if uniq_tests_cases >= 15:
            # If there are a lot of unique tests where this happens,
            # we might suggest adding it into the exclusion list
            report_message += (
                "\n\nIf this is significant change in code base you might add tests cases ids into the "
                f"{self.deprecations_ignore} file, please make sure that you also create "
                "follow up Issue/Task in https://github.com/apache/airflow/issues"
            )
        terminalreporter.write_line(report_message.rstrip(), red=True)
        terminalreporter.write_line(
            "You could disable this check by provide the `--disable-forbidden-warnings` flag, "
            "however this check always turned on in the Airflow's CI.",
            white=True,
        )

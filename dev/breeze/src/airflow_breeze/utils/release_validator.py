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

import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from airflow_breeze.utils.console import console_print


class CheckType(str, Enum):
    SVN = "svn"
    REPRODUCIBLE_BUILD = "reproducible-build"
    SIGNATURES = "signatures"
    CHECKSUMS = "checksums"
    LICENSES = "licenses"


@dataclass
class ValidationResult:
    check_type: CheckType
    passed: bool
    message: str
    details: list[str] | None = None
    duration_seconds: float | None = None


class ReleaseValidator(ABC):
    def __init__(
        self,
        version: str,
        svn_path: Path,
        airflow_repo_root: Path,
    ):
        self.version = version
        self.svn_path = svn_path
        self.airflow_repo_root = airflow_repo_root
        self.results: list[ValidationResult] = []

    @abstractmethod
    def get_distribution_name(self) -> str:
        pass

    @abstractmethod
    def get_svn_directory(self) -> Path:
        pass

    @abstractmethod
    def get_expected_files(self) -> list[str]:
        pass

    @abstractmethod
    def build_packages(self) -> bool:
        pass

    @abstractmethod
    def validate_svn_files(self) -> ValidationResult:
        pass

    @abstractmethod
    def validate_reproducible_build(self) -> ValidationResult:
        pass

    @abstractmethod
    def validate_signatures(self) -> ValidationResult:
        pass

    @abstractmethod
    def validate_checksums(self) -> ValidationResult:
        pass

    @abstractmethod
    def validate_licenses(self) -> ValidationResult:
        pass

    @property
    def check_methods(self) -> dict[CheckType, Callable]:
        return {
            CheckType.SVN: self.validate_svn_files,
            CheckType.REPRODUCIBLE_BUILD: self.validate_reproducible_build,
            CheckType.SIGNATURES: self.validate_signatures,
            CheckType.CHECKSUMS: self.validate_checksums,
            CheckType.LICENSES: self.validate_licenses,
        }

    def validate(self, checks: list[CheckType] | None = None) -> bool:
        if checks is None:
            checks = [
                CheckType.SVN,
                CheckType.REPRODUCIBLE_BUILD,
                CheckType.SIGNATURES,
                CheckType.CHECKSUMS,
                CheckType.LICENSES,
            ]

        console_print(f"\n[bold cyan]Validating {self.get_distribution_name()} {self.version}[/bold cyan]")
        console_print(f"SVN Path: {self.svn_path}")
        console_print(f"Airflow Root: {self.airflow_repo_root}")

        for check_type in checks:
            if check_type in self.check_methods:
                result = self.check_methods[check_type]()
                self.results.append(result)

        self._print_summary()
        return all(r.passed for r in self.results)

    def _print_result(self, result: ValidationResult):
        status = "[green]PASSED[/green]" if result.passed else "[red]FAILED[/red]"
        console_print(f"Status: {status} - {result.message}")

        if result.details:
            for detail in result.details:
                console_print(f"  {detail}")

        if result.duration_seconds:
            console_print(f"Duration: {result.duration_seconds:.1f}s")

    def _print_summary(self):
        console_print("\n" + "=" * 70)
        passed_count = sum(1 for r in self.results if r.passed)
        total_count = len(self.results)

        if passed_count == total_count:
            console_print(f"[bold green]ALL CHECKS PASSED ({passed_count}/{total_count})[/bold green]")
            console_print("\nYou can vote +1 (binding) on this release.")
        else:
            failed_count = total_count - passed_count
            console_print(
                f"[bold red]SOME CHECKS FAILED ({failed_count} failed, {passed_count} passed)[/bold red]"
            )
            console_print("\nFailed checks:")
            for result in self.results:
                if not result.passed:
                    console_print(f"  - {result.check_type.value}: {result.message}")
            console_print("\nPlease review failures above before voting.")

        total_duration = sum(r.duration_seconds or 0 for r in self.results)
        console_print(f"\nTotal validation time: {total_duration:.1f}s")
        console_print("=" * 70)

    def _strip_rc_suffix(self, version: str) -> str:
        return re.sub(r"rc\d+$", "", version)

    def _get_version_suffix(self) -> str:
        if "rc" in self.version:
            match = re.search(r"(rc\d+)$", self.version)
            if match:
                return match.group(1)
        return ""

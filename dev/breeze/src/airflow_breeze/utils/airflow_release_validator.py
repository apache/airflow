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

import filecmp
import shutil
import subprocess
import tarfile
import time
from pathlib import Path

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.release_validator import CheckType, ReleaseValidator, ValidationResult
from airflow_breeze.utils.run_utils import run_command


class AirflowReleaseValidator(ReleaseValidator):
    APACHE_RAT_JAR_DOWNLOAD_URL = (
        "https://downloads.apache.org/creadur/apache-rat-0.17/apache-rat-0.17-bin.tar.gz"
    )

    def __init__(
        self,
        version: str,
        svn_path: Path,
        airflow_repo_root: Path,
        task_sdk_version: str | None = None,
    ):
        super().__init__(version, svn_path, airflow_repo_root)
        self.task_sdk_version = task_sdk_version or version
        self.version_without_rc = self._strip_rc_suffix(version)
        self.task_sdk_version_without_rc = self._strip_rc_suffix(self.task_sdk_version)

    @property
    def expected_airflow_file_bases(self) -> list[str]:
        return [
            f"apache_airflow-{self.version_without_rc}-source.tar.gz",
            f"apache_airflow-{self.version_without_rc}.tar.gz",
            f"apache_airflow-{self.version_without_rc}-py3-none-any.whl",
            f"apache_airflow_core-{self.version_without_rc}.tar.gz",
            f"apache_airflow_core-{self.version_without_rc}-py3-none-any.whl",
        ]

    @property
    def expected_task_sdk_file_bases(self) -> list[str]:
        return [
            f"apache_airflow_task_sdk-{self.task_sdk_version_without_rc}.tar.gz",
            f"apache_airflow_task_sdk-{self.task_sdk_version_without_rc}-py3-none-any.whl",
        ]

    def get_distribution_name(self) -> str:
        return "Apache Airflow"

    def get_svn_directory(self) -> Path:
        return self.svn_path / self.version

    def get_task_sdk_svn_directory(self) -> Path:
        return self.svn_path / "task-sdk" / self.task_sdk_version

    def get_expected_files(self) -> list[str]:
        files = []
        for base in self.expected_airflow_file_bases:
            files.extend([base, f"{base}.asc", f"{base}.sha512"])

        return files

    def get_task_sdk_expected_files(self) -> list[str]:
        files = []

        for base in self.expected_task_sdk_file_bases:
            files.extend([base, f"{base}.asc", f"{base}.sha512"])

        return files

    def validate_svn_files(self):
        console_print("\n[bold]SVN File Verification[/bold]")
        start_time = time.time()

        airflow_svn_dir = self.get_svn_directory()
        task_sdk_svn_dir = self.get_task_sdk_svn_directory()

        console_print(f"Checking Airflow directory: {airflow_svn_dir}")
        if not airflow_svn_dir.exists():
            return ValidationResult(
                check_type=CheckType.SVN,
                passed=False,
                message=f"Airflow SVN directory not found: {airflow_svn_dir}",
                duration_seconds=time.time() - start_time,
            )

        console_print(f"Checking Task SDK directory: {task_sdk_svn_dir}")
        if not task_sdk_svn_dir.exists():
            return ValidationResult(
                check_type=CheckType.SVN,
                passed=False,
                message=f"Task SDK SVN directory not found: {task_sdk_svn_dir}",
                duration_seconds=time.time() - start_time,
            )

        actual_airflow = {f.name for f in airflow_svn_dir.iterdir() if f.is_file()}
        expected_airflow = set(self.get_expected_files())
        missing_airflow = expected_airflow - actual_airflow

        actual_task_sdk = {f.name for f in task_sdk_svn_dir.iterdir() if f.is_file()}
        expected_task_sdk = set(self.get_task_sdk_expected_files())
        missing_task_sdk = expected_task_sdk - actual_task_sdk

        details = []
        if missing_airflow:
            details.append(f"Missing {len(missing_airflow)} Airflow files:")
            details.extend([f"  - {f}" for f in sorted(missing_airflow)[:10]])
        if missing_task_sdk:
            details.append(f"Missing {len(missing_task_sdk)} Task SDK files:")
            details.extend([f"  - {f}" for f in sorted(missing_task_sdk)[:10]])

        missing = list(missing_airflow) + list(missing_task_sdk)
        total_expected = len(expected_airflow) + len(expected_task_sdk)
        message = (
            f"All {total_expected} expected files present" if not missing else f"Missing {len(missing)} files"
        )

        result = ValidationResult(
            check_type=CheckType.SVN,
            passed=not missing,
            message=message,
            details=details or None,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def validate_signatures(self):
        console_print("\n[bold]GPG Signature Verification[/bold]")
        start_time = time.time()

        asc_files = []
        for svn_dir in [self.get_svn_directory(), self.get_task_sdk_svn_directory()]:
            if svn_dir.exists():
                asc_files.extend(svn_dir.glob("*.asc"))

        if not asc_files:
            return ValidationResult(
                check_type=CheckType.SIGNATURES,
                passed=False,
                message="No .asc files found",
                duration_seconds=time.time() - start_time,
            )

        failed = [
            f.name
            for f in asc_files
            if run_command(["gpg", "--verify", str(f)], check=False, capture_output=True).returncode != 0
        ]

        message = (
            f"All {len(asc_files)} signatures verified" if not failed else f"{len(failed)} signatures failed"
        )
        result = ValidationResult(
            check_type=CheckType.SIGNATURES,
            passed=not failed,
            message=message,
            details=failed or None,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def validate_checksums(self):
        console_print("\n[bold]SHA512 Checksum Verification[/bold]")
        start_time = time.time()

        sha512_files = []
        for svn_dir in [self.get_svn_directory(), self.get_task_sdk_svn_directory()]:
            if svn_dir.exists():
                sha512_files.extend(svn_dir.glob("*.sha512"))

        if not sha512_files:
            return ValidationResult(
                check_type=CheckType.CHECKSUMS,
                passed=False,
                message="No .sha512 files found",
                duration_seconds=time.time() - start_time,
            )

        failed = []
        for sha_file in sha512_files:
            expected = sha_file.read_text().split()[0]
            target_file = sha_file.parent / sha_file.name.replace(".sha512", "")

            if not target_file.exists():
                failed.append(f"{sha_file.name} (target missing)")
                continue

            result = run_command(
                ["shasum", "-a", "512", str(target_file)], check=False, capture_output=True, text=True
            )
            if result.returncode != 0 or result.stdout.split()[0] != expected:
                failed.append(sha_file.name)

        message = (
            f"All {len(sha512_files)} checksums valid" if not failed else f"{len(failed)} checksums failed"
        )
        result = ValidationResult(
            check_type=CheckType.CHECKSUMS,
            passed=not failed,
            message=message,
            details=failed or None,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def validate_reproducible_build(self):
        console_print("\n[bold]Reproducible Build Verification[/bold]")
        start_time = time.time()

        dist_dir = self.airflow_repo_root / "dist"
        if dist_dir.exists():
            console_print("Cleaning dist directory...")
            shutil.rmtree(dist_dir)
        dist_dir.mkdir()

        console_print("Building packages from source...")
        if not self.build_packages():
            return ValidationResult(
                check_type=CheckType.REPRODUCIBLE_BUILD,
                passed=False,
                message="Failed to build packages",
                duration_seconds=time.time() - start_time,
            )

        differences = []
        for pattern in ["*.tar.gz", "*.whl"]:
            for file in dist_dir.glob(pattern):
                svn_dir = (
                    self.get_task_sdk_svn_directory() if "task_sdk" in file.name else self.get_svn_directory()
                )
                svn_file = svn_dir / file.name
                if svn_file.exists() and not filecmp.cmp(file, svn_file, shallow=False):
                    differences.append(file.name)

        message = "All packages are identical" if not differences else f"{len(differences)} packages differ"
        result = ValidationResult(
            check_type=CheckType.REPRODUCIBLE_BUILD,
            passed=not differences,
            message=message,
            details=differences or None,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def validate_licenses(self):
        console_print("\n[bold]Apache RAT License Verification[/bold]")
        start_time = time.time()

        rat_jar = Path("/tmp/apache-rat-0.17/apache-rat-0.17.jar")
        source_dir = Path("/tmp/apache-airflow-src")

        if not rat_jar.exists():
            console_print("Downloading Apache RAT...")
            wget_result = run_command(
                ["wget", "-qO-", self.APACHE_RAT_JAR_DOWNLOAD_URL],
                check=False,
                capture_output=True,
            )
            if wget_result.returncode != 0:
                return ValidationResult(
                    check_type=CheckType.LICENSES,
                    passed=False,
                    message="Failed to download Apache RAT",
                    duration_seconds=time.time() - start_time,
                )

            subprocess.run(["tar", "-C", "/tmp", "-xzf", "-"], input=wget_result.stdout, check=True)
            console_print("[green]Apache RAT downloaded[/green]")
        else:
            console_print("[green]Apache RAT already present[/green]")

        source_tarball = self.get_svn_directory() / f"apache_airflow-{self.version_without_rc}-source.tar.gz"
        if not source_tarball.exists():
            return ValidationResult(
                check_type=CheckType.LICENSES,
                passed=False,
                message=f"Source tarball not found: {source_tarball}",
                duration_seconds=time.time() - start_time,
            )

        console_print(f"Extracting source to {source_dir}...")
        if source_dir.exists():
            shutil.rmtree(source_dir)
        source_dir.mkdir(parents=True)

        with tarfile.open(source_tarball, "r:gz") as tar:
            for member in tar.getmembers():
                member.name = "/".join(member.name.split("/")[1:])
                if member.name:
                    tar.extract(member, source_dir)

        rat_excludes = source_dir / ".rat-excludes"
        console_print("Running Apache RAT...")
        result = run_command(
            [
                "java",
                "-jar",
                str(rat_jar),
                "--input-exclude-file",
                str(rat_excludes) if rat_excludes.exists() else "/dev/null",
                str(source_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        error_lines = [line.strip() for line in result.stdout.split("\n") if line.strip().startswith("!")]
        unapproved = unknown = 0

        for line in result.stdout.split("\n"):
            if "Unapproved:" in line:
                try:
                    unapproved = int(line.split("Unapproved:")[1].split()[0])
                except (IndexError, ValueError):
                    pass
            if "Unknown:" in line:
                try:
                    unknown = int(line.split("Unknown:")[1].split()[0])
                except (IndexError, ValueError):
                    pass

        details = []
        if error_lines:
            details.append(f"Found {len(error_lines)} license issues:")
            details.extend(error_lines[:10])
            if len(error_lines) > 10:
                details.append(f"... and {len(error_lines) - 10} more")
        if unapproved > 0:
            details.append(f"Unapproved licenses: {unapproved}")
        if unknown > 0:
            details.append(f"Unknown licenses: {unknown}")

        passed = not error_lines and unapproved == 0 and unknown == 0
        message = (
            "All files have approved licenses"
            if passed
            else f"Found {len(error_lines)} issues, {unapproved} unapproved, {unknown} unknown"
        )

        result = ValidationResult(
            check_type=CheckType.LICENSES,
            passed=passed,
            message=message,
            details=details or None,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def build_packages(self) -> bool:
        console_print("Building Airflow distributions...")
        if (
            run_command(
                [
                    "breeze",
                    "release-management",
                    "prepare-airflow-distributions",
                    "--distribution-format",
                    "both",
                ],
                cwd=str(self.airflow_repo_root),
                check=False,
                capture_output=True,
            ).returncode
            != 0
        ):
            console_print("[red]Failed to build Airflow distributions[/red]")
            return False

        console_print("Building Task SDK distributions...")
        if (
            run_command(
                [
                    "breeze",
                    "release-management",
                    "prepare-task-sdk-distributions",
                    "--distribution-format",
                    "both",
                ],
                cwd=str(self.airflow_repo_root),
                check=False,
                capture_output=True,
            ).returncode
            != 0
        ):
            console_print("[red]Failed to build Task SDK distributions[/red]")
            return False

        console_print("Building source tarball...")
        cmd = [
            "breeze",
            "release-management",
            "prepare-tarball",
            "--tarball-type",
            "apache_airflow",
            "--version",
            self.version_without_rc,
        ]
        if version_suffix := self._get_version_suffix():
            cmd.extend(["--version-suffix", version_suffix])

        if (
            run_command(cmd, cwd=str(self.airflow_repo_root), check=False, capture_output=True).returncode
            != 0
        ):
            console_print("[red]Failed to build source tarball[/red]")
            return False

        console_print("[green]All packages built successfully[/green]")
        return True

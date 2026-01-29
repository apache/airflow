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
import shutil
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.run_utils import run_command


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
    """Base class for release validators with common functionality for PMC verification."""

    APACHE_RAT_JAR_DOWNLOAD_URL = (
        "https://downloads.apache.org/creadur/apache-rat-0.17/apache-rat-0.17-bin.tar.gz"
    )
    APACHE_RAT_JAR_SHA512_DOWNLOAD_URL = (
        "https://downloads.apache.org/creadur/apache-rat-0.17/apache-rat-0.17-bin.tar.gz.sha512"
    )
    GPG_KEYS_URL = "https://dist.apache.org/repos/dist/release/airflow/KEYS"

    def __init__(
        self,
        version: str,
        svn_path: Path,
        airflow_repo_root: Path,
        download_gpg_keys: bool = False,
        update_svn: bool = True,
        verbose: bool = False,
    ):
        self.version = version
        self.svn_path = svn_path
        self.airflow_repo_root = airflow_repo_root
        self.download_gpg_keys = download_gpg_keys
        self.update_svn = update_svn
        self.verbose = verbose
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
    def build_packages(self, source_dir: Path | None = None) -> bool:
        pass

    @abstractmethod
    def validate_svn_files(self) -> ValidationResult:
        pass

    @abstractmethod
    def validate_reproducible_build(self) -> ValidationResult:
        pass

    @abstractmethod
    def validate_licenses(self) -> ValidationResult:
        pass

    def get_svn_directories(self) -> list[Path]:
        """Return list of SVN directories to validate. Override for multi-directory validation."""
        return [self.get_svn_directory()]

    def validate_signatures(self) -> ValidationResult:
        """Verify GPG signatures for all .asc files."""
        console_print("\n[bold]GPG Signature Verification[/bold]")
        start_time = time.time()

        asc_files: list[Path] = []
        for svn_dir in self.get_svn_directories():
            if svn_dir.exists():
                asc_files.extend(svn_dir.glob("*.asc"))

        if not asc_files:
            return ValidationResult(
                check_type=CheckType.SIGNATURES,
                passed=False,
                message="No .asc files found",
                duration_seconds=time.time() - start_time,
            )

        failed = []
        for asc_file in asc_files:
            result = run_command(
                ["gpg", "--verify", str(asc_file)], check=False, capture_output=True, text=True
            )
            if result.returncode != 0:
                failed.append(asc_file.name)
            elif self.verbose:
                # Extract signer from GPG output
                match = re.search(r"Good signature from \"(.*)\"", result.stderr)
                signer = match.group(1) if match else "Unknown"
                console_print(f"  {asc_file.name}: Valid signature from {signer}")

        message = (
            f"All {len(asc_files)} signatures verified"
            if not failed
            else f"{len(failed)} of {len(asc_files)} signatures failed"
        )

        details = failed[:] if failed else None
        if failed:
            details = details or []
            details.append(
                "Hint: If signatures failed due to missing keys, try running with --download-gpg-keys"
            )
            details.append(f"or download manually from {self.GPG_KEYS_URL}")

        result = ValidationResult(
            check_type=CheckType.SIGNATURES,
            passed=not failed,
            message=message,
            details=details,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def validate_checksums(self) -> ValidationResult:
        """Verify SHA512 checksums for all .sha512 files."""
        console_print("\n[bold]SHA512 Checksum Verification[/bold]")
        start_time = time.time()

        sha512_files: list[Path] = []
        for svn_dir in self.get_svn_directories():
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
                failed.append(f"{sha_file.name} (target file missing)")
                continue

            result = run_command(
                ["shasum", "-a", "512", str(target_file)], check=False, capture_output=True, text=True
            )
            if result.returncode != 0 or result.stdout.split()[0] != expected:
                failed.append(sha_file.name)
            elif self.verbose:
                console_print(f"  {sha_file.name}: OK")

        message = (
            f"All {len(sha512_files)} checksums valid"
            if not failed
            else f"{len(failed)} of {len(sha512_files)} checksums failed"
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

    @property
    def check_methods(self) -> dict[CheckType, Callable]:
        return {
            CheckType.SVN: self.validate_svn_files,
            CheckType.REPRODUCIBLE_BUILD: self.validate_reproducible_build,
            CheckType.SIGNATURES: self.validate_signatures,
            CheckType.CHECKSUMS: self.validate_checksums,
            CheckType.LICENSES: self.validate_licenses,
        }

    @property
    def all_check_types(self) -> list[CheckType]:
        """Return all available check types in order.

        Order matches README_RELEASE_AIRFLOW.md section order for PMC verification:
        1. Reproducible build - Build from source and compare with SVN artifacts
        2. SVN - Verify expected files exist in SVN
        3. Licenses - Apache RAT license verification
        4. Signatures - GPG signature verification
        5. Checksums - SHA512 checksum verification

        Note: Tests are independent and can run in any order.
        """
        return [
            CheckType.REPRODUCIBLE_BUILD,
            CheckType.SVN,
            CheckType.LICENSES,
            CheckType.SIGNATURES,
            CheckType.CHECKSUMS,
        ]

    def _get_prerequisites_for_checks(self, checks: list[CheckType]) -> dict[str, list[CheckType]]:
        """Return mapping of prerequisite -> list of checks that require it."""
        # Define which checks require which prerequisites
        prereq_map = {
            "java": [CheckType.LICENSES],  # Apache RAT requires Java
            "gpg": [CheckType.SIGNATURES],  # GPG signature verification
            "svn": list(CheckType),  # All checks need SVN files
            "docker": [CheckType.REPRODUCIBLE_BUILD],  # Docker builds
            "hatch": [CheckType.REPRODUCIBLE_BUILD],  # Package builds
            "clean_git": [CheckType.REPRODUCIBLE_BUILD],  # No uncommitted changes
        }

        # Filter to only prerequisites needed for the selected checks
        needed: dict[str, list[CheckType]] = {}
        for prereq, required_by in prereq_map.items():
            matching = [c for c in checks if c in required_by]
            if matching:
                needed[prereq] = matching
        return needed

    def validate_prerequisites(self, checks: list[CheckType] | None = None) -> bool:
        """Verify prerequisites based on which checks will be run."""
        if checks is None:
            checks = self.all_check_types

        console_print("\n[bold]Prerequisites Verification[/bold]")
        failed: list[str] = []
        warnings: list[str] = []

        needed_prereqs = self._get_prerequisites_for_checks(checks)

        # Check Java (required for Apache RAT / license checks)
        if "java" in needed_prereqs:
            java_path = shutil.which("java")
            if not java_path:
                failed.append("Java is not installed (required for Apache RAT)")
            elif self.verbose:
                console_print(f"  [green]✓[/green] Java: {java_path}")

        # Check GPG (required for signature verification)
        if "gpg" in needed_prereqs:
            gpg_path = shutil.which("gpg")
            if not gpg_path:
                failed.append("GPG is not installed (required for signature verification)")
            elif self.verbose:
                console_print(f"  [green]✓[/green] GPG: {gpg_path}")

        # Check SVN (required for release verification)
        if "svn" in needed_prereqs:
            svn_path = shutil.which("svn")
            if not svn_path:
                failed.append("SVN is not installed (required for release verification)")
            elif self.verbose:
                console_print(f"  [green]✓[/green] SVN: {svn_path}")

        # Check Docker (required for reproducible builds)
        if "docker" in needed_prereqs:
            docker_path = shutil.which("docker")
            if not docker_path:
                failed.append("Docker is not installed (required for reproducible builds)")
            else:
                # Check if Docker daemon is running
                result = run_command(
                    ["docker", "info"],
                    check=False,
                    capture_output=True,
                )
                if result.returncode != 0:
                    failed.append("Docker is installed but not running (start Docker daemon)")
                elif self.verbose:
                    console_print(f"  [green]✓[/green] Docker: {docker_path} (daemon running)")

        # Check hatch (required for local package builds)
        if "hatch" in needed_prereqs:
            hatch_path = shutil.which("hatch")
            if not hatch_path:
                failed.append(
                    "hatch is not installed (required for reproducible builds, install with: uv tool install hatch)"
                )
            elif self.verbose:
                console_print(f"  [green]✓[/green] hatch: {hatch_path}")

        # Check for clean git working directory (required for reproducible builds)
        if "clean_git" in needed_prereqs:
            if not self._check_clean_git_working_directory():
                failed.append(
                    "Git working directory has uncommitted or staged changes "
                    "(reproducible build requires clean checkout to switch tags)"
                )
            elif self.verbose:
                console_print("  [green]✓[/green] Git: working directory clean")

        # Optionally download GPG keys
        if self.download_gpg_keys:
            self._download_gpg_keys()

        if warnings:
            console_print("[yellow]Warnings:[/yellow]")
            for w in warnings:
                console_print(f"  - {w}")

        if failed:
            console_print("[red]Prerequisites failed:[/red]")
            for f in failed:
                console_print(f"  - {f}")
            console_print("[yellow]Please install missing prerequisites and try again.[/yellow]")
            return False

        # Optionally update SVN checkout
        if self.update_svn:
            if not self._update_svn():
                return False
        else:
            console_print("[yellow]SVN update skipped. The local revision might not be the latest.[/yellow]")

        # Check that release files exist in the SVN directory
        if not self._verify_release_files_exist():
            return False

        console_print("[green]All required prerequisites met[/green]")
        return True

    def _download_gpg_keys(self) -> None:
        """Download GPG keys from ASF."""
        console_print("Downloading GPG keys from ASF...")
        with tempfile.NamedTemporaryFile() as tmp_keys:
            run_command(["wget", "-qO", tmp_keys.name, self.GPG_KEYS_URL], check=True)
            run_command(["gpg", "--import", tmp_keys.name], check=True, capture_output=True)
        console_print("[green]GPG keys downloaded and imported[/green]")

    def _check_svn_locks(self, svn_dir: Path) -> bool:
        """Check if SVN working copy is locked."""
        # svn status shows 'L' in second column for locked items
        result = run_command(
            ["svn", "status", str(svn_dir)],
            check=False,
            capture_output=True,
            text=True,
        )
        # Check for lock indicator in output (L in column 3) or E155037 error
        if "E155037" in result.stderr:
            return True
        for line in result.stdout.splitlines():
            # SVN status format: columns are [item status][props][lock][history][switched][info][conflict]
            # Lock is in column 3 (index 2), shown as 'L'
            if len(line) > 2 and line[2] == "L":
                return True
        return False

    def _check_clean_git_working_directory(self) -> bool:
        """Check if git working directory is clean (no uncommitted or staged changes)."""
        result = run_command(
            ["git", "-C", str(self.airflow_repo_root), "status", "--porcelain"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return False
        # If output is empty, working directory is clean
        return not result.stdout.strip()

    def _update_svn(self) -> bool:
        """Update SVN checkout to ensure we have the latest release files."""
        # Update only the specific directories needed, not the entire SVN tree
        svn_dirs = self.get_svn_directories()

        for svn_dir in svn_dirs:
            # Check for SVN locks before attempting update (prevents hanging)
            if self._check_svn_locks(svn_dir.parent):
                console_print(f"[red]SVN working copy is locked: {svn_dir.parent}[/red]")
                console_print(
                    "\n[yellow]Hint: Run the following to release SVN locks:[/yellow]\n"
                    f"  svn cleanup {svn_dir.parent}\n"
                    "\n[yellow]Or skip SVN update with --no-update-svn if files are already up to date.[/yellow]"
                )
                return False

            console_print(f"Updating SVN checkout: {svn_dir}...")

            result = run_command(
                ["svn", "update", "--set-depth=infinity", str(svn_dir)],
                check=False,
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                console_print("[red]Failed to update SVN checkout[/red]")
                if result.stderr:
                    console_print(f"[red]{result.stderr.strip()}[/red]")
                console_print(
                    "[yellow]Hint: Make sure you have checked out the SVN repository:[/yellow]\n"
                    "  svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist\n"
                    "  svn update --set-depth=infinity asf-dist/dev/airflow"
                )
                return False

        console_print("[green]SVN checkout updated[/green]")
        return True

    def _verify_release_files_exist(self) -> bool:
        """Verify that the SVN directories contain release files."""
        for svn_dir in self.get_svn_directories():
            if not svn_dir.exists():
                console_print(f"[red]SVN directory does not exist: {svn_dir}[/red]")
                console_print(
                    "[yellow]Hint: Make sure the version is correct and SVN is checked out.[/yellow]\n"
                    "  You may need to run with --update-svn to fetch the latest files."
                )
                return False

            # Check for release artifacts (.tar.gz or .whl files)
            release_files = list(svn_dir.glob("*.tar.gz")) + list(svn_dir.glob("*.whl"))
            if not release_files:
                console_print(f"[red]No release files found in: {svn_dir}[/red]")
                console_print(
                    "[yellow]The directory exists but contains no release artifacts.\n"
                    "This may happen if:\n"
                    "  - The release was already published and files were moved to the release folder\n"
                    "  - The SVN checkout is out of date\n"
                    "  - The version is incorrect\n\n"
                    "Hint: Try running with --update-svn to fetch the latest files.[/yellow]"
                )
                return False

            if self.verbose:
                console_print(f"  [green]✓[/green] Found {len(release_files)} release files in {svn_dir}")

        return True

    def _download_apache_rat(self) -> Path | None:
        """Download and verify Apache RAT jar.

        Returns the path to the jar file, or None if download/verification failed.
        """
        rat_jar = Path("/tmp/apache-rat-0.17/apache-rat-0.17.jar")

        if rat_jar.exists():
            console_print("[green]Apache RAT already present[/green]")
            return rat_jar

        console_print("Downloading Apache RAT...")
        rat_tarball = Path("/tmp/apache-rat-0.17-bin.tar.gz")
        rat_sha512 = Path("/tmp/apache-rat-0.17-bin.tar.gz.sha512")

        # Download tarball
        wget_result = run_command(
            ["wget", "-qO", str(rat_tarball), self.APACHE_RAT_JAR_DOWNLOAD_URL],
            check=False,
            capture_output=True,
        )
        if wget_result.returncode != 0:
            console_print("[red]Failed to download Apache RAT[/red]")
            return None

        # Download and verify checksum
        console_print("Verifying Apache RAT Checksum...")
        sha_download = run_command(
            ["wget", "-qO", str(rat_sha512), self.APACHE_RAT_JAR_SHA512_DOWNLOAD_URL],
            check=False,
            capture_output=True,
        )
        if sha_download.returncode != 0:
            console_print("[red]Failed to download Apache RAT checksum[/red]")
            return None

        sha_result = run_command(["shasum", "-a", "512", str(rat_tarball)], capture_output=True, text=True)
        calculated_sha = sha_result.stdout.split()[0]
        expected_sha = rat_sha512.read_text().split()[0]

        if calculated_sha != expected_sha:
            console_print("[red]Apache RAT checksum verification failed![/red]")
            console_print(f"  Expected: {expected_sha[:32]}...")
            console_print(f"  Got:      {calculated_sha[:32]}...")
            return None

        # Extract
        subprocess.run(["tar", "-C", "/tmp", "-xzf", str(rat_tarball)], check=True)
        console_print("[green]Apache RAT downloaded and verified[/green]")

        return rat_jar

    def validate(self, checks: list[CheckType] | None = None) -> bool:
        """Run validation checks. Override to add prerequisites."""
        if checks is None:
            checks = self.all_check_types

        if not self.validate_prerequisites(checks):
            return False

        return self._run_checks(checks)

    def _run_checks(self, checks: list[CheckType] | None = None) -> bool:
        """Internal method to run the actual validation checks."""
        if checks is None:
            checks = self.all_check_types

        self.checks_run = checks  # Track which checks were actually run

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

        # Check if we ran all available checks
        all_checks = set(self.all_check_types)
        checks_run = set(getattr(self, "checks_run", all_checks))
        skipped_checks = all_checks - checks_run

        if passed_count == total_count:
            console_print(f"[bold green]ALL CHECKS PASSED ({passed_count}/{total_count})[/bold green]")

            console_print("\nPassed checks:")
            for result in self.results:
                console_print(f"  - {result.check_type.value}: {result.message}")

            if skipped_checks:
                console_print(
                    f"\n[yellow]Note: Only {total_count} of {len(all_checks)} checks were run.[/yellow]"
                )
                console_print("Skipped checks:")
                for check in sorted(skipped_checks, key=lambda c: c.value):
                    console_print(f"  - {check.value}")
                console_print(
                    "\n[yellow]You may vote +1 (binding) only if you have verified "
                    "the skipped checks manually or by running them separately.[/yellow]"
                )
            else:
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

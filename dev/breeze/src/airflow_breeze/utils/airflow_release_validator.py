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
import tarfile
import time
from pathlib import Path

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.release_validator import CheckType, ReleaseValidator, ValidationResult
from airflow_breeze.utils.run_utils import run_command


class AirflowReleaseValidator(ReleaseValidator):
    """Validator for Apache Airflow release candidates."""

    def __init__(
        self,
        version: str,
        svn_path: Path,
        airflow_repo_root: Path,
        task_sdk_version: str | None = None,
        download_gpg_keys: bool = False,
        update_svn: bool = True,
        verbose: bool = False,
    ):
        super().__init__(
            version=version,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            download_gpg_keys=download_gpg_keys,
            update_svn=update_svn,
            verbose=verbose,
        )
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

    def get_svn_directories(self) -> list[Path]:
        """Return both Airflow and Task SDK SVN directories."""
        return [self.get_svn_directory(), self.get_task_sdk_svn_directory()]

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

    def _compare_archives(self, built_file: Path, svn_file: Path) -> tuple[bool, list[str]]:
        """Compare two archives by content.

        Returns:
            Tuple of (matches, diff_details) where diff_details lists what differs.
        """
        diff_details = []

        if built_file.suffix == ".whl":
            import zipfile

            try:
                with zipfile.ZipFile(built_file) as z1, zipfile.ZipFile(svn_file) as z2:
                    n1 = set(z1.namelist())
                    n2 = set(z2.namelist())
                    only_in_built = {n for n in (n1 - n2)}
                    only_in_svn = {n for n in (n2 - n1)}
                    if only_in_built:
                        diff_details.append(f"Only in built: {', '.join(sorted(only_in_built)[:5])}")
                    if only_in_svn:
                        diff_details.append(f"Only in SVN: {', '.join(sorted(only_in_svn)[:5])}")
                    for n in n1 & n2:
                        if z1.getinfo(n).CRC != z2.getinfo(n).CRC:
                            diff_details.append(f"Content differs: {n}")
                    return (not diff_details, diff_details)
            except Exception as e:
                return (False, [f"Error: {e}"])

        elif built_file.suffix == ".gz":  # tar.gz
            try:
                with tarfile.open(built_file, "r:gz") as t1, tarfile.open(svn_file, "r:gz") as t2:
                    m1 = {m.name: m for m in t1.getmembers()}
                    m2 = {m.name: m for m in t2.getmembers()}
                    only_in_built = {n for n in (set(m1.keys()) - set(m2.keys()))}
                    only_in_svn = {n for n in (set(m2.keys()) - set(m1.keys()))}
                    if only_in_built:
                        diff_details.append(f"Only in built: {', '.join(sorted(only_in_built)[:5])}")
                    if only_in_svn:
                        diff_details.append(f"Only in SVN: {', '.join(sorted(only_in_svn)[:5])}")

                    # First pass: compare sizes (fast, metadata only)
                    common_names = set(m1.keys()) & set(m2.keys())
                    size_mismatches = []
                    for name in common_names:
                        if m1[name].size != m2[name].size:
                            size_mismatches.append(name)
                        elif m1[name].issym() and m2[name].issym():
                            if m1[name].linkname != m2[name].linkname:
                                diff_details.append(f"Symlink differs: {name}")
                        elif m1[name].isdir() != m2[name].isdir():
                            diff_details.append(f"Type differs: {name}")

                    if size_mismatches:
                        for name in size_mismatches[:10]:
                            diff_details.append(f"Size differs: {name} ({m1[name].size} vs {m2[name].size})")

                    # If file lists and sizes all match, archives are equivalent
                    return (not diff_details, diff_details)
            except Exception as e:
                return (False, [f"Error: {e}"])

        return (False, ["Unknown archive type"])

    def validate_reproducible_build(self):
        """Build packages from source using git checkout and compare with SVN artifacts."""
        console_print("\n[bold]Reproducible Build Verification[/bold]")
        start_time = time.time()

        tag = self.version
        repo_root = self.airflow_repo_root

        # Check for uncommitted changes
        status_result = run_command(
            ["git", "status", "--porcelain"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if status_result.stdout.strip():
            return ValidationResult(
                check_type=CheckType.REPRODUCIBLE_BUILD,
                passed=False,
                message="Repository has uncommitted changes",
                details=["Please commit or stash changes before running reproducible build check."],
                duration_seconds=time.time() - start_time,
            )

        # Save current branch name (if on a branch) or HEAD commit
        branch_result = run_command(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        original_branch = branch_result.stdout.strip() if branch_result.returncode == 0 else None

        # Save current HEAD to restore later
        head_result = run_command(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        if head_result.returncode != 0:
            return ValidationResult(
                check_type=CheckType.REPRODUCIBLE_BUILD,
                passed=False,
                message="Failed to get current HEAD",
                duration_seconds=time.time() - start_time,
            )
        original_head = head_result.stdout.strip()

        # Determine what to display and restore to
        if original_branch and original_branch != "HEAD":
            original_ref = original_branch
            original_display = f"branch '{original_branch}'"
        else:
            original_ref = original_head
            original_display = f"commit {original_head[:12]}"

        # Warn user about branch switch
        console_print(
            f"[yellow]WARNING: This check will temporarily switch from {original_display} "
            f"to tag '{tag}' and should automatically return afterwards.[/yellow]"
        )

        console_print(f"Checking out tag: {tag}")
        checkout_result = run_command(
            ["git", "checkout", tag],
            cwd=str(repo_root),
            check=False,
        )
        if checkout_result.returncode != 0:
            return ValidationResult(
                check_type=CheckType.REPRODUCIBLE_BUILD,
                passed=False,
                message=f"Failed to checkout tag {tag}",
                details=["Hint: Make sure the tag exists. Run 'git fetch --tags' to update."],
                duration_seconds=time.time() - start_time,
            )

        # Initialize result variables
        differences = []
        verified_count = 0
        missing_from_svn = []
        build_failed = False

        try:
            # Clean dist directory (as per manual release process: rm -rf dist/*)
            dist_dir = repo_root / "dist"
            if dist_dir.exists():
                console_print("Cleaning dist directory...")
                shutil.rmtree(dist_dir)

            # NOTE: git clean commented out - it removes .venv and other important files
            # The Docker-based build should handle this in isolation anyway
            # console_print("Cleaning untracked files (git clean -fdx)...")
            # run_command(
            #     ["git", "clean", "-fdx"],
            #     cwd=str(repo_root),
            #     check=False,
            # )

            # Build packages using breeze from the checked-out tag
            console_print("Building packages from source...")
            if not self.build_packages():
                build_failed = True
            else:
                # Compare built packages with SVN
                dist_dir = repo_root / "dist"

                for pattern in ["*.tar.gz", "*.whl"]:
                    for built_file in dist_dir.glob(pattern):
                        svn_dir = (
                            self.get_task_sdk_svn_directory()
                            if "task_sdk" in built_file.name
                            else self.get_svn_directory()
                        )
                        svn_file = svn_dir / built_file.name
                        if svn_file.exists():
                            console_print(f"Verifying {built_file.name}...", end=" ")
                            # Default to binary comparison
                            if filecmp.cmp(built_file, svn_file, shallow=False):
                                verified_count += 1
                                console_print("[green]OK[/green]")
                            else:
                                # Compare archive contents
                                matches, diff_details = self._compare_archives(built_file, svn_file)
                                if matches:
                                    verified_count += 1
                                    console_print("[green]OK (content match)[/green]")
                                else:
                                    differences.append(built_file.name)
                                    console_print("[red]MISMATCH[/red]")
                                    for detail in diff_details[:10]:
                                        console_print(f"  {detail}")
                                    if len(diff_details) > 10:
                                        console_print(f"  ... and {len(diff_details) - 10} more differences")

                        else:
                            missing_from_svn.append(built_file.name)
                            console_print(
                                f"[yellow]Note: {built_file.name} not in SVN (may be expected)[/yellow]"
                            )
        finally:
            # Always restore original branch/HEAD, regardless of success or failure
            console_print(f"Restoring to {original_display}...")
            restore_result = run_command(
                ["git", "checkout", original_ref],
                cwd=str(repo_root),
                check=False,
            )
            if restore_result.returncode == 0:
                console_print(f"[green]Successfully restored to {original_display}[/green]")
            else:
                console_print(
                    f"[red]WARNING: Failed to restore to {original_display}. "
                    f"Please manually run: git checkout {original_ref}[/red]"
                )

        # Return result after restoring HEAD
        if build_failed:
            result = ValidationResult(
                check_type=CheckType.REPRODUCIBLE_BUILD,
                passed=False,
                message="Failed to build packages",
                duration_seconds=time.time() - start_time,
            )
            self._print_result(result)
            return result

        if not differences:
            message = f"All {verified_count} packages are identical to SVN"
        else:
            message = f"{len(differences)} packages differ from SVN"

        details = None
        if differences:
            details = differences[:]
        if missing_from_svn and self.verbose:
            details = details or []
            details.append(f"Note: {len(missing_from_svn)} built packages not in SVN (may be expected)")

        result = ValidationResult(
            check_type=CheckType.REPRODUCIBLE_BUILD,
            passed=not differences,
            message=message,
            details=details,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def validate_licenses(self):
        """Run Apache RAT license check on source tarball."""
        console_print("\n[bold]Apache RAT License Verification[/bold]")
        start_time = time.time()

        source_dir = Path("/tmp/apache-airflow-src")

        # Download Apache RAT with checksum verification
        rat_jar = self._download_apache_rat()
        if not rat_jar:
            return ValidationResult(
                check_type=CheckType.LICENSES,
                passed=False,
                message="Failed to download or verify Apache RAT",
                duration_seconds=time.time() - start_time,
            )

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
                    tar.extract(member, source_dir, filter="data")

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

        # Show verbose RAT output if requested
        if self.verbose:
            separator_count = 0
            for line in result.stdout.splitlines():
                if line.strip().startswith("**********"):
                    separator_count += 1
                if separator_count >= 3:
                    break
                console_print(line)

        # Clean up extracted source directory (~500MB)
        if source_dir.exists():
            shutil.rmtree(source_dir)

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
        """Build Airflow distributions and source tarball."""
        console_print("Building Airflow distributions...")

        # Use breeze from the current checkout
        base_cmd = ["breeze"]

        result = run_command(
            base_cmd
            + [
                "release-management",
                "prepare-airflow-distributions",
                "--distribution-format",
                "both",
            ],
            cwd=str(self.airflow_repo_root),
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            console_print("[red]Failed to build Airflow distributions[/red]")
            if result.stdout:
                console_print(f"[yellow]STDOUT:[/yellow]\n{result.stdout[-2000:]}")
            if result.stderr:
                console_print(f"[yellow]STDERR:[/yellow]\n{result.stderr[-2000:]}")
            return False

        console_print("Building Task SDK distributions...")
        result = run_command(
            base_cmd
            + [
                "release-management",
                "prepare-task-sdk-distributions",
                "--distribution-format",
                "both",
            ],
            cwd=str(self.airflow_repo_root),
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            console_print("[red]Failed to build Task SDK distributions[/red]")
            if result.stdout:
                console_print(f"[yellow]STDOUT:[/yellow]\n{result.stdout[-2000:]}")
            if result.stderr:
                console_print(f"[yellow]STDERR:[/yellow]\n{result.stderr[-2000:]}")
            return False

        console_print("Building source tarball...")
        cmd = base_cmd + [
            "release-management",
            "prepare-tarball",
            "--tarball-type",
            "apache_airflow",
            "--version",
            self.version_without_rc,
        ]
        if version_suffix := self._get_version_suffix():
            cmd.extend(["--version-suffix", version_suffix])

        result = run_command(
            cmd, cwd=str(self.airflow_repo_root), check=False, capture_output=True, text=True
        )
        if result.returncode != 0:
            console_print("[red]Failed to build source tarball[/red]")
            if result.stdout:
                console_print(f"[yellow]STDOUT:[/yellow]\n{result.stdout[-2000:]}")
            if result.stderr:
                console_print(f"[yellow]STDERR:[/yellow]\n{result.stderr[-2000:]}")
            return False

        console_print("[green]All packages built successfully[/green]")
        return True

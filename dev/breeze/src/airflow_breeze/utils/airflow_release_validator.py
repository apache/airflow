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
import os
import shutil
import sys
import tarfile
import tempfile
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

    # validate_signatures() and validate_checksums() are inherited from base class
    # which uses get_svn_directories() to find all directories to check

    def _patch_hatch_build_for_env_var(self, worktree_path: Path) -> None:
        """Patch hatch_build.py to support AIRFLOW_GIT_VERSION environment variable.

        Older releases don't have the env var support in hatch_build.py, so we add it.
        This allows us to pass the correct git version during reproducible builds.
        Note: This causes hatch_build.py (in sdist) to differ from SVN - this is expected.
        """
        hatch_build_path = worktree_path / "airflow-core" / "hatch_build.py"
        if not hatch_build_path.exists():
            return

        content = hatch_build_path.read_text()

        # Skip if already patched
        if "AIRFLOW_GIT_VERSION" in content:
            return

        # Add env var check at the beginning of get_git_version method
        old_code = '''    def get_git_version(self) -> str:
        """
        Return a version to identify the state of the underlying git repo.'''

        new_code = '''    def get_git_version(self) -> str:
        """
        Return a version to identify the state of the underlying git repo.

        Can be overridden via AIRFLOW_GIT_VERSION environment variable.
        """
        # Allow override via environment variable for reproducible builds
        if env_version := os.environ.get("AIRFLOW_GIT_VERSION"):
            log.warning("Using git version from AIRFLOW_GIT_VERSION env var: %s", env_version)
            return env_version
        """'''

        if old_code in content:
            content = content.replace(old_code, new_code)
            hatch_build_path.write_text(content)
            console_print("Patched hatch_build.py to support AIRFLOW_GIT_VERSION env var")

    def _compare_archives(self, built_file: Path, svn_file: Path) -> tuple[bool, list[str]]:
        """Compare two archives by content.

        Returns:
            Tuple of (matches, diff_details) where diff_details lists what differs.
        """
        diff_details = []

        # Files that are expected to differ due to build environment patches
        # hatch_build.py is patched to support AIRFLOW_GIT_VERSION env var
        # .gitignore may be present in local builds but excluded in CI builds
        expected_diffs = {"hatch_build.py", "RECORD", ".gitignore"}

        def is_expected_diff(name: str) -> bool:
            return any(name.endswith(exp) for exp in expected_diffs)

        if built_file.suffix == ".whl":
            import zipfile

            try:
                with zipfile.ZipFile(built_file) as z1, zipfile.ZipFile(svn_file) as z2:
                    n1 = set(z1.namelist())
                    n2 = set(z2.namelist())
                    only_in_built = {n for n in (n1 - n2) if not is_expected_diff(n)}
                    only_in_svn = {n for n in (n2 - n1) if not is_expected_diff(n)}
                    if only_in_built:
                        diff_details.append(f"Only in built: {', '.join(sorted(only_in_built)[:5])}")
                    if only_in_svn:
                        diff_details.append(f"Only in SVN: {', '.join(sorted(only_in_svn)[:5])}")
                    for n in n1 & n2:
                        if z1.getinfo(n).CRC != z2.getinfo(n).CRC:
                            if not is_expected_diff(n):
                                diff_details.append(f"Content differs: {n}")
                    return (not diff_details, diff_details)
            except Exception as e:
                return (False, [f"Error: {e}"])

        elif built_file.suffix == ".gz":  # tar.gz
            try:
                with tarfile.open(built_file, "r:gz") as t1, tarfile.open(svn_file, "r:gz") as t2:
                    m1 = {m.name: m for m in t1.getmembers()}
                    m2 = {m.name: m for m in t2.getmembers()}
                    only_in_built = {n for n in (set(m1.keys()) - set(m2.keys())) if not is_expected_diff(n)}
                    only_in_svn = {n for n in (set(m2.keys()) - set(m1.keys())) if not is_expected_diff(n)}
                    if only_in_built:
                        diff_details.append(f"Only in built: {', '.join(sorted(only_in_built)[:5])}")
                    if only_in_svn:
                        diff_details.append(f"Only in SVN: {', '.join(sorted(only_in_svn)[:5])}")

                    # First pass: compare sizes (fast, metadata only)
                    common_names = set(m1.keys()) & set(m2.keys())
                    size_mismatches = []
                    for name in common_names:
                        if m1[name].size != m2[name].size:
                            if not is_expected_diff(name):
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
        """Build packages from source using git worktree and compare with SVN artifacts."""
        console_print("\n[bold]Reproducible Build Verification[/bold]")
        start_time = time.time()

        # Determine git tag to checkout
        tag = self.version
        console_print(f"Creating worktree for tag: {tag}")

        with tempfile.TemporaryDirectory() as tmp_dir:
            worktree_path = Path(tmp_dir) / "repo"

            # Setup worktree for the tag
            setup_cmd = ["git", "worktree", "add", "--detach", str(worktree_path), tag]
            if run_command(setup_cmd, cwd=str(self.airflow_repo_root), check=False).returncode != 0:
                return ValidationResult(
                    check_type=CheckType.REPRODUCIBLE_BUILD,
                    passed=False,
                    message=f"Failed to checkout tag {tag} into worktree",
                    details=["Hint: Make sure the tag exists. Run 'git fetch --tags' to update."],
                    duration_seconds=time.time() - start_time,
                )

            # Register worktree for cleanup on interrupt (Ctrl+C)
            self.register_worktree(worktree_path)

            # Get git SHA from tag
            result = run_command(
                ["git", "rev-parse", f"{tag}^{{commit}}"],
                cwd=str(self.airflow_repo_root),
                capture_output=True,
                text=True,
                check=False,
            )
            git_sha = result.stdout.strip() if result.returncode == 0 else ""

            # Use the exact format found in SVN artifacts
            git_version = f".dev0+{git_sha}.dirty" if git_sha else None

            # Patch worktree's hatch_build.py to support AIRFLOW_GIT_VERSION env var
            # This allows us to set the correct git version during build
            # Note: This patch will cause hatch_build.py to differ from SVN (expected)
            self._patch_hatch_build_for_env_var(worktree_path)

            try:
                # Build packages in worktree
                console_print("Building packages from source in worktree...")
                if not self.build_packages(source_dir=worktree_path, git_version=git_version):
                    return ValidationResult(
                        check_type=CheckType.REPRODUCIBLE_BUILD,
                        passed=False,
                        message="Failed to build packages",
                        duration_seconds=time.time() - start_time,
                    )

                # Compare built packages with SVN
                dist_dir = worktree_path / "dist"
                differences = []
                verified_count = 0
                missing_from_svn = []

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
                # Clean up worktree and unregister from signal handler
                run_command(
                    ["git", "worktree", "remove", "--force", str(worktree_path)],
                    cwd=str(self.airflow_repo_root),
                    check=False,
                )
                self.unregister_worktree(worktree_path)

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

        # Show verbose RAT output if requested
        if self.verbose:
            separator_count = 0
            for line in result.stdout.splitlines():
                if line.strip().startswith("**********"):
                    separator_count += 1
                if separator_count >= 3:
                    break
                console_print(line)

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

    def build_packages(
        self,
        source_dir: Path | None = None,
        git_version: str | None = None,
    ) -> bool:
        """Build Airflow distributions and source tarball.

        Args:
            source_dir: Optional path to build from (e.g., a worktree). If None, uses airflow_repo_root.
            git_version: Optional git version string to embed in the build.
        """
        source_dir = source_dir or self.airflow_repo_root
        console_print("Building Airflow distributions...")

        env = os.environ.copy()
        # Set SKIP_BREEZE_SELF_UPGRADE_CHECK to avoid Breeze self-upgrade prompts or failures
        env["SKIP_BREEZE_SELF_UPGRADE_CHECK"] = "1"
        env["ANSWER"] = "no"
        if git_version:
            env["AIRFLOW_GIT_VERSION"] = git_version
            console_print(f"Using git version: {git_version}")

        # Set pnpm store to temp directory to avoid .pnpm-store in source tree
        pnpm_home = Path(tempfile.gettempdir()) / "pnpm-store-airflow-build"
        env["PNPM_HOME"] = str(pnpm_home)
        env["npm_config_store_dir"] = str(pnpm_home)

        # If running in a worktree, we want to use the breeze code from that worktree
        # rather than the installed breeze.
        if source_dir != self.airflow_repo_root:
            python_path = source_dir / "dev" / "breeze" / "src"
            env["PYTHONPATH"] = str(python_path) + os.pathsep + env.get("PYTHONPATH", "")
            base_cmd = [sys.executable, "-c", "from airflow_breeze.breeze import main; main()"]
        else:
            base_cmd = ["breeze"]

        result = run_command(
            base_cmd
            + [
                "release-management",
                "prepare-airflow-distributions",
                "--distribution-format",
                "both",
                "--use-local-hatch",
            ],
            cwd=str(source_dir),
            check=False,
            capture_output=True,
            text=True,
            env=env,
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
                "--use-local-hatch",
            ],
            cwd=str(source_dir),
            check=False,
            capture_output=True,
            text=True,
            env=env,
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

        result = run_command(cmd, cwd=str(source_dir), check=False, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            console_print("[red]Failed to build source tarball[/red]")
            if result.stdout:
                console_print(f"[yellow]STDOUT:[/yellow]\n{result.stdout[-2000:]}")
            if result.stderr:
                console_print(f"[yellow]STDERR:[/yellow]\n{result.stderr[-2000:]}")
            return False

        console_print("[green]All packages built successfully[/green]")
        return True

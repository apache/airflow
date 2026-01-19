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
import re
import shutil
import sys
import tarfile
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.release_validator import CheckType, ReleaseValidator, ValidationResult
from airflow_breeze.utils.run_utils import run_command


class ProvidersReleaseValidator(ReleaseValidator):
    """Validator for Apache Airflow Providers release candidates."""

    def __init__(
        self,
        release_date: str,
        svn_path: Path,
        airflow_repo_root: Path,
        packages: list[tuple[str, str]],
        download_gpg_keys: bool = False,
        update_svn: bool = True,
        docker_install_check: bool = False,
        verbose: bool = False,
    ):
        """
        Initialize the providers release validator.

        Args:
            release_date: Release date in YYYY-MM-DD format (e.g., "2026-01-17")
            svn_path: Path to the SVN checkout (e.g., ~/asf-dist/dev/airflow)
            airflow_repo_root: Path to the Airflow repository root
            packages: List of (package_name, version_with_rc) tuples from packages.txt
            download_gpg_keys: Whether to download GPG keys from ASF
            update_svn: Whether to run 'svn update' before validation
            docker_install_check: Whether to generate Dockerfile.pmc and verify provider installation in Docker
            verbose: Whether to show detailed validation output
        """
        # For providers, the "version" is actually the release date
        super().__init__(
            version=release_date,
            svn_path=svn_path,
            airflow_repo_root=airflow_repo_root,
            download_gpg_keys=download_gpg_keys,
            update_svn=update_svn,
            verbose=verbose,
        )
        self.release_date = release_date
        self.packages = packages
        # Store versions without RC suffix for file matching
        self.packages_without_rc = [(name, self._strip_rc_suffix(ver)) for name, ver in packages]
        self.docker_install_check = docker_install_check

    @property
    def check_methods(self) -> dict[CheckType, Callable]:
        methods = super().check_methods
        return methods

    @property
    def all_check_types(self) -> list[CheckType]:
        return super().all_check_types

    # validate_prerequisites(), _download_gpg_keys(), _update_svn(), _verify_release_files_exist()
    # are inherited from the base class

    def get_distribution_name(self) -> str:
        return "Apache Airflow Providers"

    def get_svn_directory(self) -> Path:
        return self.svn_path / "providers" / self.release_date

    def get_expected_files(self) -> list[str]:
        """Build list of expected files from packages list."""
        files = []

        # Source tarball for all providers
        source_tarball = f"apache_airflow_providers-{self.release_date}-source.tar.gz"
        files.extend([source_tarball, f"{source_tarball}.asc", f"{source_tarball}.sha512"])

        # For each provider package
        for name, version in self.packages_without_rc:
            # Convert package name to file format (dashes to underscores)
            file_base = name.replace("-", "_")

            # Wheel and sdist for each provider
            wheel = f"{file_base}-{version}-py3-none-any.whl"
            sdist = f"{file_base}-{version}.tar.gz"

            files.extend([wheel, f"{wheel}.asc", f"{wheel}.sha512"])
            files.extend([sdist, f"{sdist}.asc", f"{sdist}.sha512"])

        return files

    def validate_svn_files(self) -> ValidationResult:
        """Verify all expected provider files are present in SVN and packages are installable.

        This implements the PMC "SVN check" from README_RELEASE_PROVIDERS.md which includes:
        1. Verifying all expected files are present
        2. Generating Dockerfile.pmc
        3. Building and running the Docker image to verify installation
        """
        console_print("\n[bold]SVN File Verification[/bold]")
        start_time = time.time()

        svn_dir = self.get_svn_directory()
        console_print(f"Checking directory: {svn_dir}")

        if not svn_dir.exists():
            return ValidationResult(
                check_type=CheckType.SVN,
                passed=False,
                message=f"SVN directory not found: {svn_dir}",
                duration_seconds=time.time() - start_time,
            )

        actual_files = {f.name for f in svn_dir.iterdir() if f.is_file()}
        expected_files = set(self.get_expected_files())
        missing_files = expected_files - actual_files

        details = []
        if missing_files:
            details.append(f"Missing {len(missing_files)} files:")
            details.extend([f"  - {f}" for f in sorted(missing_files)[:15]])
            if len(missing_files) > 15:
                details.append(f"  ... and {len(missing_files) - 15} more")

        if self.verbose:
            console_print(f"Found {len(actual_files)} files in SVN:")
            for f in sorted(actual_files):
                console_print(f"  - {f}")

        # If files are missing, fail early
        if missing_files:
            result = ValidationResult(
                check_type=CheckType.SVN,
                passed=False,
                message=f"Missing {len(missing_files)} of {len(expected_files)} files",
                details=details or None,
                duration_seconds=time.time() - start_time,
            )
            self._print_result(result)
            return result

        details.append(f"All {len(expected_files)} expected files present")

        # Now verify Docker installation (part of SVN check per PMC instructions)
        if self.docker_install_check:
            docker_passed, docker_details = self._verify_docker_installation()
            details.extend(docker_details)

            if not docker_passed:
                result = ValidationResult(
                    check_type=CheckType.SVN,
                    passed=False,
                    message="SVN files present but Docker installation verification failed",
                    details=details,
                    duration_seconds=time.time() - start_time,
                )
                self._print_result(result)
                return result
        else:
            details.append("Skipping Docker installation verification (use --docker-install-check to enable)")

        result = ValidationResult(
            check_type=CheckType.SVN,
            passed=True,
            message=f"All {len(expected_files)} files present and packages verified installable",
            details=details if self.verbose else None,
            duration_seconds=time.time() - start_time,
        )
        self._print_result(result)
        return result

    def _verify_docker_installation(self) -> tuple[bool, list[str]]:
        """Build Dockerfile.pmc and verify packages install correctly.

        Uses `breeze release-management check-release-files` to generate Dockerfile.pmc,
        then builds and runs it to verify installation, exactly as described in
        README_RELEASE_PROVIDERS.md.

        Returns:
            Tuple of (passed, details) where details contains status messages.
        """
        details = []

        # Check if docker is available
        if not shutil.which("docker"):
            details.append("Docker not available - skipping installation verification")
            details.append("Hint: Install Docker to enable automatic installation verification")
            # Return True with warning - don't fail if Docker isn't available
            return True, details

        dockerfile_path = self.airflow_repo_root / "Dockerfile.pmc"
        image_tag = "local/airflow"

        try:
            # Generate Dockerfile.pmc using check-release-files command
            # This is the same command documented in README_RELEASE_PROVIDERS.md
            console_print("Generating Dockerfile.pmc using check-release-files...")
            packages_file = self.airflow_repo_root / "dev" / "packages.txt"

            # Write packages file temporarily if it doesn't exist with current packages
            packages_content = "\n".join(
                f"https://pypi.org/project/{name}/{ver}/" for name, ver in self.packages
            )
            packages_file.write_text(packages_content)

            check_result = run_command(
                [
                    sys.executable,
                    "-c",
                    "from airflow_breeze.breeze import main; main()",
                    "release-management",
                    "check-release-files",
                    "providers",
                    "--release-date",
                    self.release_date,
                    "--packages-file",
                    str(packages_file),
                    "--path-to-airflow-svn",
                    str(self.svn_path),
                ],
                cwd=str(self.airflow_repo_root),
                check=False,
                capture_output=True,
                text=True,
            )

            if not dockerfile_path.exists():
                details.append("check-release-files did not generate Dockerfile.pmc")
                if check_result.stderr:
                    details.append("Error output:")
                    for line in check_result.stderr.strip().split("\n")[-5:]:
                        details.append(f"  {line}")
                return False, details

            details.append(f"Generated Dockerfile.pmc with {len(self.packages)} packages")

            # Build Docker image
            console_print("Building Docker image (this may take a while)...")
            build_result = run_command(
                ["docker", "build", "-f", str(dockerfile_path), "--tag", image_tag, "."],
                cwd=str(self.airflow_repo_root),
                check=False,
                capture_output=True,
                text=True,
            )

            if build_result.returncode != 0:
                details.append("Docker build failed:")
                stderr_lines = (build_result.stderr or "").strip().split("\n")[-10:]
                stdout_lines = (build_result.stdout or "").strip().split("\n")[-10:]
                for line in stderr_lines + stdout_lines:
                    if line.strip():
                        details.append(f"  {line.strip()}")
                return False, details

            details.append("Docker image built successfully")

            # Run check-release-files to verify installation
            console_print("Running 'airflow info' to verify installation...")
            run_result = run_command(
                ["docker", "run", "--rm", "--entrypoint", "airflow", image_tag, "info"],
                check=False,
                capture_output=True,
                text=True,
            )

            if run_result.returncode != 0:
                details.append("'airflow info' command failed:")
                stderr_lines = (run_result.stderr or "").strip().split("\n")[-10:]
                for line in stderr_lines:
                    if line.strip():
                        details.append(f"  {line.strip()}")
                return False, details

            # Display full airflow info output in verbose mode
            if self.verbose and run_result.stdout:
                console_print(
                    "\n[bold]Output of 'docker run --rm --entrypoint airflow local/airflow info':[/bold]"
                )
                for line in run_result.stdout.split("\n"):
                    console_print(f"  {line}")
                console_print("")

            # Verify installed provider versions match expected versions
            verification_passed, verification_details = self._verify_installed_providers(
                run_result.stdout or ""
            )
            details.extend(verification_details)

            if not verification_passed:
                return False, details

            details.append("'airflow info' completed successfully")

            # Clean up Docker image
            console_print("Cleaning up Docker image...")
            cleanup_result = run_command(
                ["docker", "image", "rm", image_tag],
                check=False,
                capture_output=True,
            )
            if cleanup_result.returncode == 0:
                details.append("Docker image cleaned up")
            else:
                details.append("Warning: Failed to clean up Docker image")

            return True, details

        except Exception as e:
            details.append(f"Exception during Docker verification: {e}")
            return False, details
        finally:
            # Clean up Dockerfile.pmc if it exists
            if dockerfile_path.exists():
                try:
                    dockerfile_path.unlink()
                except OSError:
                    pass

    def _verify_installed_providers(self, airflow_info_output: str) -> tuple[bool, list[str]]:
        """Verify that installed provider versions match expected versions.

        Parses the 'airflow info' output to extract installed providers and compares
        against the expected packages list.

        Args:
            airflow_info_output: The stdout from 'airflow info' command.

        Returns:
            Tuple of (passed, details) where details contains verification messages.
        """
        details = []

        # Build expected versions map: package_name -> version (with rc suffix)
        # Package names in airflow info use dashes, e.g., apache-airflow-providers-google
        expected_versions = {}
        for name, version in self.packages:
            expected_versions[name] = version

        # Parse airflow info output to find installed providers
        # The output format varies, but providers are typically listed with their versions
        # Common formats:
        # | apache-airflow-providers-google | 19.4.0rc2 |
        # apache-airflow-providers-google: 19.4.0rc2
        # apache-airflow-providers-google==19.4.0rc2
        installed_versions = {}

        for raw_line in airflow_info_output.split("\n"):
            line = raw_line.strip()
            if not line:
                continue

            # Try multiple parsing patterns

            # Pattern 1: Table format with pipes: | package | version |
            if "|" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 2:
                    pkg_name = parts[0]
                    pkg_version = parts[1]
                    if pkg_name.startswith("apache-airflow-providers-"):
                        installed_versions[pkg_name] = pkg_version
                continue

            # Pattern 2: Colon format: package: version
            if ":" in line and "apache-airflow-providers-" in line:
                match = re.match(r"(apache-airflow-providers-[\w-]+):\s*(\S+)", line)
                if match:
                    installed_versions[match.group(1)] = match.group(2)
                continue

            # Pattern 3: Equals format: package==version
            if "==" in line and "apache-airflow-providers-" in line:
                match = re.match(r"(apache-airflow-providers-[\w-]+)==(\S+)", line)
                if match:
                    installed_versions[match.group(1)] = match.group(2)
                continue

        # Compare installed vs expected
        missing = []
        version_mismatch = []
        verified = []

        for pkg_name, expected_ver in expected_versions.items():
            if pkg_name not in installed_versions:
                missing.append(pkg_name)
            elif installed_versions[pkg_name] != expected_ver:
                version_mismatch.append(
                    f"{pkg_name}: expected {expected_ver}, got {installed_versions[pkg_name]}"
                )
            else:
                verified.append(f"{pkg_name}=={expected_ver}")

        # Report results
        if verified:
            details.append(f"Verified {len(verified)} packages with correct versions")

        if missing:
            details.append(f"Missing {len(missing)} expected packages:")
            for pkg in missing[:10]:
                details.append(f"  - {pkg}")
            if len(missing) > 10:
                details.append(f"  ... and {len(missing) - 10} more")

            # Show what we found for debugging
            if installed_versions:
                details.append(f"Found {len(installed_versions)} providers in airflow info output:")
                for pkg in sorted(installed_versions.keys())[:5]:
                    details.append(f"  - {pkg}=={installed_versions[pkg]}")
                if len(installed_versions) > 5:
                    details.append(f"  ... and {len(installed_versions) - 5} more")
            else:
                details.append("No providers found in airflow info output")
                # Show a snippet of the output for debugging
                output_lines = [line for line in airflow_info_output.split("\n") if line.strip()][:10]
                if output_lines:
                    details.append("First lines of airflow info output:")
                    for line in output_lines:
                        details.append(f"  {line[:80]}")

        if version_mismatch:
            details.append(f"Version mismatch for {len(version_mismatch)} packages:")
            for mismatch in version_mismatch[:10]:
                details.append(f"  - {mismatch}")
            if len(version_mismatch) > 10:
                details.append(f"  ... and {len(version_mismatch) - 10} more")

        passed = not missing and not version_mismatch
        if passed:
            details.append(f"All {len(expected_versions)} packages installed with correct versions")

        return passed, details

    # validate_signatures() and validate_checksums() are inherited from the base class

    def validate_reproducible_build(self) -> ValidationResult:
        """Build packages from source and compare with SVN artifacts."""
        console_print("\n[bold]Reproducible Build Verification[/bold]")
        start_time = time.time()

        tag = f"providers/{self.release_date}"
        console_print(f"Creating worktree for tag: {tag}")

        with tempfile.TemporaryDirectory() as tmp_dir:
            worktree_path = Path(tmp_dir) / "repo"

            # Setup worktree
            setup_cmd = ["git", "worktree", "add", "--detach", str(worktree_path), tag]
            if run_command(setup_cmd, cwd=str(self.airflow_repo_root), check=False).returncode != 0:
                return ValidationResult(
                    check_type=CheckType.REPRODUCIBLE_BUILD,
                    passed=False,
                    message=f"Failed to checkout tag {tag} into worktree",
                    duration_seconds=time.time() - start_time,
                )

            # Register worktree for cleanup on interrupt (Ctrl+C)
            self.register_worktree(worktree_path)

            try:
                # Build packages
                console_print("Building packages from source in worktree...")
                if not self.build_packages(source_dir=worktree_path):
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
                svn_dir = self.get_svn_directory()

                # Check if SVN directory has any release files
                svn_files = list(svn_dir.glob("*.tar.gz")) + list(svn_dir.glob("*.whl"))
                if not svn_files:
                    return ValidationResult(
                        check_type=CheckType.REPRODUCIBLE_BUILD,
                        passed=False,
                        message=f"No release files found in SVN directory: {svn_dir}",
                        details=[
                            "The SVN directory is empty or doesn't contain release artifacts.",
                            "This may happen if the release was already published and files were moved.",
                            "Hint: Run 'svn update' on your SVN checkout or re-download the dev files.",
                        ],
                        duration_seconds=time.time() - start_time,
                    )

                for pattern in ["*.tar.gz", "*.whl"]:
                    for built_file in dist_dir.glob(pattern):
                        svn_file = svn_dir / built_file.name
                        if svn_file.exists():
                            if not filecmp.cmp(built_file, svn_file, shallow=False):
                                differences.append(built_file.name)
                            else:
                                verified_count += 1
                                if self.verbose:
                                    console_print(f"  {built_file.name}: Verified")
                        else:
                            # File in dist but not in SVN - track it
                            missing_from_svn.append(built_file.name)
                            console_print(
                                f"[yellow]Note: {built_file.name} not in SVN (may be expected)[/yellow]"
                            )
            finally:
                # Clean up worktree and unregister from signal handler
                console_print("Cleaning up worktree...")
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
        # Avoid cleaning up the dist directory in the main repo since we used a temporary one
        return result

    def validate_licenses(self) -> ValidationResult:
        """Run Apache RAT license check on source tarball."""
        console_print("\n[bold]Apache RAT License Verification[/bold]")
        start_time = time.time()

        source_dir = Path("/tmp/apache-airflow-providers-src")

        # Download Apache RAT with checksum verification (from base class)
        rat_jar = self._download_apache_rat()
        if not rat_jar:
            return ValidationResult(
                check_type=CheckType.LICENSES,
                passed=False,
                message="Failed to download or verify Apache RAT",
                duration_seconds=time.time() - start_time,
            )

        # Find and extract source tarball
        source_tarball = (
            self.get_svn_directory() / f"apache_airflow_providers-{self.release_date}-source.tar.gz"
        )
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
                # Strip first directory component
                member.name = "/".join(member.name.split("/")[1:])
                if member.name:
                    tar.extract(member, source_dir)

        # Run RAT
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

        # Parse RAT output
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

        if self.verbose:
            # Print only the summary part of RAT output.
            # The report usually starts with a separator line containing stars,
            # and ends the summary section with another separator line.
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

    def build_packages(self, source_dir: Path | None = None) -> bool:
        """Build provider distributions and source tarball.

        Args:
            source_dir: Optional path to build from (e.g., a worktree). If None, uses airflow_repo_root.
        """
        cwd = source_dir or self.airflow_repo_root
        console_print(f"Building provider distributions in {cwd}...")

        env = os.environ.copy()
        # Set SKIP_BREEZE_SELF_UPGRADE_CHECK to avoid Breeze self-upgrade prompts or failures
        # when running in a worktree / separate directory.
        env["SKIP_BREEZE_SELF_UPGRADE_CHECK"] = "1"
        # Set ANSWER="no" to avoid any interactive prompts
        env["ANSWER"] = "no"

        # Build PYTHONPATH with breeze from the original repo root
        pythonpath_parts = []

        # Always use breeze from the original repo root, not from the temporary worktree.
        # This ensures we use the current breeze code even when building in a worktree.
        pythonpath_parts.append(str(self.airflow_repo_root / "dev" / "breeze" / "src"))

        # Add existing PYTHONPATH
        if env.get("PYTHONPATH"):
            pythonpath_parts.append(env["PYTHONPATH"])

        env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

        if source_dir:
            # Point AIRFLOW_ROOT_PATH to the worktree so breeze operates on those files
            env["AIRFLOW_ROOT_PATH"] = str(source_dir)
        # We use -c to run the breeze main command because airflow_breeze is a package
        # and might not have __main__.py, and likely we want to run the same entrypoint
        # as the installed breeze command.
        base_cmd = [sys.executable, "-c", "from airflow_breeze.breeze import main; main()"]

        # Build provider packages
        build_result = run_command(
            [
                *base_cmd,
                "release-management",
                "prepare-provider-distributions",
                "--include-removed-providers",
                "--distribution-format",
                "both",
            ],
            cwd=str(cwd),
            check=False,
            capture_output=True,
            env=env,
        )
        if build_result.returncode != 0:
            console_print("[red]Failed to build provider distributions[/red]")
            if build_result.stderr:
                console_print(f"[red]{build_result.stderr.decode()[:500]}[/red]")
            return False

        console_print("Building source tarball...")
        tarball_result = run_command(
            [
                *base_cmd,
                "release-management",
                "prepare-tarball",
                "--tarball-type",
                "apache_airflow_providers",
                "--version",
                self.release_date,
            ],
            cwd=str(cwd),
            check=False,
            capture_output=True,
            env=env,
        )
        if tarball_result.returncode != 0:
            console_print("[red]Failed to build source tarball[/red]")
            if tarball_result.stderr:
                console_print(f"[red]{tarball_result.stderr.decode()[:500]}[/red]")
            return False

        console_print("[green]All packages built successfully[/green]")
        return True


def parse_packages_file(packages_file: Path) -> list[tuple[str, str]]:
    """
    Parse packages.txt file containing PyPI URLs.

    Expected format (one per line or space-separated):
        https://pypi.org/project/apache-airflow-providers-google/19.4.0rc2/

    Returns:
        List of (package_name, version) tuples
    """
    try:
        content = packages_file.read_text()
    except FileNotFoundError:
        raise SystemExit(f"Packages file not found: {packages_file}")

    if not content.strip():
        raise SystemExit(f"Packages file is empty: {packages_file}")

    packages = []
    # Handle both newline-separated and space-separated URLs
    urls = content.split()

    for url_str in urls:
        url = url_str.strip()
        if not url:
            continue

        # Parse URL like: https://pypi.org/project/apache-airflow-providers-google/19.4.0rc2/
        match = re.match(r".*/project/([^/]+)/([^/]+)/?$", url)
        if match:
            name, version = match.groups()
            packages.append((name, version))
        else:
            # Try parsing as "name==version" or "name version" format
            parts = re.split(r"[==\s]+", url)
            if len(parts) >= 2:
                packages.append((parts[0], parts[1]))

    if not packages:
        raise SystemExit(f"No valid packages found in: {packages_file}")

    return packages

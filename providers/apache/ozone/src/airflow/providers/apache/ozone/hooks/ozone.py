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
import shlex
import subprocess
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import Literal, cast, overload

from airflow.providers.apache.ozone.utils.cli_runner import (
    CliRunner,
    OzoneCliRunner,
    ProcessOutputAnalysis,
)
from airflow.providers.apache.ozone.utils.connection_schema import (
    OZONE_CONNECTION_UI_FIELD_BEHAVIOUR,
    OzoneConnSnapshot,
)
from airflow.providers.apache.ozone.utils.errors import (
    ADMIN_RESOURCE_SPECS,
    OzoneCliError,
    OzoneCliErrors,
    OzoneProviderError,
)
from airflow.providers.apache.ozone.utils.helpers import (
    FileHelper,
    JsonDict,
    JsonValue,
    TypeNormalizationHelper,
    URIHelper,
)
from airflow.providers.apache.ozone.utils.security import KerberosConfig, SSLConfig
from airflow.providers.apache.ozone.version_compat import AirflowException, BaseHook, redact

RETRY_ATTEMPTS = 3
FAST_TIMEOUT_SECONDS = 5 * 60
SLOW_TIMEOUT_SECONDS = 60 * 60


class ExistingTargetPolicy(str, Enum):
    """Policy for operations that write to an existing Ozone target."""

    ERROR = "error"
    IGNORE = "ignore"
    OVERWRITE = "overwrite"


class OzoneCliHook(BaseHook):
    """Base hook for Ozone CLI commands with retry and auth handling."""

    conn_name_attr = "ozone_conn_id"
    default_conn_name = "ozone_default"
    conn_type = "ozone"
    hook_name = "Ozone"

    def __init__(
        self,
        ozone_conn_id: str = default_conn_name,
        *,
        retry_attempts: int = RETRY_ATTEMPTS,
    ) -> None:
        super().__init__()
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self._kerberos_ticket_ready = False

    @cached_property
    def connection_snapshot(self) -> OzoneConnSnapshot:
        """Validate and cache required host, port and extra connection fields."""
        return OzoneConnSnapshot.from_connection(
            self.get_connection(self.ozone_conn_id),
            conn_id=self.ozone_conn_id,
            require_host_port=True,
        )

    @cached_property
    def _cached_ssl_env(self) -> dict[str, str] | None:
        """SSL/TLS configuration from snapshot."""
        try:
            ssl_env_vars = SSLConfig.from_snapshot(
                self.connection_snapshot,
                conn_id=self.ozone_conn_id,
                scope="ozone",
            ).as_env()
            if not ssl_env_vars:
                self.log.debug("No SSL/TLS configuration found in connection snapshot")
                return None
            ssl_env = SSLConfig.apply_ssl_env_vars(ssl_env_vars)
            self.log.debug("SSL/TLS configuration loaded from connection: %s", list(ssl_env_vars.keys()))
            if self.connection_snapshot.ozone_security_enabled:
                self.log.info("SSL/TLS enabled for connection")
            return ssl_env
        except AirflowException as err:
            self.log.debug("Could not load SSL configuration: %s", str(err))
            return None

    @cached_property
    def _cached_kerberos_env(self) -> dict[str, str] | None:
        """Kerberos configuration from connection Extra."""
        return KerberosConfig.load_ozone_env(
            snapshot=self.connection_snapshot,
            conn_id=self.ozone_conn_id,
        )

    @cached_property
    def _cached_effective_config_dir(self) -> str | None:
        """Config dir used for CLI --config and subprocess environment."""
        if self.connection_snapshot.ozone_conf_dir:
            self.log.info(
                "Using ozone_conf_dir from connection extra: %s", self.connection_snapshot.ozone_conf_dir
            )
            return self.connection_snapshot.ozone_conf_dir

        for env_name in ("OZONE_CONF_DIR", "HADOOP_CONF_DIR"):
            env_value = os.environ.get(env_name)
            if env_value:
                self.log.info("Using ozone_conf_dir from %s environment variable: %s", env_name, env_value)
                return env_value

        if self.connection_snapshot.kerberos_enabled:
            raise OzoneProviderError(
                "Kerberos is enabled but ozone_conf_dir is not configured. "
                "Set ozone_conf_dir in the connection extra, or provide OZONE_CONF_DIR/HADOOP_CONF_DIR "
                "in the worker environment."
            )
        return None

    def _prepared_cli_env(self) -> dict[str, str]:
        """Build subprocess environment for Ozone CLI calls."""
        env = CliRunner.merge_env(None)
        env["OZONE_OM_ADDRESS"] = f"{self.connection_snapshot.host}:{self.connection_snapshot.port}"

        if self._cached_ssl_env:
            env.update(self._cached_ssl_env)
        if self._cached_kerberos_env:
            env.update(self._cached_kerberos_env)

        config_dir = self._cached_effective_config_dir
        if config_dir:
            env["OZONE_CONF_DIR"] = config_dir
            env["HADOOP_CONF_DIR"] = config_dir

        self.log.debug("Prepared Ozone CLI env for connection '%s'", self.ozone_conn_id)
        return env

    @classmethod
    def get_ui_field_behaviour(cls) -> JsonDict:
        """Describe Ozone connection extras in Airflow UI."""
        return OZONE_CONNECTION_UI_FIELD_BEHAVIOUR

    def test_connection(self) -> tuple[bool, str]:
        """Run a minimal CLI command to verify Ozone auth and connectivity."""
        try:
            result = self.run_cli(
                ["ozone", "sh", "volume", "list", "/"],
                timeout=FAST_TIMEOUT_SECONDS,
                retry_attempts=0,
                check=False,
                log_output=False,
                return_result=True,
            )
        except OzoneCliError as err:
            return False, f"Ozone CLI connection test failed: {err}"

        if result.returncode == 0:
            return True, "Ozone CLI connection test succeeded."

        error_text = ProcessOutputAnalysis.pick_process_output(result) or "Unknown CLI error"
        return False, f"Ozone CLI connection test failed: {error_text}"

    def _prepare_cli_command(self, cmd: list[str]) -> list[str]:
        """Add --config for Kerberos-enabled commands when available."""
        if not self.connection_snapshot.kerberos_enabled:
            return cmd

        if "--config" in cmd:
            return cmd

        config_dir = self._cached_effective_config_dir
        config_files_exist = KerberosConfig.check_config_files_exist(
            config_dir,
            snapshot=self.connection_snapshot,
        )
        if not config_files_exist:
            self.log.warning(
                "Kerberos enabled but configuration files (core-site.xml, ozone-site.xml) "
                "not found in %s. Adding --config flag anyway as it is critical for Kerberos authentication.",
                config_dir,
            )
        else:
            self.log.debug("Configuration files found in %s, adding --config flag", config_dir)

        if cmd[:1] == ["ozone"]:
            if not config_dir:
                return cmd
            new_cmd = [cmd[0], "--config", config_dir, *cmd[1:]]
            self.log.debug("Added --config flag to Ozone CLI command: %s", redact(shlex.join(new_cmd)))
            return new_cmd

        return cmd

    def runtime_mode_label(self) -> str:
        """Return connection runtime mode label for operator/hook logs."""
        ssl_enabled = self.connection_snapshot.ssl_enabled
        kerberos_enabled = self.connection_snapshot.kerberos_enabled

        if ssl_enabled and kerberos_enabled:
            return "ssl+kerberos"
        if ssl_enabled:
            return "ssl"
        if kerberos_enabled:
            return "kerberos"
        return "plain"

    @overload
    def run_cli(
        self,
        cmd: list[str],
        *,
        timeout: int = ...,
        retry_attempts: int | None = ...,
        input_text: str | None = ...,
        check: bool = ...,
        log_output: bool = ...,
        return_result: Literal[True],
        return_json_result: Literal[False] = False,
    ) -> subprocess.CompletedProcess[str]: ...

    @overload
    def run_cli(
        self,
        cmd: list[str],
        *,
        timeout: int = ...,
        retry_attempts: int | None = ...,
        input_text: str | None = ...,
        check: bool = ...,
        log_output: bool = ...,
        return_result: Literal[False] = False,
        return_json_result: Literal[True],
    ) -> JsonValue: ...

    @overload
    def run_cli(
        self,
        cmd: list[str],
        *,
        timeout: int = ...,
        retry_attempts: int | None = ...,
        input_text: str | None = ...,
        check: bool = ...,
        log_output: bool = ...,
        return_result: Literal[False] = False,
        return_json_result: Literal[False] = False,
    ) -> str: ...

    def run_cli(
        self,
        cmd: list[str],
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
        retry_attempts: int | None = None,
        input_text: str | None = None,
        check: bool = True,
        log_output: bool = True,
        return_result: bool = False,
        return_json_result: bool = False,
    ) -> str | subprocess.CompletedProcess[str] | JsonValue:
        """Execute Ozone CLI command with common auth, retry and logging handling."""
        if return_result and return_json_result:
            raise ValueError("return_result and return_json_result cannot be enabled together")
        self.log.info(
            "Executing Ozone CLI command (connection: %s, mode: %s)",
            self.ozone_conn_id,
            self.runtime_mode_label(),
        )
        self._kerberos_ticket_ready = KerberosConfig.ensure_ticket(
            snapshot=self.connection_snapshot,
            conn_id=self.ozone_conn_id,
            kerberos_ticket_ready=self._kerberos_ticket_ready,
        )
        prepared_cmd = self._prepare_cli_command(cmd)
        effective_retry_attempts = self.retry_attempts if retry_attempts is None else retry_attempts
        result = OzoneCliRunner.run_ozone(
            prepared_cmd,
            env_overrides=self._prepared_cli_env(),
            timeout=timeout,
            input_text=input_text,
            check=check,
            log_output=log_output,
            retry_attempts=effective_retry_attempts,
        )
        if return_result:
            return result
        output = ProcessOutputAnalysis.pick_process_output(result)
        if return_json_result:
            return TypeNormalizationHelper.parse_json_output(output)
        return output


class OzoneFsHook(OzoneCliHook):
    """Interact with Ozone through the Hadoop-compatible ozone fs interface."""

    hook_name = "Ozone FS"

    @staticmethod
    def _validate_if_exists(
        value: ExistingTargetPolicy | str,
        *,
        allow_overwrite: bool = True,
    ) -> ExistingTargetPolicy:
        """Validate existing-target policy used by create/copy/upload helpers."""
        allowed: set[ExistingTargetPolicy] = {ExistingTargetPolicy.ERROR, ExistingTargetPolicy.IGNORE}
        if allow_overwrite:
            allowed.add(ExistingTargetPolicy.OVERWRITE)
        try:
            policy = ExistingTargetPolicy(value)
        except ValueError:
            expected = ", ".join(sorted(allowed))
            raise ValueError(f"if_exists must be one of: {expected}. Got: {value!r}")
        if policy not in allowed:
            expected = ", ".join(sorted(allowed))
            raise ValueError(f"if_exists must be one of: {expected}. Got: {value!r}")
        return policy

    # ==============================
    # Key operations
    # ==============================
    def create_key(
        self,
        path: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
    ) -> None:
        """Create an empty key in Ozone FS."""
        policy = self._validate_if_exists(if_exists)
        if self.exists(path, timeout=timeout):
            if policy == ExistingTargetPolicy.IGNORE:
                self.log.info("Ozone key %s already exists, treating as success.", path)
                return
            if policy == ExistingTargetPolicy.OVERWRITE:
                self.delete_key(path, timeout=timeout)
            else:
                raise OzoneProviderError(f"Ozone key already exists: {path}")
        self.run_cli(self._fs_cmd("-touchz", path), timeout=timeout)

    def key_exists(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> bool:
        """Return True when a key exists in Ozone FS."""
        return self.exists(path, timeout=timeout)

    def list_keys(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> list[str]:
        """List keys for a plain path or wildcard pattern."""
        if URIHelper.contains_wildcards(path):
            matched = self.list_wildcard_matches(path, timeout=timeout, fallback_action="listing")
            if matched is None:
                return self.list_paths(path, timeout=timeout)
            return matched
        return self.list_paths(path, timeout=timeout)

    def glob(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> list[str]:
        """Return wildcard matches for a key or path pattern."""
        return self.list_keys(path, timeout=timeout)

    def get_key_info(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> JsonDict:
        """Return full metadata for a key using ozone sh key info."""
        parsed = self.run_cli(
            ["ozone", "sh", "key", "info", URIHelper.to_key_uri(path)],
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from key info {path}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    def get_key_property(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> JsonDict:
        """Return the most useful key properties as a compact dictionary."""
        info = self.get_key_info(path, timeout=timeout)
        replication_config = info.get("replicationConfig")
        if replication_config is None:
            self.log.warning(
                "Key info for %s does not contain 'replicationConfig'; falling back to top-level fields",
                path,
            )
        if isinstance(replication_config, dict):
            replication_type = replication_config.get("replicationType", info.get("replicationType"))
            replication_value = replication_config.get("requiredNodes", info.get("replicationFactor"))
        else:
            replication_type = info.get("replicationType")
            replication_value = info.get("replicationFactor")

        return {
            "volume_name": info.get("volumeName"),
            "bucket_name": info.get("bucketName"),
            "name": info.get("name"),
            "data_size": info.get("dataSize"),
            "creation_time": info.get("creationTime"),
            "modification_time": info.get("modificationTime"),
            "replication_type": replication_type,
            "replication": replication_value,
            "metadata": info.get("metadata"),
            "file_encryption_info": info.get("fileEncryptionInfo"),
        }

    def read_text(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> str:
        """Read a small text key from Ozone FS."""
        return self.run_cli(self._fs_cmd("-cat", path), timeout=timeout)

    def set_key_property(
        self,
        path: str,
        replication_factor: int | None = None,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """
        Set writable key properties.

        Currently only replication factor is exposed because it is the only key-level
        property already used in the provider codebase.
        """
        if replication_factor is None:
            self.log.debug("No key properties were specified to be set for path: %s", path)
            return

        self.run_cli(
            self._fs_cmd("-setrep", str(replication_factor), path),
            timeout=timeout,
        )

    def delete_key(self, path: str, *, timeout: int = SLOW_TIMEOUT_SECONDS) -> None:
        """Delete a key or a wildcard-matched group of keys."""
        self._delete_fs(
            path,
            recursive=False,
            timeout=timeout,
            fallback_action="direct delete",
        )

    # ==============================
    # Path operations
    # ==============================
    def create_path(
        self,
        path: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
        recursive: bool = True,
        fail_if_exists: bool = False,
    ) -> None:
        """
        Create a directory tree in Ozone FS.

        Deprecated. Use `make_path(..., if_exists=...)` instead.
        """
        self.log.warning(
            "OzoneFsHook.create_path(..., fail_if_exists=...) is deprecated and will be removed "
            "in a future version. Use make_path(..., if_exists=...) instead."
        )
        policy = ExistingTargetPolicy.ERROR if fail_if_exists else ExistingTargetPolicy.IGNORE
        self.make_path(path, timeout=timeout, recursive=recursive, if_exists=policy)

    def make_path(
        self,
        path: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
        recursive: bool = True,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.IGNORE,
    ) -> None:
        """Create a directory tree in Ozone FS."""
        policy = self._validate_if_exists(if_exists, allow_overwrite=False)
        target_path = path.rstrip("/")
        if not target_path:
            return
        if self.exists(target_path, timeout=timeout):
            if policy == ExistingTargetPolicy.IGNORE:
                self.log.info("Ozone path %s already exists, treating as success.", target_path)
                return
            raise OzoneProviderError(f"Destination path already exists: {target_path}")

        cmd = self._fs_cmd("-mkdir")
        if recursive:
            cmd.append("-p")
        cmd.append(target_path)

        self.run_cli(cmd, timeout=timeout)

    def exists(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> bool:
        """Return True when a file or directory exists in Ozone FS."""
        result = self.run_cli(
            self._fs_cmd("-test", "-e", path),
            timeout=timeout,
            retry_attempts=0,
            check=False,
            log_output=False,
            return_result=True,
        )
        if result.returncode == 0:
            return True

        output = ProcessOutputAnalysis.from_completed_process(result)
        normalized = output.normalized_meaningful_output
        if any(marker in normalized for marker in ("not found", "does not exist", "no such file")):
            return False
        if result.returncode == 1 and not output.meaningful_lines:
            self.log.debug(
                "Treating `ozone fs -test -e` return code 1 for %s as missing path because only noise output was produced.",
                path,
            )
            return False

        error_text = output.preferred_output or "Unknown CLI error"
        raise OzoneCliError(
            f"Ozone FS existence check failed (return code: {result.returncode}): {redact(error_text)}",
            command=self._fs_cmd("-test", "-e", path),
            stderr=result.stderr,
            returncode=result.returncode,
            retryable=OzoneCliErrors.is_retryable_failure(result.returncode, error_text),
        )

    def path_exists(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> bool:
        """Return True when a path exists in Ozone FS."""
        return self.exists(path, timeout=timeout)

    def list_paths(self, path: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> list[str]:
        """List file system paths under the provided Ozone URI."""
        output = self.run_cli(self._fs_cmd("-ls", "-C", path), timeout=timeout)
        paths, skipped_lines = ProcessOutputAnalysis.extract_meaningful_output_lines(
            output,
            accept_line=lambda line: (
                URIHelper.parse_ozone_uri(line)[0] in {"ofs", "o3fs", "o3"} or line.startswith("/")
            ),
        )

        if skipped_lines:
            self.log.debug(
                "Ignored %d non-path line(s) from `ozone fs -ls` output: %s",
                len(skipped_lines),
                skipped_lines[:3],
            )

        return paths

    def delete_path(
        self,
        path: str,
        *,
        recursive: bool = True,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Delete a file or directory path, optionally recursively."""
        self._delete_fs(
            path,
            recursive=recursive,
            timeout=timeout,
            fallback_action="direct path delete",
        )

    # ==============================
    # Transfer operations
    # ==============================
    def upload_key(
        self,
        local_path: str,
        remote_path: str,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
    ) -> None:
        """Upload a local file to an Ozone URI."""
        policy = self._validate_if_exists(if_exists)
        local_file = Path(local_path)
        if not FileHelper.is_readable_file(local_file):
            raise OzoneProviderError(f"Local file does not exist or is not readable: {local_path}")

        if self.exists(remote_path, timeout=timeout):
            if policy == ExistingTargetPolicy.IGNORE:
                self.log.info("Remote path %s already exists, treating as success.", remote_path)
                return
            if policy == ExistingTargetPolicy.ERROR:
                raise OzoneProviderError(f"Remote path already exists: {remote_path}")
            self.log.info("Remote path %s already exists, deleting it before upload.", remote_path)
            self.delete_key(remote_path, timeout=timeout)

        cmd = ["ozone", "sh", "key", "put", URIHelper.to_key_uri(remote_path), str(local_file)]
        self.run_cli(cmd, timeout=timeout)

    def download_key(
        self,
        remote_path: str,
        local_path: str,
        *,
        overwrite: bool = False,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Download one file from Ozone to the local filesystem."""
        local_file = Path(local_path)
        if local_file.exists() and not overwrite:
            raise OzoneProviderError(
                f"Local file already exists: {local_path}. Set overwrite=True to replace it."
            )
        local_file.parent.mkdir(parents=True, exist_ok=True)
        if not FileHelper.is_writable_target(local_file):
            raise OzoneProviderError(f"Local target path is not writable: {local_path}")

        cmd = self._fs_cmd("-get")
        if overwrite:
            cmd.append("-f")
        cmd.extend([remote_path, str(local_file)])
        self.run_cli(cmd, timeout=timeout)

    def move(
        self,
        source_path: str,
        dest_path: str,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
    ) -> None:
        """Move or rename one key or wildcard-selected keys within Ozone FS."""
        self._transfer_path(
            "-mv",
            source_path,
            dest_path,
            timeout=timeout,
            fallback_action="direct move",
            if_exists=if_exists,
        )

    def copy_path(
        self,
        source_path: str,
        dest_path: str,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
    ) -> None:
        """Copy one key or wildcard-selected keys within Ozone FS."""
        self._transfer_path(
            "-cp",
            source_path,
            dest_path,
            timeout=timeout,
            fallback_action="direct copy",
            if_exists=if_exists,
        )

    # ==============================
    # Shared FS helpers
    # ==============================
    def _fs_cmd(self, action: str, *args: str) -> list[str]:
        """Build an ozone fs command."""
        return ["ozone", "fs", action, *args]

    def list_wildcard_matches(
        self,
        path: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
        fallback_action: str = "direct operation",
    ) -> list[str] | None:
        """Resolve wildcard matches, or return None when listing should fall back."""
        try:
            _, matched = URIHelper.resolve_wildcard_matches(
                path,
                lambda current_path: self.list_paths(current_path, timeout=timeout),
            )
        except OzoneCliError as err:
            if not err.retryable:
                raise
            source_dir, _ = URIHelper.split_ozone_wildcard_path(path)
            self.log.warning(
                "Could not list wildcard source directory %s (%s); falling back to %s for %s",
                source_dir,
                type(err).__name__,
                fallback_action,
                path,
            )
            return None
        return matched

    def _delete_fs(
        self,
        path: str,
        *,
        recursive: bool,
        timeout: int,
        fallback_action: str,
    ) -> None:
        """Shared delete implementation for key and path deletion."""
        rm_cmd = self._fs_cmd("-rm")
        if recursive:
            rm_cmd.append("-r")

        if not URIHelper.contains_wildcards(path):
            if self.exists(path, timeout=timeout):
                self.run_cli([*rm_cmd, path], timeout=timeout)
            return

        matched = self.list_wildcard_matches(path, timeout=timeout, fallback_action=fallback_action)
        if matched is None:
            self.run_cli([*rm_cmd, path], timeout=timeout)
            return

        for matched_path in matched:
            self.run_cli([*rm_cmd, matched_path], timeout=timeout)

    def _transfer_path(
        self,
        action: str,
        source_path: str,
        dest_path: str,
        *,
        timeout: int,
        fallback_action: str,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
    ) -> None:
        """Shared move and copy implementation with wildcard handling."""
        policy = self._validate_if_exists(if_exists, allow_overwrite=False)
        if URIHelper.contains_wildcards(source_path):
            matched = self.list_wildcard_matches(
                source_path, timeout=timeout, fallback_action=fallback_action
            )
            if matched is None:
                self.make_path(dest_path, timeout=timeout)
                self.run_cli(self._fs_cmd(action, source_path, dest_path), timeout=timeout)
                return

            if not matched:
                return

            self.make_path(dest_path, timeout=timeout)
            dest_dir = dest_path.rstrip("/")
            for matched_path in matched:
                _, filename = URIHelper.split_ozone_path(matched_path)
                dest_file_path = URIHelper.join_ozone_path(dest_dir, filename)
                if self.exists(dest_file_path, timeout=timeout):
                    if policy == ExistingTargetPolicy.IGNORE:
                        self.log.info("Destination path %s already exists, skipping.", dest_file_path)
                        continue
                    raise OzoneProviderError(f"Destination path already exists: {dest_file_path}")
                self.run_cli(self._fs_cmd(action, matched_path, dest_file_path), timeout=timeout)
            return

        parent_path, _ = URIHelper.split_ozone_path(dest_path)
        if parent_path:
            self.make_path(parent_path, timeout=timeout)
        if self.exists(dest_path, timeout=timeout):
            if policy == ExistingTargetPolicy.IGNORE:
                self.log.info("Destination path %s already exists, treating as success.", dest_path)
                return
            raise OzoneProviderError(f"Destination path already exists: {dest_path}")
        if self.exists(source_path, timeout=timeout):
            self.run_cli(self._fs_cmd(action, source_path, dest_path), timeout=timeout)


class OzoneResource(str, Enum):
    """Supported Ozone admin resource types."""

    VOLUME = "volume"
    BUCKET = "bucket"


class OzoneAdminHook(OzoneCliHook):
    """Interact with core namespace admin operations through ozone sh."""

    default_conn_name = "ozone_admin_default"
    hook_name = "Ozone Admin"

    # ==============================
    # Volume operations
    # ==============================
    def create_volume(
        self,
        volume_name: str,
        quota: str | None = None,
        *,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
        owner: str | None = None,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.IGNORE,
    ) -> None:
        """Create a volume with optional quotas and owner."""
        self._create_resource(
            resource=OzoneResource.VOLUME,
            resource_path=self._resource_path(volume_name),
            quota=quota,
            space_quota=space_quota,
            namespace_quota=namespace_quota,
            owner=owner,
            timeout=timeout,
            if_exists=if_exists,
        )

    def list_volumes(self, *, timeout: int = FAST_TIMEOUT_SECONDS) -> list[JsonDict]:
        """List all volumes visible to the current user."""
        return self._list_resources(
            resource=OzoneResource.VOLUME,
            parent_path="/",
            timeout=timeout,
        )

    def get_volume_info(self, volume_name: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> JsonDict:
        """Return detailed metadata for a volume."""
        return self._get_resource_info(volume=volume_name, timeout=timeout)

    def volume_exists(self, volume_name: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> bool:
        """Return True when the target volume exists."""
        return self._resource_exists(volume=volume_name, timeout=timeout)

    def delete_volume(
        self,
        volume_name: str,
        recursive: bool = False,
        force: bool = False,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Delete a volume, optionally recursively."""
        self._delete_resource(
            resource=OzoneResource.VOLUME,
            resource_path=self._resource_path(volume_name),
            recursive=recursive,
            force=force,
            timeout=timeout,
        )

    def set_volume_owner(
        self,
        volume_name: str,
        owner: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Change the owner of an existing volume."""
        parsed = self.run_cli(
            ["ozone", "sh", "volume", "update", self._resource_path(volume_name), "--user", owner],
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from volume update {volume_name}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    # ==============================
    # Bucket operations
    # ==============================
    def create_bucket(
        self,
        volume_name: str,
        bucket_name: str,
        quota: str | None = None,
        *,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
        owner: str | None = None,
        layout: str | None = None,
        replication_type: str | None = None,
        replication: str | None = None,
        encryption_key: str | None = None,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.IGNORE,
    ) -> None:
        """Create a bucket with optional owner, layout, quotas, replication and encryption key."""
        extra_args: list[str] = []
        if owner:
            extra_args.extend(["--user", owner])
        if layout:
            extra_args.extend(["--layout", layout])
        if replication_type:
            extra_args.extend(["--type", replication_type])
        if replication:
            extra_args.extend(["--replication", replication])
        if encryption_key:
            extra_args.extend(["--bucketkey", encryption_key])

        self._create_resource(
            resource=OzoneResource.BUCKET,
            resource_path=self._resource_path(volume_name, bucket_name),
            quota=quota,
            space_quota=space_quota,
            namespace_quota=namespace_quota,
            extra_args=extra_args,
            timeout=timeout,
            if_exists=if_exists,
        )

    def list_buckets(self, volume_name: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> list[JsonDict]:
        """List buckets under the given volume."""
        return self._list_resources(
            resource=OzoneResource.BUCKET,
            parent_path=self._resource_path(volume_name),
            timeout=timeout,
        )

    def get_bucket_info(
        self,
        volume_name: str,
        bucket_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Return detailed metadata for a bucket."""
        return self._get_resource_info(volume=volume_name, bucket=bucket_name, timeout=timeout)

    def bucket_exists(
        self,
        volume_name: str,
        bucket_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> bool:
        """Return True when the target bucket exists."""
        return self._resource_exists(volume=volume_name, bucket=bucket_name, timeout=timeout)

    def delete_bucket(
        self,
        volume_name: str,
        bucket_name: str,
        recursive: bool = False,
        force: bool = False,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Delete a bucket, optionally recursively."""
        self._delete_resource(
            resource=OzoneResource.BUCKET,
            resource_path=self._resource_path(volume_name, bucket_name),
            recursive=recursive,
            force=force,
            timeout=timeout,
        )

    def set_bucket_owner(
        self,
        volume_name: str,
        bucket_name: str,
        owner: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Change the owner of an existing bucket."""
        parsed = self.run_cli(
            [
                "ozone",
                "sh",
                "bucket",
                "update",
                self._resource_path(volume_name, bucket_name),
                "--user",
                owner,
            ],
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                "Unexpected JSON payload from bucket update "
                f"{volume_name}/{bucket_name}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    # ==============================
    # Quota operations
    # ==============================
    def set_quota(
        self,
        *,
        volume: str,
        bucket: str | None = None,
        quota: str | None = None,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """Set space and/or namespace quota for a volume or bucket."""
        if quota is None and space_quota is None and namespace_quota is None:
            raise ValueError("At least one quota must be provided.")

        self.run_cli(
            self._quota_cmd(
                "setquota",
                volume=volume,
                bucket=bucket,
                quota=quota,
                space_quota=space_quota,
                namespace_quota=namespace_quota,
            ),
            timeout=timeout,
        )

    def clear_quota(
        self,
        *,
        volume: str,
        bucket: str | None = None,
        clear_space_quota: bool = True,
        clear_namespace_quota: bool = False,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """Clear space and/or namespace quota for a volume or bucket."""
        if not clear_space_quota and not clear_namespace_quota:
            raise ValueError("At least one quota flag must be set to True.")

        resource, target = self._resolve_admin_target(volume=volume, bucket=bucket)
        cmd = self._admin_cmd(resource, "clrquota")
        if clear_space_quota:
            cmd.append("--space-quota")
        if clear_namespace_quota:
            cmd.append("--namespace-quota")
        cmd.append(target)
        self.run_cli(cmd, timeout=timeout)

    def get_quota(
        self,
        *,
        volume: str,
        bucket: str | None = None,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Return quota and usage fields from volume or bucket info."""
        info = self._get_resource_info(volume=volume, bucket=bucket, timeout=timeout)
        return {
            "quota_in_bytes": info.get("quotaInBytes"),
            "quota_in_namespace": info.get("quotaInNamespace"),
            "used_bytes": info.get("usedBytes"),
            "used_namespace": info.get("usedNamespace"),
        }

    def set_volume_quota(
        self,
        volume_name: str,
        *,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """Set one or both quota types for a volume."""
        self.set_quota(
            volume=volume_name,
            space_quota=space_quota,
            namespace_quota=namespace_quota,
            timeout=timeout,
        )

    def set_bucket_quota(
        self,
        volume_name: str,
        bucket_name: str,
        *,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """Set one or both quota types for a bucket."""
        self.set_quota(
            volume=volume_name,
            bucket=bucket_name,
            space_quota=space_quota,
            namespace_quota=namespace_quota,
            timeout=timeout,
        )

    def clear_volume_quota(
        self,
        volume_name: str,
        *,
        clear_space_quota: bool = True,
        clear_namespace_quota: bool = False,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """Clear one or both quota types for a volume."""
        self.clear_quota(
            volume=volume_name,
            clear_space_quota=clear_space_quota,
            clear_namespace_quota=clear_namespace_quota,
            timeout=timeout,
        )

    def clear_bucket_quota(
        self,
        volume_name: str,
        bucket_name: str,
        *,
        clear_space_quota: bool = True,
        clear_namespace_quota: bool = False,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> None:
        """Clear one or both quota types for a bucket."""
        self.clear_quota(
            volume=volume_name,
            bucket=bucket_name,
            clear_space_quota=clear_space_quota,
            clear_namespace_quota=clear_namespace_quota,
            timeout=timeout,
        )

    def get_volume_quota(self, volume_name: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> JsonDict:
        """Return quota and usage fields for a volume."""
        return self.get_quota(volume=volume_name, timeout=timeout)

    def get_bucket_quota(
        self,
        volume_name: str,
        bucket_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Return quota and usage fields for a bucket."""
        return self.get_quota(volume=volume_name, bucket=bucket_name, timeout=timeout)

    # ==============================
    # Shared admin helpers
    # ==============================
    def _admin_cmd(self, resource: OzoneResource, action: str, *args: str) -> list[str]:
        """Build an ozone sh command for a resource."""
        return ["ozone", "sh", resource.value, action, *args]

    def _resource_path(self, volume: str, bucket: str | None = None) -> str:
        """Build a native Ozone resource path."""
        if bucket:
            return f"/{volume}/{bucket}"
        return f"/{volume}"

    def _resolve_admin_target(
        self,
        *,
        volume: str,
        bucket: str | None = None,
    ) -> tuple[OzoneResource, str]:
        """Resolve resource type and path for volume-or-bucket methods."""
        if bucket:
            return OzoneResource.BUCKET, self._resource_path(volume, bucket)
        return OzoneResource.VOLUME, self._resource_path(volume)

    def _normalize_quota_inputs(
        self,
        *,
        quota: str | None = None,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
    ) -> list[str]:
        """Build quota CLI args while keeping old quota as a compatibility alias."""
        if quota is not None and space_quota is not None:
            raise ValueError("Use either quota or space_quota, but not both.")

        effective_space_quota = space_quota if space_quota is not None else quota
        args: list[str] = []
        if effective_space_quota is not None:
            args.extend(["--space-quota", str(effective_space_quota)])
        if namespace_quota is not None:
            args.extend(["--namespace-quota", str(namespace_quota)])
        return args

    def _create_resource(
        self,
        *,
        resource: OzoneResource,
        resource_path: str,
        quota: str | None = None,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
        owner: str | None = None,
        extra_args: list[str] | None = None,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.IGNORE,
    ) -> None:
        """Create a volume or bucket and treat already-exists errors as success."""
        policy = OzoneFsHook._validate_if_exists(if_exists, allow_overwrite=False)
        spec = ADMIN_RESOURCE_SPECS[resource.value]
        cmd = self._admin_cmd(
            resource,
            "create",
            *self._normalize_quota_inputs(
                quota=quota,
                space_quota=space_quota,
                namespace_quota=namespace_quota,
            ),
        )
        if owner:
            cmd.extend(["--user", owner])
        if extra_args:
            cmd.extend(extra_args)
        cmd.append(resource_path)

        result = self.run_cli(
            cmd,
            timeout=timeout,
            retry_attempts=0,
            check=False,
            log_output=False,
            return_result=True,
        )
        if result.returncode == 0:
            return

        output = ProcessOutputAnalysis.from_completed_process(result)
        error_message = output.preferred_output or "No error message provided"
        if spec.already_exists_marker.lower() in error_message.lower():
            if policy == ExistingTargetPolicy.ERROR:
                raise OzoneProviderError(f"{resource.value.capitalize()} already exists: {resource_path}")
            self.log.info(
                "%s %s already exists, treating as success.", resource.value.capitalize(), resource_path
            )
            return

        raise OzoneProviderError(
            f"Ozone command failed (return code: {result.returncode}): {redact(error_message)}"
        )

    def _delete_resource(
        self,
        *,
        resource: OzoneResource,
        resource_path: str,
        recursive: bool = False,
        force: bool = False,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> None:
        """Delete a volume or bucket with idempotent not-found handling."""
        spec = ADMIN_RESOURCE_SPECS[resource.value]
        cmd = self._admin_cmd(resource, "delete")
        if recursive:
            cmd.append("-r")
        cmd.append(resource_path)
        input_text = "yes\n" if recursive and force else None

        result = self.run_cli(
            cmd,
            timeout=timeout,
            input_text=input_text,
            retry_attempts=0,
            check=False,
            log_output=False,
            return_result=True,
        )
        if result.returncode == 0:
            return

        output = ProcessOutputAnalysis.from_completed_process(result)
        error_text = output.preferred_output or "Unknown error"
        normalized = output.normalized_preferred_output
        if (
            any(marker.lower() in normalized for marker in spec.not_found_markers)
            or "does not exist" in normalized
        ):
            self.log.info(
                "%s %s does not exist, treating as success.", resource.value.capitalize(), resource_path
            )
            return

        raise OzoneProviderError(f"Failed to delete {resource.value} {resource_path}: {redact(error_text)}")

    def _get_resource_info(
        self,
        *,
        volume: str,
        bucket: str | None = None,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Return JSON info for a volume or bucket."""
        resource, target = self._resolve_admin_target(volume=volume, bucket=bucket)
        parsed = self.run_cli(
            self._admin_cmd(resource, "info", target),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from {resource.value} info {target}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    def _resource_exists(
        self,
        *,
        volume: str,
        bucket: str | None = None,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> bool:
        """Check existence of a volume or bucket using the corresponding info command."""
        resource, target = self._resolve_admin_target(volume=volume, bucket=bucket)
        spec = ADMIN_RESOURCE_SPECS[resource.value]

        result = self.run_cli(
            self._admin_cmd(resource, "info", target),
            timeout=timeout,
            retry_attempts=0,
            check=False,
            log_output=False,
            return_result=True,
        )
        if result.returncode == 0:
            return True

        output = ProcessOutputAnalysis.from_completed_process(result)
        error_text = output.preferred_output
        normalized = output.normalized_preferred_output
        if (
            any(marker.lower() in normalized for marker in spec.not_found_markers)
            or "does not exist" in normalized
        ):
            return False

        raise OzoneProviderError(
            f"Failed to check {resource.value} existence for {target}: {redact(error_text or 'Unknown error')}"
        )

    def _list_resources(
        self,
        *,
        resource: OzoneResource,
        parent_path: str,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> list[JsonDict]:
        """Return parsed list output for volume or bucket commands."""
        parsed = self.run_cli(
            self._admin_cmd(resource, "list", parent_path),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, list):
            raise OzoneProviderError(
                f"Unexpected JSON payload from {resource.value} list {parent_path}: expected list, got {type(parsed).__name__}."
            )
        return cast("list[JsonDict]", parsed)

    def _quota_cmd(
        self,
        action: str,
        *,
        volume: str,
        bucket: str | None = None,
        quota: str | None = None,
        space_quota: str | None = None,
        namespace_quota: str | int | None = None,
    ) -> list[str]:
        """Build a quota-related command for a volume or bucket."""
        resource, target = self._resolve_admin_target(volume=volume, bucket=bucket)
        return self._admin_cmd(
            resource,
            action,
            *self._normalize_quota_inputs(
                quota=quota,
                space_quota=space_quota,
                namespace_quota=namespace_quota,
            ),
            target,
        )


class OzoneAdminExtraHook(OzoneAdminHook):
    """Interact with advanced admin features: snapshots, tenants and cluster reports."""

    hook_name = "Ozone Admin Extra"

    # ==============================
    # Advanced bucket operations
    # ==============================
    def set_bucket_replication_config(
        self,
        volume_name: str,
        bucket_name: str,
        replication_type: str,
        replication: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """Set default replication config for new keys in a bucket."""
        return self.run_cli(
            [
                "ozone",
                "sh",
                "bucket",
                "set-replication-config",
                self._resource_path(volume_name, bucket_name),
                "--type",
                replication_type,
                "--replication",
                replication,
            ],
            timeout=timeout,
        )

    def create_bucket_link(
        self,
        source_volume: str,
        source_bucket: str,
        target_volume: str,
        target_bucket: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """Create a bucket link pointing to another bucket in the same cluster."""
        return self.run_cli(
            [
                "ozone",
                "sh",
                "bucket",
                "link",
                self._resource_path(source_volume, source_bucket),
                self._resource_path(target_volume, target_bucket),
            ],
            timeout=timeout,
        )

    # ==============================
    # Snapshot operations
    # ==============================
    def create_snapshot(
        self,
        volume_name: str,
        bucket_name: str,
        snapshot_name: str | None = None,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> str:
        """Create a point-in-time snapshot for a bucket."""
        cmd = self._snapshot_cmd("create", self._resource_path(volume_name, bucket_name))
        if snapshot_name:
            cmd.append(snapshot_name)
        return self.run_cli(cmd, timeout=timeout)

    def list_snapshots(
        self,
        volume_name: str,
        bucket_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> list[str]:
        """List snapshots for a bucket."""
        output = self.run_cli(
            self._snapshot_cmd("list", self._resource_path(volume_name, bucket_name)),
            timeout=timeout,
        )
        return output.splitlines() if output else []

    def get_snapshot_info(
        self,
        volume_name: str,
        bucket_name: str,
        snapshot_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """Return raw snapshot information for a named snapshot."""
        return self.run_cli(
            self._snapshot_cmd("info", self._resource_path(volume_name, bucket_name), snapshot_name),
            timeout=timeout,
        )

    def diff_snapshots(
        self,
        volume_name: str,
        bucket_name: str,
        from_snapshot: str,
        to_snapshot_or_bucket: str,
        *,
        page_size: int | None = None,
        continuation_token: str | None = None,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> str:
        """Return diff output between two snapshots or a snapshot and the live bucket."""
        cmd = self._snapshot_cmd("diff")
        if page_size is not None:
            cmd.extend(["-p", str(page_size)])
        if continuation_token:
            cmd.extend(["-t", continuation_token])
        cmd.extend(
            [
                self._resource_path(volume_name, bucket_name),
                from_snapshot,
                to_snapshot_or_bucket,
            ]
        )
        return self.run_cli(cmd, timeout=timeout)

    def list_snapshot_diff_jobs(
        self,
        volume_name: str,
        bucket_name: str,
        *,
        job_status: str | None = None,
        all_status: bool = False,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """List snapshot diff jobs for a bucket."""
        cmd = self._snapshot_cmd("listDiff", self._resource_path(volume_name, bucket_name))
        if job_status:
            cmd.extend(["--job-status", job_status])
        if all_status:
            cmd.append("--all-status")
        return self.run_cli(cmd, timeout=timeout)

    def cancel_snapshot_diff(self, job_id: str, *, timeout: int = FAST_TIMEOUT_SECONDS) -> str:
        """Cancel an in-progress snapshot diff job by its job ID."""
        return self.run_cli(self._snapshot_cmd("cancelDiff", job_id), timeout=timeout)

    def delete_snapshot(
        self,
        volume_name: str,
        bucket_name: str,
        snapshot_name: str,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> str:
        """Delete a named snapshot from a bucket."""
        return self.run_cli(
            self._snapshot_cmd("delete", self._resource_path(volume_name, bucket_name), snapshot_name),
            timeout=timeout,
        )

    # ==============================
    # Tenant operations
    # ==============================
    def create_tenant(
        self,
        tenant_name: str,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Create a new tenant and return verbose JSON metadata."""
        parsed = self.run_cli(
            self._tenant_cmd("--verbose", "create", tenant_name),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from tenant create {tenant_name}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    def list_tenants(self, *, timeout: int = FAST_TIMEOUT_SECONDS) -> list[JsonDict]:
        """List all tenants in JSON form."""
        parsed = self.run_cli(
            self._tenant_cmd("list", "--json"),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, list):
            raise OzoneProviderError(
                f"Unexpected JSON payload from tenant list: expected list, got {type(parsed).__name__}."
            )
        return cast("list[JsonDict]", parsed)

    def delete_tenant(
        self,
        tenant_name: str,
        *,
        timeout: int = SLOW_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Delete an empty tenant and return verbose JSON metadata."""
        parsed = self.run_cli(
            self._tenant_cmd("--verbose", "delete", tenant_name),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from tenant delete {tenant_name}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    def tenant_assign_user(
        self,
        user_name: str,
        tenant_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """Assign a user to a tenant and let Ozone generate access credentials."""
        return self.run_cli(
            self._tenant_cmd("user", "assign", user_name, f"--tenant={tenant_name}"),
            timeout=timeout,
        )

    def tenant_revoke_user(
        self,
        access_id: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """Revoke tenant access for an access ID."""
        return self.run_cli(self._tenant_cmd("user", "revoke", access_id), timeout=timeout)

    def tenant_assign_admin(
        self,
        access_id: str,
        tenant_name: str,
        *,
        delegated: bool = False,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Promote a tenant access ID to tenant admin and return verbose JSON metadata."""
        cmd = self._tenant_cmd("--verbose", "user", "assignadmin", access_id)
        if delegated:
            cmd.append("--delegated")
        cmd.append(f"--tenant={tenant_name}")
        parsed = self.run_cli(
            cmd,
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from tenant assignadmin {access_id}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    def tenant_revoke_admin(
        self,
        access_id: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Revoke tenant admin privileges and return verbose JSON metadata."""
        parsed = self.run_cli(
            self._tenant_cmd("--verbose", "user", "revokeadmin", access_id),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from tenant revokeadmin {access_id}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    def list_tenant_users(
        self,
        tenant_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> list[JsonDict]:
        """List users assigned to a tenant in JSON form."""
        parsed = self.run_cli(
            self._tenant_cmd("user", "list", "--json", tenant_name),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, list):
            raise OzoneProviderError(
                f"Unexpected JSON payload from tenant user list {tenant_name}: expected list, got {type(parsed).__name__}."
            )
        return cast("list[JsonDict]", parsed)

    def get_tenant_user_info(
        self,
        user_name: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> JsonDict:
        """Return all tenant assignments for a user in JSON form."""
        parsed = self.run_cli(
            self._tenant_cmd("user", "info", "--json", user_name),
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, dict):
            raise OzoneProviderError(
                f"Unexpected JSON payload from tenant user info {user_name}: expected dict, got {type(parsed).__name__}."
            )
        return parsed

    def get_tenant_user_secret(
        self,
        access_id: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """Return the current secret for a tenant access ID without generating a new one."""
        return self.run_cli(self._tenant_cmd("user", "get-secret", access_id), timeout=timeout)

    def set_tenant_user_secret(
        self,
        access_id: str,
        secret_key: str,
        *,
        timeout: int = FAST_TIMEOUT_SECONDS,
    ) -> str:
        """Set the secret key for a tenant access ID."""
        return self.run_cli(
            self._tenant_cmd("user", "set-secret", access_id, "--secret", secret_key),
            timeout=timeout,
        )

    # ==============================
    # Cluster report operations
    # ==============================
    def get_container_report(self, *, timeout: int = SLOW_TIMEOUT_SECONDS) -> JsonDict | list[JsonDict]:
        """Fetch and parse the JSON container report from SCM."""
        parsed = self.run_cli(
            ["ozone", "admin", "container", "report", "--json"],
            timeout=timeout,
            return_json_result=True,
        )
        if not isinstance(parsed, (dict, list)):
            raise OzoneProviderError(
                "Unexpected JSON payload from container report: "
                f"expected dict or list, got {type(parsed).__name__}."
            )
        return cast("JsonDict | list[JsonDict]", parsed)

    # ==============================
    # Shared extra helpers
    # ==============================
    def _tenant_cmd(self, *args: str) -> list[str]:
        """Build an ozone tenant command."""
        return ["ozone", "tenant", *args]

    def _snapshot_cmd(self, action: str, *args: str) -> list[str]:
        """Build an ozone sh snapshot command."""
        return ["ozone", "sh", "snapshot", action, *args]

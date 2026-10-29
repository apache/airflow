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

"""Unified subprocess execution helpers for the Ozone provider."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeAlias, TypeVar, cast

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from typing_extensions import ParamSpec

from airflow.providers.apache.ozone.utils.errors import OzoneCliError, OzoneCliErrors
from airflow.providers.apache.ozone.version_compat import redact

log = logging.getLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")
EnvOverrideValue: TypeAlias = "str | int | float | bool"
EnvOverridesMapping: TypeAlias = "Mapping[str, EnvOverrideValue]"


DEFAULT_PROCESS_TIMEOUT_SECONDS = 300
DEFAULT_RETRY_WAIT = wait_exponential(multiplier=2, min=2, max=60)


@dataclass(frozen=True)
class ProcessOutputAnalysis:
    """Immutable parsed snapshot of a completed process output."""

    NOISE_LINE_PREFIXES = (
        "log4j:",
        "usage:",
        "the general command line syntax is:",
        "generic options supported are:",
    )
    NOISE_LINE_CONTAINS = (
        "please initialize the log4j system properly",
        "see http://logging.apache.org/log4j",
    )

    raw_output: str
    meaningful_lines: tuple[str, ...]
    skipped_lines: tuple[str, ...]
    meaningful_output: str
    preferred_output: str
    normalized_meaningful_output: str
    normalized_preferred_output: str

    @staticmethod
    def pick_process_output(process_result: subprocess.CompletedProcess[str]) -> str:
        """Return output with stdout-first fallback to stderr."""
        return ((process_result.stdout or "").strip()) or ((process_result.stderr or "").strip())

    @classmethod
    def is_noise_output_line(cls, line: str) -> bool:
        """Return True for non-data CLI noise lines."""
        lowered = (line or "").strip().lower()
        if not lowered:
            return True
        if lowered.startswith(cls.NOISE_LINE_PREFIXES):
            return True
        return any(fragment in lowered for fragment in cls.NOISE_LINE_CONTAINS)

    @classmethod
    def extract_meaningful_output_lines(
        cls,
        output: str | None,
        *,
        accept_line: Callable[[str], bool] | None = None,
    ) -> tuple[list[str], list[str]]:
        """
        Split output into (accepted, skipped) lines after noise filtering.

        If ``accept_line`` is not provided, all non-noise lines are accepted.
        """
        if not output:
            return [], []

        accepted: list[str] = []
        skipped: list[str] = []
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if cls.is_noise_output_line(line):
                if line:
                    skipped.append(line)
                continue
            if accept_line is None or accept_line(line):
                accepted.append(line)
            else:
                skipped.append(line)
        return accepted, skipped

    @classmethod
    def from_completed_process(
        cls,
        process_result: subprocess.CompletedProcess[str],
        *,
        accept_line: Callable[[str], bool] | None = None,
    ) -> ProcessOutputAnalysis:
        """Build a normalized analysis snapshot for a completed process."""
        raw_output = "\n".join(part for part in (process_result.stderr, process_result.stdout) if part)
        meaningful_lines, skipped_lines = cls.extract_meaningful_output_lines(
            raw_output,
            accept_line=accept_line,
        )
        meaningful_output = "\n".join(meaningful_lines)
        preferred_output = meaningful_output or cls.pick_process_output(process_result)
        return cls(
            raw_output=raw_output,
            meaningful_lines=tuple(meaningful_lines),
            skipped_lines=tuple(skipped_lines),
            meaningful_output=meaningful_output,
            preferred_output=preferred_output,
            normalized_meaningful_output=meaningful_output.lower(),
            normalized_preferred_output=preferred_output.lower(),
        )


class CliRunner:
    """Subprocess execution helpers grouped by provider use-cases."""

    TERMINATE_GRACE_PERIOD_SECONDS = 5

    @staticmethod
    def merge_env(env_overrides: EnvOverridesMapping | None) -> dict[str, str]:
        """Merge process env with stringified overrides."""
        env = os.environ.copy()
        if env_overrides:
            env.update({str(key): str(value) for key, value in env_overrides.items()})
        return env

    @staticmethod
    def _stream_text(value: str | bytes | None) -> str | None:
        """Return process output as text. ``text=True`` already yields ``str``."""
        if value is None or isinstance(value, str):
            return value
        return value.decode(errors="replace")

    @staticmethod
    def log_streams(
        stdout: str | None,
        stderr: str | None,
        *,
        level: int = logging.DEBUG,
    ) -> None:
        """Log stdout/stderr when they are not empty."""
        if stdout and stdout.strip():
            log.log(level, "stdout: %s", redact(stdout.strip()))
        if stderr and stderr.strip():
            log.log(level, "stderr: %s", redact(stderr.strip()))

    @classmethod
    def run(
        cls,
        command: Sequence[str],
        *,
        env_overrides: EnvOverridesMapping | None = None,
        timeout: int = DEFAULT_PROCESS_TIMEOUT_SECONDS,
        input_text: str | None = None,
        cwd: str | None = None,
        check: bool = True,
        log_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """Run command once without retry."""
        cmd = list(command)
        masked_command = cast("str", redact(shlex.join(cmd)))
        log.debug("Executing command: %s", masked_command)

        process: subprocess.Popen[str] | None = None
        try:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE if input_text is not None else None,
                env=cls.merge_env(env_overrides),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=cwd,
            )
            stdout, stderr = process.communicate(input=input_text, timeout=timeout)
            result = subprocess.CompletedProcess(
                args=cmd,
                returncode=process.returncode if process.returncode is not None else 1,
                stdout=stdout,
                stderr=stderr,
            )
            if check and result.returncode != 0:
                raise subprocess.CalledProcessError(
                    returncode=result.returncode,
                    cmd=cmd,
                    output=result.stdout,
                    stderr=result.stderr,
                )
        except subprocess.CalledProcessError as err:
            cls.log_streams(cls._stream_text(err.stdout), cls._stream_text(err.stderr), level=logging.ERROR)
            log.error("Command failed with return code %s: %s", err.returncode, masked_command)
            raise
        except subprocess.TimeoutExpired as err:
            if process is not None:
                cls.terminate_process(process, masked_command=masked_command)
            cls.log_streams(cls._stream_text(err.stdout), cls._stream_text(err.stderr), level=logging.ERROR)
            log.error("Command timed out after %s seconds: %s", timeout, masked_command)
            raise
        except FileNotFoundError:
            log.error("Executable not found: %s", redact(cmd[0] if cmd else "<empty-command>"))
            raise
        except OSError as err:
            log.error("OS error while running command %s: %s", masked_command, redact(str(err)))
            raise
        except BaseException:
            if process is not None:
                cls.terminate_process(process, masked_command=masked_command)
            raise

        if log_output:
            cls.log_streams(result.stdout, result.stderr)
        return result

    @classmethod
    def terminate_process(cls, process: subprocess.Popen[str], *, masked_command: str) -> None:
        """Gracefully stop process and force-kill if it does not exit."""
        if process.poll() is not None:
            return

        process.terminate()
        try:
            process.wait(timeout=cls.TERMINATE_GRACE_PERIOD_SECONDS)
            log.debug("Terminated process: %s", masked_command)
            return
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=cls.TERMINATE_GRACE_PERIOD_SECONDS)
            except subprocess.TimeoutExpired:
                log.warning("Process did not exit after kill: %s", masked_command)
            log.warning("Force-killed process after graceful timeout: %s", masked_command)

    @classmethod
    def run_streaming(
        cls,
        command: Sequence[str],
        *,
        env: EnvOverridesMapping | None = None,
        cwd: str | None = None,
        check: bool = True,
        on_start: Callable[[subprocess.Popen[str]], None] | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> str:
        """Run command via ``Popen`` and stream combined output line-by-line."""
        cmd = list(command)
        masked_command = redact(shlex.join(cmd))
        log.debug("Executing streaming command: %s", masked_command)

        try:
            sub_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=cwd,
                close_fds=True,
                env=None if env is None else {str(k): str(v) for k, v in env.items()},
                text=True,
            )
        except FileNotFoundError:
            log.error("Executable not found: %s", redact(cmd[0] if cmd else "<empty-command>"))
            raise
        except OSError as err:
            log.error("OS error while running command %s: %s", masked_command, redact(str(err)))
            raise

        if on_start is not None:
            on_start(sub_process)

        if sub_process.stdout is None:
            raise RuntimeError("Subprocess has no stdout")

        stdout_lines: list[str] = []
        for line in sub_process.stdout:
            stdout_lines.append(line)
            if on_output is not None:
                on_output(line.rstrip("\n"))

        sub_process.wait()
        stdout = "".join(stdout_lines)
        if check and sub_process.returncode:
            raise subprocess.CalledProcessError(
                returncode=sub_process.returncode,
                cmd=cmd,
                output=stdout,
                stderr=stdout,
            )
        return stdout

    @classmethod
    def retry_for(
        cls,
        *,
        retry_exceptions: type[BaseException] | tuple[type[BaseException], ...] | None = None,
        retry_condition: Callable[[BaseException], bool] | None = None,
        logger: logging.Logger,
        retry_attempts: int = 3,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Build tenacity retry decorator with provider defaults."""
        if retry_exceptions is None and retry_condition is None:
            raise ValueError("retry_for requires retry_exceptions or retry_condition")

        if retry_condition is not None:
            condition_policy = retry_if_exception(retry_condition)
            retry_policy = (
                retry_if_exception_type(retry_exceptions) & condition_policy
                if retry_exceptions is not None
                else condition_policy
            )
        else:
            if retry_exceptions is None:
                raise ValueError("retry_for requires retry_exceptions or retry_condition")
            retry_policy = retry_if_exception_type(retry_exceptions)

        return retry(
            wait=DEFAULT_RETRY_WAIT,
            stop=stop_after_attempt(retry_attempts),
            retry=retry_policy,
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    @classmethod
    def run_process(
        cls,
        command: Sequence[str],
        *,
        retry_exceptions: type[BaseException] | tuple[type[BaseException], ...] = (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ),
        retry_condition: Callable[[BaseException], bool] | None = None,
        env_overrides: EnvOverridesMapping | None = None,
        timeout: int = DEFAULT_PROCESS_TIMEOUT_SECONDS,
        input_text: str | None = None,
        cwd: str | None = None,
        retry_attempts: int = 3,
        check: bool = True,
        log_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """Run generic process command with retry."""
        retry_decorator = cls.retry_for(
            retry_exceptions=retry_exceptions,
            retry_condition=retry_condition,
            retry_attempts=retry_attempts,
            logger=log,
        )

        @retry_decorator
        def _execute() -> subprocess.CompletedProcess[str]:
            return cls.run(
                command,
                env_overrides=env_overrides,
                timeout=timeout,
                input_text=input_text,
                cwd=cwd,
                check=check,
                log_output=log_output,
            )

        return _execute()


class OzoneCliRunner(CliRunner):
    """Ozone CLI specific process helpers and retry policy."""

    @staticmethod
    def _is_retryable_ozone_cli_exception(exc: BaseException) -> bool:
        """Return True when Ozone CLI error should be retried."""
        return isinstance(exc, OzoneCliError) and exc.retryable

    @staticmethod
    def _as_ozone_cli_error(
        err: BaseException,
        *,
        command: Sequence[str],
        timeout: int,
    ) -> OzoneCliError:
        """Convert low-level process errors to OzoneCliError."""
        cmd = list(command)
        if isinstance(err, FileNotFoundError):
            return OzoneCliError(
                "Ozone CLI not found. Please ensure Ozone is installed and available in PATH.",
                command=cmd,
                returncode=None,
                retryable=False,
            )
        if isinstance(err, subprocess.TimeoutExpired):
            error_message = (
                CliRunner._stream_text(err.stderr)
                or CliRunner._stream_text(err.stdout)
                or "Ozone command timed out"
            ).strip()
            return OzoneCliError(
                f"Ozone command timed out after {timeout} seconds: {redact(error_message)}",
                command=cmd,
                stderr=error_message,
                returncode=None,
                retryable=True,
            )
        if isinstance(err, subprocess.CalledProcessError):
            error_message = (err.stderr or err.stdout or "No error message provided").strip()
            return_code = err.returncode
            is_retryable = OzoneCliErrors.is_retryable_failure(return_code, error_message)
            error_kind = "retryable" if is_retryable else "non-retryable"
            return OzoneCliError(
                f"Ozone command failed ({error_kind}, return code: {return_code}): {redact(error_message)}",
                command=cmd,
                stderr=error_message,
                returncode=return_code,
                retryable=is_retryable,
            )
        if isinstance(err, OSError):
            return OzoneCliError(
                f"Ozone command execution failed: {redact(str(err))}",
                command=cmd,
                stderr=str(err),
                returncode=None,
                retryable=False,
            )
        return OzoneCliError(
            f"Ozone command execution failed: {redact(str(err))}",
            command=cmd,
            stderr=str(err),
            returncode=None,
            retryable=False,
        )

    @classmethod
    def run_ozone_once(
        cls,
        command: Sequence[str],
        *,
        env_overrides: EnvOverridesMapping | None = None,
        timeout: int = DEFAULT_PROCESS_TIMEOUT_SECONDS,
        input_text: str | None = None,
        cwd: str | None = None,
        check: bool = True,
        log_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """Run Ozone CLI command once without retry."""
        start = time.monotonic()
        try:
            result = cls.run(
                command,
                env_overrides=env_overrides,
                timeout=timeout,
                input_text=input_text,
                cwd=cwd,
                check=check,
                log_output=log_output,
            )
            elapsed = time.monotonic() - start
            log.debug(
                "Ozone command finished in %.2f seconds: %s", elapsed, redact(shlex.join(list(command)))
            )
            return result
        except (FileNotFoundError, subprocess.TimeoutExpired, subprocess.CalledProcessError, OSError) as err:
            elapsed = time.monotonic() - start
            log.debug("Ozone command failed in %.2f seconds: %s", elapsed, redact(shlex.join(list(command))))
            raise cls._as_ozone_cli_error(err, command=command, timeout=timeout) from err

    @classmethod
    def run_ozone(
        cls,
        command: Sequence[str],
        *,
        env_overrides: EnvOverridesMapping | None = None,
        timeout: int = DEFAULT_PROCESS_TIMEOUT_SECONDS,
        input_text: str | None = None,
        cwd: str | None = None,
        retry_attempts: int = 3,
        check: bool = True,
        log_output: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        """Run Ozone CLI command with retry policy and CLI-aware retry condition."""
        effective_retry_attempts = max(1, retry_attempts)

        retry_decorator = cls.retry_for(
            retry_exceptions=OzoneCliError,
            retry_condition=cls._is_retryable_ozone_cli_exception,
            logger=log,
            retry_attempts=effective_retry_attempts,
        )

        @retry_decorator
        def _execute() -> subprocess.CompletedProcess[str]:
            return cls.run_ozone_once(
                command,
                env_overrides=env_overrides,
                timeout=timeout,
                input_text=input_text,
                cwd=cwd,
                check=check,
                log_output=log_output,
            )

        return _execute()


class KerberosCliRunner(CliRunner):
    """Kerberos command execution helper."""

    @classmethod
    def run_kerberos(
        cls,
        command: Sequence[str],
        *,
        env_overrides: EnvOverridesMapping | None = None,
        timeout: int = DEFAULT_PROCESS_TIMEOUT_SECONDS,
        input_text: str | None = None,
        cwd: str | None = None,
        log_output: bool = False,
    ) -> bool:
        """Run command and return ``True`` on success, ``False`` on failure."""
        try:
            cls.run(
                command,
                env_overrides=env_overrides,
                timeout=timeout,
                input_text=input_text,
                cwd=cwd,
                check=True,
                log_output=log_output,
            )
            return True
        except subprocess.CalledProcessError as err:
            log.error("Kerberos command failed with return code %s", err.returncode)
            cls.log_streams(cls._stream_text(err.stdout), cls._stream_text(err.stderr), level=logging.ERROR)
            return False
        except subprocess.TimeoutExpired as err:
            log.error("Kerberos command timed out after %s seconds", timeout)
            cls.log_streams(cls._stream_text(err.stdout), cls._stream_text(err.stderr), level=logging.ERROR)
            return False
        except FileNotFoundError:
            log.error("Kerberos executable not found")
            return False
        except OSError as err:
            log.error("Kerberos command OS error: %s", redact(str(err)))
            return False

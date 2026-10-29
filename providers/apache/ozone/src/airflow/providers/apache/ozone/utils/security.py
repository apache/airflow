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

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from airflow.configuration import ensure_secrets_loaded
from airflow.providers.apache.ozone.utils.cli_runner import KerberosCliRunner
from airflow.providers.apache.ozone.utils.connection_schema import (
    OzoneConnSnapshot,
)
from airflow.providers.apache.ozone.utils.errors import OzoneProviderError
from airflow.providers.apache.ozone.utils.helpers import FileHelper
from airflow.providers.apache.ozone.version_compat import AirflowException, mask_secret

log = logging.getLogger(__name__)


def _normalize_and_validate_scope(scope: str) -> str:
    """Normalize security scope and validate supported values."""
    normalized = str(scope).strip().lower()
    if normalized not in {"ozone", "hdfs", "all"}:
        raise ValueError("scope must be one of: 'ozone', 'hdfs', 'all'")
    return normalized


class SecretResolver:
    """Resolve `secret://` references via Airflow Secrets Backend."""

    @classmethod
    def get_secret_value(
        cls, value: str | None, conn_id: str | None = None, *, mask: bool = True
    ) -> str | None:
        """
        Resolve secret from Airflow Secrets Backend or return value as-is.

        If ``value`` starts with ``secret://``, each configured backend is queried
        via ``get_config(value)`` and the first non-None value is returned.
        """
        if not value:
            return value

        if not value.startswith("secret://"):
            if mask:
                mask_secret(value)
            return value

        for backend in ensure_secrets_loaded():
            try:
                secret_value = backend.get_config(value)
            except (AttributeError, TypeError) as err:
                log.error("Secrets Backend returned invalid response for %s: %s", value, str(err))
                raise ValueError(f"Failed to retrieve secret '{value}': {err}") from err

            if secret_value is not None:
                resolved_value = str(secret_value)
                if conn_id:
                    log.debug(
                        "Resolved secret from Secrets Backend for connection %s: %s",
                        conn_id,
                        value.split("/")[-1] if "/" in value else value,
                    )
                if mask:
                    mask_secret(resolved_value)
                return resolved_value
        raise ValueError(f"Secret not found: {value}")


@dataclass(frozen=True)
class SSLConfig:
    """Structured SSL/TLS configuration split by subsystem."""

    ozone: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)
    conn_id: str | None = None

    def as_env(self) -> dict[str, str]:
        """Merge all subsystem env vars into a single mapping."""
        merged: dict[str, str] = {}
        merged.update(self.ozone)
        merged.update(self.hdfs)
        return merged

    @classmethod
    def from_snapshot(
        cls, snapshot: OzoneConnSnapshot, *, conn_id: str | None = None, scope: str
    ) -> SSLConfig:
        """Build SSL configuration from typed snapshot by explicit scope."""
        normalized_scope = _normalize_and_validate_scope(scope)
        return cls(
            ozone=cls._build_ozone_env(snapshot, conn_id) if normalized_scope in {"ozone", "all"} else {},
            hdfs=cls._build_hdfs_env(snapshot, conn_id) if normalized_scope in {"hdfs", "all"} else {},
            conn_id=conn_id,
        )

    @classmethod
    def _build_ozone_env(cls, snapshot: OzoneConnSnapshot, conn_id: str | None) -> dict[str, str]:
        """Build SSL env vars for Ozone Native CLI (ozone-site.xml mapping)."""
        env: dict[str, str] = {}
        if snapshot.ozone_security_enabled:
            env["OZONE_SECURITY_ENABLED"] = "true"
            for field_name, env_key, is_secret in OzoneConnSnapshot.OZONE_SSL_ENV_MAPPING:
                value = getattr(snapshot, field_name)
                if value is None:
                    continue
                if is_secret:
                    value = SecretResolver.get_secret_value(str(value), conn_id=conn_id)
                env[env_key] = str(value)
        return env

    @classmethod
    def _build_hdfs_env(cls, snapshot: OzoneConnSnapshot, conn_id: str | None) -> dict[str, str]:
        """Build SSL env vars for HDFS clients (core-site.xml / hdfs-site.xml mapping)."""
        env: dict[str, str] = {}
        if snapshot.hdfs_ssl_enabled:
            env["HDFS_SSL_ENABLED"] = "true"
            for field_name, env_key, is_secret in OzoneConnSnapshot.HDFS_SSL_ENV_MAPPING:
                value = getattr(snapshot, field_name)
                if value is None:
                    continue
                if is_secret:
                    value = SecretResolver.get_secret_value(str(value), conn_id=conn_id)
                env[env_key] = str(value)
        return env

    @staticmethod
    def apply_ssl_env_vars(
        env_vars: dict[str, str], existing_env: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Apply SSL environment variables to existing environment."""
        if existing_env is None:
            return env_vars.copy()
        env = existing_env.copy()
        env.update(env_vars)
        return env


@dataclass(frozen=True)
class KerberosConfig:
    """Structured Kerberos configuration split by subsystem."""

    core: dict[str, str] = field(default_factory=dict)
    hdfs: dict[str, str] = field(default_factory=dict)

    def as_env(self) -> dict[str, str]:
        merged: dict[str, str] = {}
        merged.update(self.core)
        merged.update(self.hdfs)
        return merged

    @staticmethod
    def _hdfs_principal_keytab_from_snapshot(
        snapshot: OzoneConnSnapshot,
        conn_id: str | None,
    ) -> None | tuple[str, str]:
        """Return HDFS principal and resolved keytab from snapshot when configured."""
        principal_value = snapshot.hdfs_kerberos_principal
        keytab_value = snapshot.hdfs_kerberos_keytab
        if principal_value is None or keytab_value is None:
            return None
        return (
            str(principal_value),
            str(SecretResolver.get_secret_value(str(keytab_value), conn_id=conn_id)),
        )

    @staticmethod
    def _hdfs_principal_password_from_snapshot(
        snapshot: OzoneConnSnapshot,
        conn_id: str | None,
    ) -> None | tuple[str, str]:
        """Return HDFS principal and resolved password from snapshot when configured."""
        principal_value = snapshot.hdfs_kerberos_principal
        password_value = snapshot.hdfs_kerberos_password
        if principal_value is None or password_value is None:
            return None
        return (
            str(principal_value),
            str(SecretResolver.get_secret_value(str(password_value), conn_id=conn_id)),
        )

    @classmethod
    def _get_core_env(cls, snapshot: OzoneConnSnapshot, conn_id: str | None) -> dict[str, str]:
        """Kerberos env for Ozone/Hadoop core tools."""
        env_vars: dict[str, str] = {}

        if str(snapshot.hadoop_security_authentication or "").lower() == "kerberos":
            env_vars["HADOOP_SECURITY_AUTHENTICATION"] = "kerberos"
            for field_name, env_key, is_secret in OzoneConnSnapshot.OZONE_KERBEROS_ENV_MAPPING:
                value = getattr(snapshot, field_name)
                if value is None:
                    continue
                if is_secret:
                    value = SecretResolver.get_secret_value(str(value), conn_id=conn_id)
                env_vars[env_key] = str(value)

        return env_vars

    @classmethod
    def _get_hdfs_env(cls, snapshot: OzoneConnSnapshot, conn_id: str | None) -> dict[str, str]:
        """Build HDFS Kerberos subprocess env without exporting passwords."""
        env_vars: dict[str, str] = {}

        if snapshot.hdfs_kerberos_enabled:
            if snapshot.hdfs_kerberos_principal:
                env_vars["HDFS_KERBEROS_PRINCIPAL"] = str(snapshot.hdfs_kerberos_principal)
            if snapshot.hdfs_kerberos_keytab:
                env_vars["HDFS_KERBEROS_KEYTAB"] = str(
                    SecretResolver.get_secret_value(str(snapshot.hdfs_kerberos_keytab), conn_id=conn_id)
                )

        return env_vars

    @classmethod
    def get_env_vars(
        cls,
        snapshot: OzoneConnSnapshot,
        *,
        scope: str,
        conn_id: str | None = None,
    ) -> dict[str, str]:
        """Extract Kerberos environment variables from snapshot by explicit scope."""
        normalized_scope = _normalize_and_validate_scope(scope)
        config = cls(
            core=cls._get_core_env(snapshot, conn_id) if normalized_scope in {"ozone", "all"} else {},
            hdfs=cls._get_hdfs_env(snapshot, conn_id) if normalized_scope in {"hdfs", "all"} else {},
        )
        return config.as_env()

    @classmethod
    def kinit_with_keytab(
        cls,
        principal: str,
        keytab: str,
        krb5_conf: str | None = None,
        *,
        snapshot: OzoneConnSnapshot,
    ) -> bool:
        """
        Perform Kerberos authentication using keytab file.

        Runs system ``kinit -kt keytab principal`` with optional KRB5_CONFIG.
        """
        if not principal or not keytab:
            log.warning("Kerberos principal or keytab not provided, skipping kinit")
            return False

        if not FileHelper.is_readable_file(keytab):
            log.error("Keytab file is missing or not readable: %s", keytab)
            return False

        cmd = ["kinit", "-kt", keytab, principal]
        env_overrides: dict[str, str] = {}
        if krb5_conf:
            if not FileHelper.is_readable_file(krb5_conf):
                log.error("KRB5 config file is missing or not readable: %s", krb5_conf)
                return False
            env_overrides["KRB5_CONFIG"] = krb5_conf

        if KerberosCliRunner.run_kerberos(
            cmd,
            env_overrides=env_overrides or None,
            timeout=snapshot.kinit_timeout_seconds,
        ):
            log.info("Successfully authenticated with Kerberos: %s", principal)
            return True
        return False

    @classmethod
    def kinit_with_password(
        cls,
        principal: str,
        password: str,
        krb5_conf: str | None = None,
        *,
        snapshot: OzoneConnSnapshot,
    ) -> bool:
        """
        Perform Kerberos authentication using a password.

        Runs system ``kinit principal`` and supplies the password through stdin.
        """
        if not principal or not password:
            log.warning("Kerberos principal or password not provided, skipping kinit")
            return False

        mask_secret(password)
        env_overrides: dict[str, str] = {}
        if krb5_conf:
            if not FileHelper.is_readable_file(krb5_conf):
                log.error("KRB5 config file is missing or not readable: %s", krb5_conf)
                return False
            env_overrides["KRB5_CONFIG"] = krb5_conf

        if KerberosCliRunner.run_kerberos(
            ["kinit", principal],
            env_overrides=env_overrides or None,
            timeout=snapshot.kinit_timeout_seconds,
            input_text=f"{password}\n",
        ):
            log.info("Successfully authenticated with Kerberos: %s", principal)
            return True
        return False

    @classmethod
    def kinit_from_snapshot(
        cls,
        *,
        snapshot: OzoneConnSnapshot,
        conn_id: str | None = None,
    ) -> bool:
        """Run kinit using credentials defined in the connection snapshot."""
        principal = snapshot.kerberos_principal
        if not principal:
            return False

        krb5_conf = snapshot.krb5_conf
        if snapshot.kerberos_keytab:
            keytab = SecretResolver.get_secret_value(str(snapshot.kerberos_keytab), conn_id=conn_id)
            return cls.kinit_with_keytab(principal, str(keytab), krb5_conf, snapshot=snapshot)

        if snapshot.kerberos_password:
            password = SecretResolver.get_secret_value(str(snapshot.kerberos_password), conn_id=conn_id)
            return cls.kinit_with_password(principal, str(password), krb5_conf, snapshot=snapshot)

        return False

    @classmethod
    def kinit_hdfs_from_snapshot(
        cls,
        *,
        snapshot: OzoneConnSnapshot,
        conn_id: str | None = None,
    ) -> bool:
        """Run HDFS kinit using keytab or password credentials from snapshot."""
        if not snapshot.hdfs_kerberos_enabled:
            return False

        pair = cls._hdfs_principal_keytab_from_snapshot(snapshot, conn_id)
        if pair:
            principal, keytab = pair
            return cls.kinit_with_keytab(principal, keytab, snapshot.krb5_conf, snapshot=snapshot)

        password_pair = cls._hdfs_principal_password_from_snapshot(snapshot, conn_id)
        if password_pair:
            principal, password = password_pair
            return cls.kinit_with_password(principal, password, snapshot.krb5_conf, snapshot=snapshot)

        return False

    @staticmethod
    def apply_env_vars(
        env_vars: dict[str, str], existing_env: dict[str, str] | None = None
    ) -> dict[str, str]:
        """Build Kerberos-related environment overrides for a subprocess."""
        base_env = (existing_env if existing_env is not None else os.environ).copy()
        overrides: dict[str, str] = env_vars.copy()

        enabled = (
            overrides.get(
                "HADOOP_SECURITY_AUTHENTICATION", base_env.get("HADOOP_SECURITY_AUTHENTICATION", "")
            ).lower()
            == "kerberos"
        )
        if not enabled:
            return overrides

        hadoop_opts = overrides.get("HADOOP_OPTS") or base_env.get("HADOOP_OPTS", "")
        hadoop_flag = "-Dhadoop.security.authentication=kerberos"
        if hadoop_flag not in hadoop_opts:
            hadoop_opts = (hadoop_opts + " " + hadoop_flag).strip()
        overrides["HADOOP_OPTS"] = hadoop_opts

        ozone_opts = overrides.get("OZONE_OPTS") or base_env.get("OZONE_OPTS", "")
        ozone_flags = [
            "-Dhadoop.security.authentication=kerberos",
            "-Dozone.security.enabled=true",
        ]
        for flag in ozone_flags:
            if flag not in ozone_opts:
                ozone_opts = (ozone_opts + " " + flag).strip()
        overrides["OZONE_OPTS"] = ozone_opts

        ozone_conf_dir = overrides.get("OZONE_CONF_DIR")
        if ozone_conf_dir:
            overrides["HADOOP_CONF_DIR"] = ozone_conf_dir
        else:
            base_ozone_conf_dir = base_env.get("OZONE_CONF_DIR")
            if base_ozone_conf_dir:
                overrides["OZONE_CONF_DIR"] = base_ozone_conf_dir
                overrides["HADOOP_CONF_DIR"] = base_ozone_conf_dir
        return overrides

    @classmethod
    def load_ozone_env(
        cls,
        *,
        snapshot: OzoneConnSnapshot,
        conn_id: str,
    ) -> dict[str, str] | None:
        """Build Kerberos env for Ozone CLI with unified logging and errors."""
        try:
            kerberos_env_vars = cls.get_env_vars(snapshot, scope="ozone", conn_id=conn_id)
            if kerberos_env_vars:
                kerberos_env = cls.apply_env_vars(kerberos_env_vars)
                log.debug("Kerberos configuration loaded from connection: %s", list(kerberos_env_vars.keys()))
                if str(snapshot.hadoop_security_authentication or "").lower() == "kerberos":
                    log.debug("Kerberos authentication enabled for Ozone Native CLI")
                return kerberos_env
            log.debug("No Kerberos configuration found in connection Extra")
            return None
        except AirflowException as err:
            log.debug("Could not load Kerberos configuration (connection may not exist): %s", str(err))
            return None
        except ValueError as err:
            raise OzoneProviderError(
                f"Invalid Kerberos configuration in connection '{conn_id}': {err}"
            ) from err

    @classmethod
    def load_hdfs_env(
        cls,
        *,
        snapshot: OzoneConnSnapshot,
        conn_id: str,
    ) -> dict[str, str] | None:
        """Build HDFS Kerberos subprocess env with unified logging and errors."""
        try:
            kerberos_env_vars = cls.get_env_vars(snapshot, scope="hdfs", conn_id=conn_id)
            if kerberos_env_vars:
                log.debug(
                    "HDFS Kerberos configuration loaded from connection: %s",
                    list(kerberos_env_vars.keys()),
                )
                return kerberos_env_vars
            log.debug("No HDFS Kerberos configuration found in connection Extra")
            return None
        except AirflowException as err:
            log.debug("Could not load HDFS Kerberos configuration (connection may not exist): %s", str(err))
            return None
        except ValueError as err:
            raise OzoneProviderError(
                f"Invalid HDFS Kerberos configuration in connection '{conn_id}': {err}"
            ) from err

    @classmethod
    def ensure_ticket(
        cls,
        *,
        snapshot: OzoneConnSnapshot,
        conn_id: str,
        kerberos_ticket_ready: bool,
    ) -> bool:
        """Ensure Kerberos ticket is initialized; returns updated readiness flag."""
        if kerberos_ticket_ready and cls.has_valid_ticket(snapshot=snapshot):
            return True
        if kerberos_ticket_ready:
            log.warning(
                "Cached Kerberos ticket flag is set but no valid ticket found; re-initializing for connection '%s'",
                conn_id,
            )
        if not snapshot.kerberos_enabled:
            return False

        principal = snapshot.kerberos_principal
        keytab_configured = bool(snapshot.kerberos_keytab)
        password_configured = bool(snapshot.kerberos_password)
        if not principal:
            if snapshot.kerberos_enabled:
                raise OzoneProviderError(
                    f"Kerberos authentication is enabled for connection '{conn_id}' "
                    "but 'kerberos_principal' is missing."
                )
            return False
        if not keytab_configured and not password_configured:
            if snapshot.kerberos_enabled:
                raise OzoneProviderError(
                    f"Kerberos authentication is enabled for connection '{conn_id}' "
                    "but neither 'kerberos_keytab' nor 'kerberos_password' is configured."
                )
            return False

        try:
            authenticated = cls.kinit_from_snapshot(
                snapshot=snapshot,
                conn_id=conn_id,
            )
        except ValueError as err:
            raise OzoneProviderError(
                f"Invalid Kerberos configuration in connection '{conn_id}': {err}"
            ) from err
        if not authenticated:
            raise OzoneProviderError(
                f"Kerberos authentication failed for connection '{conn_id}' using principal '{principal}'."
            )
        log.debug("Kerberos ticket is ready for connection '%s'", conn_id)
        return True

    @staticmethod
    def is_enabled(kerberos_env: dict[str, str] | None) -> bool:
        """Return True if Kerberos is effectively enabled."""
        if not kerberos_env:
            return False
        return str(kerberos_env.get("HADOOP_SECURITY_AUTHENTICATION", "")).lower() == "kerberos"

    @staticmethod
    def resolve_config_dir(kerberos_env: dict[str, str] | None) -> str | None:
        """Return effective config dir for Kerberos-enabled Ozone CLI."""
        if not kerberos_env:
            return None
        return kerberos_env.get("OZONE_CONF_DIR")

    @classmethod
    def has_valid_ticket(cls, *, snapshot: OzoneConnSnapshot) -> bool:
        """Return True when current default Kerberos ticket cache has non-expired credentials."""
        return KerberosCliRunner.run_kerberos(
            ["klist", "-s"],
            timeout=snapshot.kinit_timeout_seconds,
            log_output=False,
        )

    @staticmethod
    def check_config_files_exist(
        config_dir: str | None,
        *,
        snapshot: OzoneConnSnapshot,
    ) -> bool:
        """Return True when both core-site.xml and ozone-site.xml exist."""
        if not config_dir:
            log.debug("Config directory is None or empty")
            return False

        config_path = Path(config_dir)
        core_site = config_path / snapshot.core_site_xml
        ozone_site = config_path / snapshot.ozone_site_xml
        core_exists = FileHelper.is_readable_file(core_site)
        ozone_exists = FileHelper.is_readable_file(ozone_site)
        if not core_exists:
            log.debug("core-site.xml not found or not readable: %s", core_site)
        if not ozone_exists:
            log.debug("ozone-site.xml not found or not readable: %s", ozone_site)
        if core_exists and ozone_exists:
            log.debug("Both configuration files found in %s", config_dir)
        return core_exists and ozone_exists

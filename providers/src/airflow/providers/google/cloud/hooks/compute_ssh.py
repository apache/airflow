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

import random
import shlex
import time
from functools import cached_property
from io import StringIO
from typing import Any

from googleapiclient.errors import HttpError
from paramiko.ssh_exception import SSHException

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.hooks.os_login import OSLoginHook
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.types import NOTSET, ArgNotSet

# Paramiko should be imported after airflow.providers.ssh. Then the import will fail with
# cannot import "airflow.providers.ssh" and will be correctly discovered as optional feature
# TODO:(potiuk) We should add test harness detecting such cases shortly
import paramiko  # isort:skip

CMD_TIMEOUT = 10


class _GCloudAuthorizedSSHClient(paramiko.SSHClient):
    """SSH Client that maintains the context for gcloud authorization during the connection."""

    def __init__(self, google_hook, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ssh_client = paramiko.SSHClient()
        self.google_hook = google_hook
        self.decorator = None

    def connect(self, *args, **kwargs):
        self.decorator = self.google_hook.provide_authorized_gcloud()
        self.decorator.__enter__()
        return super().connect(*args, **kwargs)

    def close(self):
        if self.decorator:
            self.decorator.__exit__(None, None, None)
        self.decorator = None
        return super().close()

    def __exit__(self, type_, value, traceback):
        if self.decorator:
            self.decorator.__exit__(type_, value, traceback)
        self.decorator = None
        return super().__exit__(type_, value, traceback)


class ComputeEngineSSHHook(SSHHook):
    """
    Hook to connect to a remote instance in compute engine.

    :param instance_name: The name of the Compute Engine instance
    :param zone: The zone of the Compute Engine instance
    :param user: The name of the user on which the login attempt will be made
    :param project_id: The project ID of the remote instance
    :param gcp_conn_id: The connection id to use when fetching connection info
    :param hostname: The hostname of the target instance. If it is not passed, it will be detected
        automatically.
    :param use_iap_tunnel: Whether to connect through IAP tunnel
    :param use_internal_ip: Whether to connect using internal IP
    :param use_oslogin: Whether to manage keys using OsLogin API. If false,
        keys are managed using instance metadata
    :param expire_time: The maximum amount of time in seconds before the private key expires
    :param gcp_conn_id: The connection id to use when fetching connection information
    :param max_retries: Maximum number of retries the process will try to establish connection to instance.
        Could be decreased/increased by user based on the amount of parallel SSH connections to the instance.
    :param impersonation_chain: Optional. The service account email to impersonate using short-term
        credentials. The provided service account must grant the originating account
        the Service Account Token Creator IAM role and have the sufficient rights to perform the request
    """

    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_ssh_default"
    conn_type = "gcpssh"
    hook_name = "Google Cloud SSH"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        instance_name: str | None = None,
        zone: str | None = None,
        user: str | None = "root",
        project_id: str = PROVIDE_PROJECT_ID,
        hostname: str | None = None,
        use_internal_ip: bool = False,
        use_iap_tunnel: bool = False,
        use_oslogin: bool = True,
        expire_time: int = 300,
        cmd_timeout: int | ArgNotSet = NOTSET,
        max_retries: int = 10,
        impersonation_chain: str | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonation_chain`"
            )
        # Ignore original constructor
        # super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.instance_name = instance_name
        self.zone = zone
        self.user = user
        self.project_id = project_id
        self.hostname = hostname
        self.use_internal_ip = use_internal_ip
        self.use_iap_tunnel = use_iap_tunnel
        self.use_oslogin = use_oslogin
        self.expire_time = expire_time
        self.cmd_timeout = cmd_timeout
        self.max_retries = max_retries
        self.impersonation_chain = impersonation_chain
        self._conn: Any | None = None

    @cached_property
    def _oslogin_hook(self) -> OSLoginHook:
        return OSLoginHook(gcp_conn_id=self.gcp_conn_id)

    @cached_property
    def _compute_hook(self) -> ComputeEngineHook:
        if self.impersonation_chain:
            return ComputeEngineHook(
                gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
            )
        else:
            return ComputeEngineHook(gcp_conn_id=self.gcp_conn_id)

    def _load_connection_config(self):
        def _boolify(value):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                if value.lower() == "false":
                    return False
                elif value.lower() == "true":
                    return True
            return False

        def intify(key, value, default):
            if value is None:
                return default
            if isinstance(value, str) and value.strip() == "":
                return default
            try:
                return int(value)
            except ValueError:
                raise AirflowException(
                    f"The {key} field should be a integer. "
                    f'Current value: "{value}" (type: {type(value)}). '
                    f"Please check the connection configuration."
                )

        conn = self.get_connection(self.gcp_conn_id)
        if conn and conn.conn_type == "gcpssh":
            self.instance_name = self._compute_hook._get_field(
                "instance_name", self.instance_name
            )
            self.zone = self._compute_hook._get_field("zone", self.zone)
            self.user = conn.login if conn.login else self.user
            # self.project_id is skipped intentionally
            self.hostname = conn.host if conn.host else self.hostname
            self.use_internal_ip = _boolify(
                self._compute_hook._get_field("use_internal_ip")
            )
            self.use_iap_tunnel = _boolify(
                self._compute_hook._get_field("use_iap_tunnel")
            )
            self.use_oslogin = _boolify(self._compute_hook._get_field("use_oslogin"))
            self.expire_time = intify(
                "expire_time",
                self._compute_hook._get_field("expire_time"),
                self.expire_time,
            )

            if conn.extra is not None:
                extra_options = conn.extra_dejson
                if "cmd_timeout" in extra_options and self.cmd_timeout is NOTSET:
                    if extra_options["cmd_timeout"]:
                        self.cmd_timeout = int(extra_options["cmd_timeout"])
                    else:
                        self.cmd_timeout = None

            if self.cmd_timeout is NOTSET:
                self.cmd_timeout = CMD_TIMEOUT

    def get_conn(self) -> paramiko.SSHClient:
        """Return SSH connection."""
        self._load_connection_config()
        if not self.project_id:
            self.project_id = self._compute_hook.project_id

        missing_fields = [
            k for k in ["instance_name", "zone", "project_id"] if not getattr(self, k)
        ]
        if not self.instance_name or not self.zone or not self.project_id:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters be passed either as "
                "keyword parameter or as extra field in Airflow connection definition. Both are not set!"
            )

        self.log.info(
            "Connecting to instance: instance_name=%s, user=%s, zone=%s, "
            "use_internal_ip=%s, use_iap_tunnel=%s, use_os_login=%s",
            self.instance_name,
            self.user,
            self.zone,
            self.use_internal_ip,
            self.use_iap_tunnel,
            self.use_oslogin,
        )
        if not self.hostname:
            hostname = self._compute_hook.get_instance_address(
                zone=self.zone,
                resource_id=self.instance_name,
                project_id=self.project_id,
                use_internal_ip=self.use_internal_ip or self.use_iap_tunnel,
            )
        else:
            hostname = self.hostname

        privkey, pubkey = self._generate_ssh_key(self.user)

        max_delay = 10
        sshclient = None
        for retry in range(self.max_retries + 1):
            try:
                if self.use_oslogin:
                    user = self._authorize_os_login(pubkey)
                else:
                    user = self.user
                    self._authorize_compute_engine_instance_metadata(pubkey)
                proxy_command = None
                if self.use_iap_tunnel:
                    proxy_command_args = [
                        "gcloud",
                        "compute",
                        "start-iap-tunnel",
                        str(self.instance_name),
                        "22",
                        "--listen-on-stdin",
                        f"--project={self.project_id}",
                        f"--zone={self.zone}",
                        "--verbosity=warning",
                    ]
                    if self.impersonation_chain:
                        proxy_command_args.append(
                            f"--impersonate-service-account={self.impersonation_chain}"
                        )
                    proxy_command = " ".join(
                        shlex.quote(arg) for arg in proxy_command_args
                    )
                sshclient = self._connect_to_instance(
                    user, hostname, privkey, proxy_command
                )
                break
            except (HttpError, AirflowException, SSHException) as exc:
                if (isinstance(exc, HttpError) and exc.resp.status == 412) or (
                    isinstance(exc, AirflowException)
                    and "412 PRECONDITION FAILED" in str(exc)
                ):
                    self.log.info(
                        "Error occurred when trying to update instance metadata: %s", exc
                    )
                elif isinstance(exc, SSHException):
                    self.log.info(
                        "Error occurred when establishing SSH connection using Paramiko: %s",
                        exc,
                    )
                else:
                    raise
                if retry == self.max_retries:
                    raise AirflowException(
                        "Maximum retries exceeded. Aborting operation."
                    )
                delay = random.randint(0, max_delay)
                self.log.info(
                    "Failed establish SSH connection, waiting %s seconds to retry...",
                    delay,
                )
                time.sleep(delay)
        if not sshclient:
            raise AirflowException("Unable to establish SSH connection.")
        return sshclient

    def _connect_to_instance(
        self, user, hostname, pkey, proxy_command
    ) -> paramiko.SSHClient:
        self.log.info(
            "Opening remote connection to host: username=%s, hostname=%s", user, hostname
        )
        max_time_to_wait = 5
        for time_to_wait in range(max_time_to_wait + 1):
            try:
                client = _GCloudAuthorizedSSHClient(self._compute_hook)
                # Default is RejectPolicy
                # No known host checking since we are not storing privatekey
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # nosec B507
                client.connect(
                    hostname=hostname,
                    username=user,
                    pkey=pkey,
                    sock=paramiko.ProxyCommand(proxy_command) if proxy_command else None,
                    look_for_keys=False,
                )
                return client
            except paramiko.SSHException:
                if time_to_wait == max_time_to_wait:
                    raise
            self.log.info("Failed to connect. Waiting %ds to retry", time_to_wait)
            time.sleep(time_to_wait)
        raise AirflowException("Can not connect to instance")

    def _authorize_compute_engine_instance_metadata(self, pubkey):
        self.log.info("Appending SSH public key to instance metadata")
        instance_info = self._compute_hook.get_instance_info(
            zone=self.zone, resource_id=self.instance_name, project_id=self.project_id
        )

        keys = self.user + ":" + pubkey + "\n"
        metadata = instance_info["metadata"]
        items = metadata.get("items", [])
        for item in items:
            if item.get("key") == "ssh-keys":
                keys += item["value"]
                item["value"] = keys
                break
        else:
            new_dict = {"key": "ssh-keys", "value": keys}
            metadata["items"] = [*items, new_dict]

        self._compute_hook.set_instance_metadata(
            zone=self.zone,
            resource_id=self.instance_name,
            metadata=metadata,
            project_id=self.project_id,
        )

    def _authorize_os_login(self, pubkey):
        username = self._oslogin_hook._get_credentials_email
        self.log.info("Importing SSH public key using OSLogin: user=%s", username)
        expiration = int((time.time() + self.expire_time) * 1000000)
        ssh_public_key = {"key": pubkey, "expiration_time_usec": expiration}
        response = self._oslogin_hook.import_ssh_public_key(
            user=username, ssh_public_key=ssh_public_key, project_id=self.project_id
        )
        profile = response.login_profile
        account = profile.posix_accounts[0]
        user = account.username
        return user

    def _generate_ssh_key(self, user):
        try:
            self.log.info("Generating ssh keys...")
            pkey_file = StringIO()
            pkey_obj = paramiko.RSAKey.generate(2048)
            pkey_obj.write_private_key(pkey_file)
            pubkey = f"{pkey_obj.get_name()} {pkey_obj.get_base64()} {user}"
            return pkey_obj, pubkey
        except (OSError, paramiko.SSHException) as err:
            raise AirflowException(f"Error encountered creating ssh keys, {err}")

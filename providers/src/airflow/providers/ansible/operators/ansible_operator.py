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

from typing import Any, Union, Optional, Collection, Mapping, Sequence
import json
import base64
import os
import ansible_runner
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
import airflow.models.xcom_arg
from airflow.lineage import prepare_lineage, apply_lineage
from airflow.providers.ansible.utils.sync_git_repo import sync_repo
from airflow.providers.ansible.utils.kms import get_secret


ALL_KEYS = {}

ANSIBLE_SSH_USER_KEY = "12345"
ANSIBLE_ARTIFACT_DIR = "/tmp/ansible/"
ANSIBLE_PLYBOOK_PROJECT = "Playbooks"
ANSIBLE_EVENT_STATUS = {
    "playbook_on_start": "running",
    "playbook_on_task_start": "running",
    "runner_on_ok": "successful",
    "runner_on_skipped": "skipped",
    "runner_on_failed": "failed",
    "runner_on_unreachable": "unreachable",
    "on_any": "unknown",
}
ANSIBLE_DEFAULT_VARS = {
    "ansible_user": "root",
}


class AnsibleOperator(BaseOperator):
    """
    Run an Ansible Runner task in the foreground and return a Runner object when complete.

    :param str playbook: The playbook (as a path relative to ``private_data_dir/project``) that will be invoked by runner when executing Ansible.
    :param dict or list roles_path: Directory or list of directories to assign to ANSIBLE_ROLES_PATH
    :param str or dict or list inventory: Overrides the inventory directory/file (supplied at ``private_data_dir/inventory``) with
        a specific host or list of hosts. This can take the form of:

            - Path to the inventory file in the ``private_data_dir``
            - Native python dict supporting the YAML/json inventory structure
            - A text INI formatted string
            - A list of inventory sources, or an empty list to disable passing inventory

    :param int forks: Control Ansible parallel concurrency
    :param str artifact_dir: The path to the directory where artifacts should live, this defaults to 'artifacts' under the private data dir
    :param int timeout: The timeout value in seconds that will be passed to either ``pexpect`` of ``subprocess`` invocation
                    (based on ``runner_mode`` selected) while executing command. It the timeout is triggered it will force cancel the
                    execution.
    :param dict extravars: Extra variables to be passed to Ansible at runtime using ``-e``. Extra vars will also be
                read from ``env/extravars`` in ``private_data_dir``.

    :param str ssh_key: The ssh key to be used (if necessary) to connect to the remote host, it need user authorize the kms key to AppId 100047735
    :param list kms_keys: The list of KMS keys to be used to decrypt the ansible extra vars, it need user authorize the kms key to AppId 100047735
    :param str path: The path to run the playbook under project directory
    :param str conn_id: The connection ID for the playbook git repo
    :param dict git_extra: Extra arguments to pass to the git clone command, e.g. {"branch": "prod"} {"tag": "v1.0.0"} {"commit_id": "123456"}
    :param list tags: List of tags to run
    :param list skip_tags: List of tags to skip
    :param bool get_ci_events: Get CI events
    """

    operator_fields: Sequence[str] = (
        "playbook",
        "kms_keys",
        "inventory",
        "roles_path",
        "extravars",
        "tags",
        "skip_tags",
        "artifact_dir",
        "conn_id",
        "git_extra",
        "path",
        "get_ci_events",
        "forks",
        "timeout",
        "ansible_vars",
    )
    template_fields_renderers = {
        "ssh_key": ANSIBLE_SSH_USER_KEY,
        "kms_keys": None,
        "path": "",
        "artifact_dir": ANSIBLE_ARTIFACT_DIR,
        "inventory": None,
        "conn_id": ANSIBLE_PLYBOOK_PROJECT,
        "roles_path": None,
        "extravars": None,
        "tags": None,
        "skip_tags": None,
        "get_ci_events": False,
        "forks": 10,
        "timeout": None,
        "git_extra": None,
    }
    ui_color = "#FFEFEB"
    ui_fgcolor = "#FF0000"

    def __init__(
        self,
        *,
        playbook: str = "",
        ssh_key: str = ANSIBLE_SSH_USER_KEY,
        kms_keys: Union[list, None] = None,
        path: str = "",
        artifact_dir: str = ANSIBLE_ARTIFACT_DIR,
        inventory: Union[dict, str, list, None] = None,
        conn_id: str = ANSIBLE_PLYBOOK_PROJECT,
        roles_path: Union[dict, list] = None,
        extravars: Union[dict, None] = None,
        tags: Union[list, None] = None,
        skip_tags: Union[list, None] = None,
        get_ci_events: bool = False,
        forks: int = 10,
        timeout: Union[int, None] = None,
        git_extra: Union[dict, None] = None,
        ansible_vars: dict = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            **kwargs,
        )
        self.playbook = playbook
        self.ssh_key = ssh_key
        self.kms_keys = kms_keys
        self.path = path
        self.artifact_dir = artifact_dir
        self.inventory = inventory
        self.conn_id = conn_id
        self.roles_path = roles_path
        self.extravars = extravars
        self.tags = tags
        self.skip_tags = skip_tags
        self.get_ci_events = get_ci_events
        self.forks = forks
        self.timeout = timeout
        self.git_extra = git_extra
        self.ansible_vars = ansible_vars
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}

        self.ci_events = {}
        self.last_event = {}
        self.project_dir = ""
        self.log.debug("playbook: %s", self.playbook)
        self.log.debug("playbook type: %s", type(self.playbook))

    def event_handler(self, data):
        """event handler"""
        if self.get_ci_events and data.get("event_data", {}).get("host"):
            self.ci_events[data["event_data"]["host"]] = data
        last_event = data
        self.log.info("event: %s", last_event)

    def get_key(self, kms_key: None) -> Optional[dict]:
        """get ssh key"""
        global ALL_KEYS  # pylint: disable=global-variable-not-assigned
        if kms_key in ALL_KEYS:
            return ALL_KEYS[kms_key]
        if kms_key is None:
            return None
        _, pwdValue = get_secret(token=kms_key)
        if pwdValue is None:
            return None
        try:
            ALL_KEYS[kms_key] = base64.b64decode(pwdValue).decode("utf-8")
            return ALL_KEYS[kms_key]
        except Exception:  # pylint: disable=broad-except
            return None

    @prepare_lineage
    def pre_execute(self, context: Context):
        if isinstance(self.ansible_vars, airflow.models.xcom_arg.PlainXComArg):
            self.ansible_vars = self.ansible_vars.resolve(context)
        if self.ansible_vars:
            for k in self.operator_fields:
                if k not in self.op_kwargs and k in self.ansible_vars:
                    setattr(self, k, self.ansible_vars.get(k))
        for attr in self.operator_fields:
            value = getattr(self, attr)
            if isinstance(value, airflow.models.xcom_arg.PlainXComArg):
                setattr(self, attr, value.resolve(context))

        for t in self.kms_keys or []:
            pwdKey, pwdValue = get_secret(token=t)
            if pwdKey and pwdKey not in self.extravars:
                self.extravars[pwdKey] = pwdValue
        for k, v in ANSIBLE_DEFAULT_VARS.items():
            if k not in self.extravars:
                self.extravars[k] = v
        self.log.debug("conn_id: %s", self.conn_id)
        self.log.debug("git_extra: %s", self.git_extra)
        self.project_dir = sync_repo(conn_id=self.conn_id, extra=self.git_extra)
        self.log.info(
            "project_dir: %s, project path: %s, playbook: %s",
            self.project_dir,
            self.path,
            self.playbook,
        )
        if self.project_dir == "":
            self.log.critical("project_dir is empty")
            raise AirflowException("project_dir is empty")
        if not os.path.exists(self.project_dir):
            self.log.critical("project_dir is not exist")
            raise AirflowException("project_dir is not exist")
        if not os.path.exists(self.artifact_dir):
            os.makedirs(self.artifact_dir)

        # tip: this will default inventory was a str for path, cannot pass it as ini
        if isinstance(self.inventory, str):
            self.inventory = os.path.join(self.project_dir, self.path, self.inventory)

    def execute(self, context: Context):
        self.log.info(
            "playbook: %s, roles_path: %s, project_dir: %s, inventory: %s, conn_id: %s, extravars: %s, tags: %s, "
            "skip_tags: %s",
            self.playbook,
            self.roles_path,
            self.project_dir,
            self.inventory,
            self.conn_id,
            self.extravars,
            self.tags,
            self.skip_tags,
        )
        r = ansible_runner.run(
            ssh_key=self.get_key(kms_key=self.ssh_key),
            quiet=True,
            roles_path=self.roles_path,
            tags=",".join(self.tags) if self.tags else None,
            skip_tags=",".join(self.skip_tags) if self.skip_tags else None,
            artifact_dir=self.artifact_dir,
            project_dir=os.path.join(self.project_dir, self.path),
            playbook=self.playbook,
            extravars=self.extravars,
            forks=self.forks,
            timeout=self.timeout,
            inventory=self.inventory,
            event_handler=self.event_handler,
            # status_handler=my_status_handler, # Disable printing to prevent sensitive information leakage, also unnecessary
            # artifacts_handler=my_artifacts_handler, # No need to print
            # cancel_callback=my_cancel_callback,
            # finished_callback=finish_callback,  # No need to print
        )
        self.log.info(
            "status: %s, artifact_dir: %s, command: %s, inventory: %s, playbook: %s, private_data_dir: %s, "
            "project_dir: %s, ci_events: %s",
            r.status,
            r.config.artifact_dir,
            r.config.command,
            r.config.inventory,
            r.config.playbook,
            r.config.private_data_dir,
            r.config.project_dir,
            self.ci_events,
        )
        context["ansible_return"] = {
            "canceled": r.canceled,
            "directory_isolation_cleanup": r.directory_isolation_cleanup,
            "directory_isolation_path": r.directory_isolation_path,
            "errored": r.errored,
            "last_stdout_update": r.last_stdout_update,
            "process_isolation": r.process_isolation,
            "process_isolation_path_actual": r.process_isolation_path_actual,
            "rc": r.rc,
            "remove_partials": r.remove_partials,
            "runner_mode": r.runner_mode,
            "stats": r.stats,
            "status": r.status,
            "timed_out": r.timed_out,
            # config
            "artifact_dir": r.config.artifact_dir,
            "command": r.config.command,
            "cwd": r.config.cwd,
            "fact_cache": r.config.fact_cache,
            "fact_cache_type": r.config.fact_cache_type,
            "ident": r.config.ident,
            "inventory": r.config.inventory,
            "playbook": r.config.playbook,
            "private_data_dir": r.config.private_data_dir,
            "project_dir": r.config.project_dir,
            # event
            "last_event": self.last_event,
            "ci_events": self.ci_events,
        }
        context["ti"].xcom_push(key="runner_id", value=r.config.ident)
        return context["ansible_return"]

    @apply_lineage
    def post_execute(self, context: Any, result: Any = None):
        """
        Execute right after self.execute() is called.

        It is passed the execution context and any results returned by the operator.
        """
        self.log.debug("post_execute context: %s", context)
        # Discuss whether to compress the results and transfer them to storage
        return
        artifact_path = os.path.join(self.artifact_dir, result["ident"])
        artifact_result_file = os.path.join(artifact_path, "result.txt")
        with open(artifact_result_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(result, indent=4))
        # Zip the artifact_path
        zip_file = os.path.join(
            self.artifact_dir,
            context["start_date"].strftime("%Y-%m-%d"),
            f"{context['run_id']}.zip",
        )
        os.system(f"zip -r {zip_file} {artifact_path}")
        self.log.info("Zipped artifact path: %s", zip_file)
        # todo: upload to some storage

    def on_kill(self) -> None:
        """
        Override this method to clean up subprocesses when a task instance gets killed.

        Any use of the threading, subprocess or multiprocessing module within an
        operator needs to be cleaned up, or it will leave ghost processes behind.
        """

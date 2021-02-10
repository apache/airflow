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

from __future__ import absolute_import

from airflow.upgrade.rules.base_rule import BaseRule
from airflow.configuration import conf

invalid_config_keys = {
    "airflow_configmap",
    "airflow_local_settings_configmap",
    "dags_in_image",
    "dags_volume_subpath",
    "dags_volume_mount_point", "dags_volume_claim",
    "logs_volume_subpath", "logs_volume_claim",
    "dags_volume_host", "logs_volume_host",
    "env_from_configmap_ref", "env_from_secret_ref", "git_repo",
    "git_branch", "git_sync_depth", "git_subpath",
    "git_sync_rev", "git_user", "git_password",
    "git_sync_root", "git_sync_dest",
    "git_dags_folder_mount_point", "git_ssh_key_secret_name",
    "git_ssh_known_hosts_configmap_name", "git_sync_credentials_secret",
    "git_sync_container_repository",
    "git_sync_container_tag", "git_sync_init_container_name",
    "git_sync_run_as_user",
    "worker_service_account_name", "image_pull_secrets",
    "gcp_service_account_keys", "affinity",
    "tolerations", "run_as_user", "fs_group"
}


class PodTemplateFileRule(BaseRule):
    title = "Users must set a kubernetes.pod_template_file value"

    description = """\
In Airflow 2.0, KubernetesExecutor Users need to set a pod_template_file as a base
value for all pods launched by the KubernetesExecutor. Many Kubernetes configs are no longer
needed once this pod_template_file has been generated.
"""

    def check(self):
        pod_template_file = conf.get("kubernetes", "pod_template_file", fallback=None)
        if not pod_template_file:
            return (
                "Please create a pod_template_file by running `airflow generate_pod_template`.\n"
                "This will generate a pod using your aiflow.cfg settings"
            )

        conf_dict = conf.as_dict(display_sensitive=True)
        kube_conf = conf_dict['kubernetes']
        keys = kube_conf.keys()
        resp = [k for k in keys if k in invalid_config_keys]
        if conf_dict['kubernetes_labels']:
            resp.append("kubernetes_labels")
        if conf_dict['kubernetes_secrets']:
            resp.append("kubernetes_secrets")

        if resp:
            resp_string = "\n".join(resp)
            return "The following invalid keys were found in your airflow.cfg: \
                   \n\n{resp_string}\n\n \
                   Now that you have a pod_template_file, these keys no longer do anything.\n\
                   Please delete these keys.".format(resp_string=resp_string)

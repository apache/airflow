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

from typing import TYPE_CHECKING

from jinja2 import TemplateAssertionError, UndefinedError
from kubernetes.client.api_client import ApiClient

from airflow.providers.cncf.kubernetes.kube_config import KubeConfig
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import create_unique_id
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator, generate_pod_command_args
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


def render_k8s_pod_yaml(task_instance: TaskInstance) -> dict | None:
    """Render k8s pod yaml."""
    kube_config = KubeConfig()
    if task_instance.executor_config and task_instance.executor_config.get("pod_template_file"):
        # If a specific pod_template_file was passed to the executor, we make
        # sure to render the k8s pod spec using this one, and not the default one.
        pod_template_file = task_instance.executor_config["pod_template_file"]
    else:
        # If no such pod_template_file override was passed, we can simply render
        # The pod spec using the default template.
        pod_template_file = kube_config.pod_template_file

    # Generate command args using shared utility function
    command_args = generate_pod_command_args(task_instance)

    pod = PodGenerator.construct_pod(
        dag_id=task_instance.dag_id,
        run_id=task_instance.run_id,
        task_id=task_instance.task_id,
        map_index=task_instance.map_index,
        date=None,
        pod_id=create_unique_id(task_instance.dag_id, task_instance.task_id),
        try_number=task_instance.try_number,
        kube_image=kube_config.kube_image,
        args=command_args,
        pod_override_object=PodGenerator.from_obj(task_instance.executor_config),
        scheduler_job_id="0",
        namespace=kube_config.executor_namespace,
        base_worker_pod=PodGenerator.deserialize_model_file(pod_template_file),
        with_mutation_hook=True,
    )
    sanitized_pod = ApiClient().sanitize_for_serialization(pod)
    return sanitized_pod


@provide_session
def get_rendered_k8s_spec(task_instance: TaskInstance, session=NEW_SESSION) -> dict | None:
    """Fetch rendered template fields from DB."""
    from airflow.models.renderedtifields import RenderedTaskInstanceFields

    rendered_k8s_spec = RenderedTaskInstanceFields.get_k8s_pod_yaml(task_instance, session=session)
    if not rendered_k8s_spec:
        try:
            rendered_k8s_spec = render_k8s_pod_yaml(task_instance)
        except (TemplateAssertionError, UndefinedError) as e:
            raise AirflowException(f"Unable to render a k8s spec for this taskinstance: {e}") from e
    return rendered_k8s_spec

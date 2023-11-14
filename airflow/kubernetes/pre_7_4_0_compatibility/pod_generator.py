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
"""
Pod generator compatible with cncf-providers released before 2.7.0 of airflow.

Compatible with pre-7.4.0 of the cncf.kubernetes provider.

This module provides an interface between the previous Pod
API and outputs a kubernetes.client.models.V1Pod.
The advantage being that the full Kubernetes API
is supported and no serialization need be written.
"""
from __future__ import annotations

import copy
import logging
import os
import secrets
import string
import warnings
from functools import reduce
from typing import TYPE_CHECKING

import re2
from dateutil import parser
from kubernetes.client import models as k8s
from kubernetes.client.api_client import ApiClient

from airflow.exceptions import (
    AirflowConfigException,
    PodMutationHookException,
    PodReconciliationError,
    RemovedInAirflow3Warning,
)
from airflow.kubernetes.pre_7_4_0_compatibility.pod_generator_deprecated import (
    PodDefaults,
    PodGenerator as PodGeneratorDeprecated,
)
from airflow.utils import yaml
from airflow.utils.hashlib_wrapper import md5
from airflow.version import version as airflow_version

if TYPE_CHECKING:
    import datetime

log = logging.getLogger(__name__)

MAX_LABEL_LEN = 63

alphanum_lower = string.ascii_lowercase + string.digits


def rand_str(num):
    """Generate random lowercase alphanumeric string of length num.

    :meta private:
    """
    return "".join(secrets.choice(alphanum_lower) for _ in range(num))


def add_pod_suffix(pod_name: str, rand_len: int = 8, max_len: int = 80) -> str:
    """Add random string to pod name while staying under max length.

    :param pod_name: name of the pod
    :param rand_len: length of the random string to append
    :max_len: maximum length of the pod name
    :meta private:
    """
    suffix = "-" + rand_str(rand_len)
    return pod_name[: max_len - len(suffix)].strip("-.") + suffix


def make_safe_label_value(string: str) -> str:
    """
    Normalize a provided label to be of valid length and characters.

    Valid label values must be 63 characters or less and must be empty or begin and
    end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
    dots (.), and alphanumerics between.

    If the label value is greater than 63 chars once made safe, or differs in any
    way from the original value sent to this function, then we need to truncate to
    53 chars, and append it with a unique hash.
    """
    safe_label = re2.sub(r"^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$", "", string)

    if len(safe_label) > MAX_LABEL_LEN or string != safe_label:
        safe_hash = md5(string.encode()).hexdigest()[:9]
        safe_label = safe_label[: MAX_LABEL_LEN - len(safe_hash) - 1] + "-" + safe_hash

    return safe_label


def datetime_to_label_safe_datestring(datetime_obj: datetime.datetime) -> str:
    """
    Transform a datetime string to use as a label.

    Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but
    not "_" let's
    replace ":" with "_"

    :param datetime_obj: datetime.datetime object
    :return: ISO-like string representing the datetime
    """
    return datetime_obj.isoformat().replace(":", "_").replace("+", "_plus_")


def label_safe_datestring_to_datetime(string: str) -> datetime.datetime:
    """
    Transform a label back to a datetime object.

    Kubernetes doesn't permit ":" in labels. ISO datetime format uses ":" but not
    "_", let's
    replace ":" with "_"

    :param string: str
    :return: datetime.datetime object
    """
    return parser.parse(string.replace("_plus_", "+").replace("_", ":"))


class PodGenerator:
    """
    Contains Kubernetes Airflow Worker configuration logic.

    Represents a kubernetes pod and manages execution of a single pod.
    Any configuration that is container specific gets applied to
    the first container in the list of containers.

    :param pod: The fully specified pod. Mutually exclusive with `pod_template_file`
    :param pod_template_file: Path to YAML file. Mutually exclusive with `pod`
    :param extract_xcom: Whether to bring up a container for xcom
    """

    def __init__(
        self,
        pod: k8s.V1Pod | None = None,
        pod_template_file: str | None = None,
        extract_xcom: bool = True,
    ):
        if not pod_template_file and not pod:
            raise AirflowConfigException(
                "Podgenerator requires either a `pod` or a `pod_template_file` argument"
            )
        if pod_template_file and pod:
            raise AirflowConfigException("Cannot pass both `pod` and `pod_template_file` arguments")

        if pod_template_file:
            self.ud_pod = self.deserialize_model_file(pod_template_file)
        else:
            self.ud_pod = pod

        # Attach sidecar
        self.extract_xcom = extract_xcom

    def gen_pod(self) -> k8s.V1Pod:
        """Generate pod."""
        warnings.warn("This function is deprecated. ", RemovedInAirflow3Warning)
        result = self.ud_pod

        result.metadata.name = add_pod_suffix(pod_name=result.metadata.name)

        if self.extract_xcom:
            result = self.add_xcom_sidecar(result)

        return result

    @staticmethod
    def add_xcom_sidecar(pod: k8s.V1Pod) -> k8s.V1Pod:
        """Add sidecar."""
        warnings.warn(
            "This function is deprecated. "
            "Please use airflow.providers.cncf.kubernetes.utils.xcom_sidecar.add_xcom_sidecar instead"
        )
        pod_cp = copy.deepcopy(pod)
        pod_cp.spec.volumes = pod.spec.volumes or []
        pod_cp.spec.volumes.insert(0, PodDefaults.VOLUME)
        pod_cp.spec.containers[0].volume_mounts = pod_cp.spec.containers[0].volume_mounts or []
        pod_cp.spec.containers[0].volume_mounts.insert(0, PodDefaults.VOLUME_MOUNT)
        pod_cp.spec.containers.append(PodDefaults.SIDECAR_CONTAINER)

        return pod_cp

    @staticmethod
    def from_obj(obj) -> dict | k8s.V1Pod | None:
        """Convert to pod from obj."""
        if obj is None:
            return None

        k8s_legacy_object = obj.get("KubernetesExecutor", None)
        k8s_object = obj.get("pod_override", None)

        if k8s_legacy_object and k8s_object:
            raise AirflowConfigException(
                "Can not have both a legacy and new"
                "executor_config object. Please delete the KubernetesExecutor"
                "dict and only use the pod_override kubernetes.client.models.V1Pod"
                "object."
            )
        if not k8s_object and not k8s_legacy_object:
            return None

        if isinstance(k8s_object, k8s.V1Pod):
            return k8s_object
        elif isinstance(k8s_legacy_object, dict):
            warnings.warn(
                "Using a dictionary for the executor_config is deprecated and will soon be removed."
                'please use a `kubernetes.client.models.V1Pod` class with a "pod_override" key'
                " instead. ",
                category=RemovedInAirflow3Warning,
            )
            return PodGenerator.from_legacy_obj(obj)
        else:
            raise TypeError(
                "Cannot convert a non-kubernetes.client.models.V1Pod object into a KubernetesExecutorConfig"
            )

    @staticmethod
    def from_legacy_obj(obj) -> k8s.V1Pod | None:
        """Convert to pod from obj."""
        if obj is None:
            return None

        # We do not want to extract constant here from ExecutorLoader because it is just
        # A name in dictionary rather than executor selection mechanism and it causes cyclic import
        namespaced = obj.get("KubernetesExecutor", {})

        if not namespaced:
            return None

        resources = namespaced.get("resources")

        if resources is None:
            requests = {
                "cpu": namespaced.pop("request_cpu", None),
                "memory": namespaced.pop("request_memory", None),
                "ephemeral-storage": namespaced.get("ephemeral-storage"),  # We pop this one in limits
            }
            limits = {
                "cpu": namespaced.pop("limit_cpu", None),
                "memory": namespaced.pop("limit_memory", None),
                "ephemeral-storage": namespaced.pop("ephemeral-storage", None),
            }
            all_resources = list(requests.values()) + list(limits.values())
            if all(r is None for r in all_resources):
                resources = None
            else:
                # remove None's so they don't become 0's
                requests = {k: v for k, v in requests.items() if v is not None}
                limits = {k: v for k, v in limits.items() if v is not None}
                resources = k8s.V1ResourceRequirements(requests=requests, limits=limits)
        namespaced["resources"] = resources
        return PodGeneratorDeprecated(**namespaced).gen_pod()

    @staticmethod
    def reconcile_pods(base_pod: k8s.V1Pod, client_pod: k8s.V1Pod | None) -> k8s.V1Pod:
        """
        Merge Kubernetes Pod objects.

        :param base_pod: has the base attributes which are overwritten if they exist
            in the client pod and remain if they do not exist in the client_pod
        :param client_pod: the pod that the client wants to create.
        :return: the merged pods

        This can't be done recursively as certain fields are overwritten and some are concatenated.
        """
        if client_pod is None:
            return base_pod

        client_pod_cp = copy.deepcopy(client_pod)
        client_pod_cp.spec = PodGenerator.reconcile_specs(base_pod.spec, client_pod_cp.spec)
        client_pod_cp.metadata = PodGenerator.reconcile_metadata(base_pod.metadata, client_pod_cp.metadata)
        client_pod_cp = merge_objects(base_pod, client_pod_cp)

        return client_pod_cp

    @staticmethod
    def reconcile_metadata(base_meta, client_meta):
        """
        Merge Kubernetes Metadata objects.

        :param base_meta: has the base attributes which are overwritten if they exist
            in the client_meta and remain if they do not exist in the client_meta
        :param client_meta: the spec that the client wants to create.
        :return: the merged specs
        """
        if base_meta and not client_meta:
            return base_meta
        if not base_meta and client_meta:
            return client_meta
        elif client_meta and base_meta:
            client_meta.labels = merge_objects(base_meta.labels, client_meta.labels)
            client_meta.annotations = merge_objects(base_meta.annotations, client_meta.annotations)
            extend_object_field(base_meta, client_meta, "managed_fields")
            extend_object_field(base_meta, client_meta, "finalizers")
            extend_object_field(base_meta, client_meta, "owner_references")
            return merge_objects(base_meta, client_meta)

        return None

    @staticmethod
    def reconcile_specs(
        base_spec: k8s.V1PodSpec | None, client_spec: k8s.V1PodSpec | None
    ) -> k8s.V1PodSpec | None:
        """
        Merge Kubernetes PodSpec objects.

        :param base_spec: has the base attributes which are overwritten if they exist
            in the client_spec and remain if they do not exist in the client_spec
        :param client_spec: the spec that the client wants to create.
        :return: the merged specs
        """
        if base_spec and not client_spec:
            return base_spec
        if not base_spec and client_spec:
            return client_spec
        elif client_spec and base_spec:
            client_spec.containers = PodGenerator.reconcile_containers(
                base_spec.containers, client_spec.containers
            )
            merged_spec = extend_object_field(base_spec, client_spec, "init_containers")
            merged_spec = extend_object_field(base_spec, merged_spec, "volumes")
            return merge_objects(base_spec, merged_spec)

        return None

    @staticmethod
    def reconcile_containers(
        base_containers: list[k8s.V1Container], client_containers: list[k8s.V1Container]
    ) -> list[k8s.V1Container]:
        """
        Merge Kubernetes Container objects.

        :param base_containers: has the base attributes which are overwritten if they exist
            in the client_containers and remain if they do not exist in the client_containers
        :param client_containers: the containers that the client wants to create.
        :return: the merged containers

        The runs recursively over the list of containers.
        """
        if not base_containers:
            return client_containers
        if not client_containers:
            return base_containers

        client_container = client_containers[0]
        base_container = base_containers[0]
        client_container = extend_object_field(base_container, client_container, "volume_mounts")
        client_container = extend_object_field(base_container, client_container, "env")
        client_container = extend_object_field(base_container, client_container, "env_from")
        client_container = extend_object_field(base_container, client_container, "ports")
        client_container = extend_object_field(base_container, client_container, "volume_devices")
        client_container = merge_objects(base_container, client_container)

        return [
            client_container,
            *PodGenerator.reconcile_containers(base_containers[1:], client_containers[1:]),
        ]

    @classmethod
    def construct_pod(
        cls,
        dag_id: str,
        task_id: str,
        pod_id: str,
        try_number: int,
        kube_image: str,
        date: datetime.datetime | None,
        args: list[str],
        pod_override_object: k8s.V1Pod | None,
        base_worker_pod: k8s.V1Pod,
        namespace: str,
        scheduler_job_id: str,
        run_id: str | None = None,
        map_index: int = -1,
        *,
        with_mutation_hook: bool = False,
    ) -> k8s.V1Pod:
        """
        Create a Pod.

        Construct a pod by gathering and consolidating the configuration from 3 places:
            - airflow.cfg
            - executor_config
            - dynamic arguments
        """
        if len(pod_id) > 253:
            warnings.warn(
                "pod_id supplied is longer than 253 characters; truncating and adding unique suffix."
            )
            pod_id = add_pod_suffix(pod_name=pod_id, max_len=253)
        try:
            image = pod_override_object.spec.containers[0].image  # type: ignore
            if not image:
                image = kube_image
        except Exception:
            image = kube_image

        annotations = {
            "dag_id": dag_id,
            "task_id": task_id,
            "try_number": str(try_number),
        }
        if map_index >= 0:
            annotations["map_index"] = str(map_index)
        if date:
            annotations["execution_date"] = date.isoformat()
        if run_id:
            annotations["run_id"] = run_id

        dynamic_pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                namespace=namespace,
                annotations=annotations,
                name=pod_id,
                labels=cls.build_labels_for_k8s_executor_pod(
                    dag_id=dag_id,
                    task_id=task_id,
                    try_number=try_number,
                    airflow_worker=scheduler_job_id,
                    map_index=map_index,
                    execution_date=date,
                    run_id=run_id,
                ),
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        args=args,
                        image=image,
                        env=[k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value="True")],
                    )
                ]
            ),
        )

        # Reconcile the pods starting with the first chronologically,
        # Pod from the pod_template_File -> Pod from executor_config arg -> Pod from the K8s executor
        pod_list = [base_worker_pod, pod_override_object, dynamic_pod]

        try:
            pod = reduce(PodGenerator.reconcile_pods, pod_list)
        except Exception as e:
            raise PodReconciliationError from e

        if with_mutation_hook:
            from airflow.settings import pod_mutation_hook

            try:
                pod_mutation_hook(pod)
            except Exception as e:
                raise PodMutationHookException from e

        return pod

    @classmethod
    def build_selector_for_k8s_executor_pod(
        cls,
        *,
        dag_id,
        task_id,
        try_number,
        map_index=None,
        execution_date=None,
        run_id=None,
        airflow_worker=None,
    ):
        """
        Generate selector for kubernetes executor pod.

        :meta private:
        """
        labels = cls.build_labels_for_k8s_executor_pod(
            dag_id=dag_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            execution_date=execution_date,
            run_id=run_id,
            airflow_worker=airflow_worker,
        )
        label_strings = [f"{label_id}={label}" for label_id, label in sorted(labels.items())]
        if not airflow_worker:  # this filters out KPO pods even when we don't know the scheduler job id
            label_strings.append("airflow-worker")
        selector = ",".join(label_strings)
        return selector

    @classmethod
    def build_labels_for_k8s_executor_pod(
        cls,
        *,
        dag_id,
        task_id,
        try_number,
        airflow_worker=None,
        map_index=None,
        execution_date=None,
        run_id=None,
    ):
        """
        Generate labels for kubernetes executor pod.

        :meta private:
        """
        labels = {
            "dag_id": make_safe_label_value(dag_id),
            "task_id": make_safe_label_value(task_id),
            "try_number": str(try_number),
            "kubernetes_executor": "True",
            "airflow_version": airflow_version.replace("+", "-"),
        }
        if airflow_worker is not None:
            labels["airflow-worker"] = make_safe_label_value(str(airflow_worker))
        if map_index is not None and map_index >= 0:
            labels["map_index"] = str(map_index)
        if execution_date:
            labels["execution_date"] = datetime_to_label_safe_datestring(execution_date)
        if run_id:
            labels["run_id"] = make_safe_label_value(run_id)
        return labels

    @staticmethod
    def serialize_pod(pod: k8s.V1Pod) -> dict:
        """
        Convert a k8s.V1Pod into a json serializable dictionary.

        :param pod: k8s.V1Pod object
        :return: Serialized version of the pod returned as dict
        """
        api_client = ApiClient()
        return api_client.sanitize_for_serialization(pod)

    @staticmethod
    def deserialize_model_file(path: str) -> k8s.V1Pod:
        """
        Generate a Pod from a file.

        :param path: Path to the file
        :return: a kubernetes.client.models.V1Pod
        """
        if os.path.exists(path):
            with open(path) as stream:
                pod = yaml.safe_load(stream)
        else:
            pod = None
            log.warning("Model file %s does not exist", path)

        return PodGenerator.deserialize_model_dict(pod)

    @staticmethod
    def deserialize_model_dict(pod_dict: dict | None) -> k8s.V1Pod:
        """
        Deserializes a Python dictionary to k8s.V1Pod.

        Unfortunately we need access to the private method
        ``_ApiClient__deserialize_model`` from the kubernetes client.
        This issue is tracked here; https://github.com/kubernetes-client/python/issues/977.

        :param pod_dict: Serialized dict of k8s.V1Pod object
        :return: De-serialized k8s.V1Pod
        """
        api_client = ApiClient()
        return api_client._ApiClient__deserialize_model(pod_dict, k8s.V1Pod)

    @staticmethod
    def make_unique_pod_id(pod_id: str) -> str | None:
        r"""
        Generate a unique Pod name.

        Kubernetes pod names must consist of one or more lowercase
        rfc1035/rfc1123 labels separated by '.' with a maximum length of 253
        characters.

        Name must pass the following regex for validation
        ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

        For more details, see:
        https://github.com/kubernetes/kubernetes/blob/release-1.1/docs/design/identifiers.md

        :param pod_id: requested pod name
        :return: ``str`` valid Pod name of appropriate length
        """
        warnings.warn(
            "This function is deprecated. Use `add_pod_suffix` in `kubernetes_helper_functions`.",
            RemovedInAirflow3Warning,
        )

        if not pod_id:
            return None

        max_pod_id_len = 100  # arbitrarily chosen
        suffix = rand_str(8)  # 8 seems good enough
        base_pod_id_len = max_pod_id_len - len(suffix) - 1  # -1 for separator
        trimmed_pod_id = pod_id[:base_pod_id_len].rstrip("-.")
        return f"{trimmed_pod_id}-{suffix}"


def merge_objects(base_obj, client_obj):
    """
    Merge objects.

    :param base_obj: has the base attributes which are overwritten if they exist
        in the client_obj and remain if they do not exist in the client_obj
    :param client_obj: the object that the client wants to create.
    :return: the merged objects
    """
    if not base_obj:
        return client_obj
    if not client_obj:
        return base_obj

    client_obj_cp = copy.deepcopy(client_obj)

    if isinstance(base_obj, dict) and isinstance(client_obj_cp, dict):
        base_obj_cp = copy.deepcopy(base_obj)
        base_obj_cp.update(client_obj_cp)
        return base_obj_cp

    for base_key in base_obj.to_dict().keys():
        base_val = getattr(base_obj, base_key, None)
        if not getattr(client_obj, base_key, None) and base_val:
            if not isinstance(client_obj_cp, dict):
                setattr(client_obj_cp, base_key, base_val)
            else:
                client_obj_cp[base_key] = base_val
    return client_obj_cp


def extend_object_field(base_obj, client_obj, field_name):
    """
    Add field values to existing objects.

    :param base_obj: an object which has a property `field_name` that is a list
    :param client_obj: an object which has a property `field_name` that is a list.
        A copy of this object is returned with `field_name` modified
    :param field_name: the name of the list field
    :return: the client_obj with the property `field_name` being the two properties appended
    """
    client_obj_cp = copy.deepcopy(client_obj)
    base_obj_field = getattr(base_obj, field_name, None)
    client_obj_field = getattr(client_obj, field_name, None)

    if (not isinstance(base_obj_field, list) and base_obj_field is not None) or (
        not isinstance(client_obj_field, list) and client_obj_field is not None
    ):
        raise ValueError("The chosen field must be a list.")

    if not base_obj_field:
        return client_obj_cp
    if not client_obj_field:
        setattr(client_obj_cp, field_name, base_obj_field)
        return client_obj_cp

    appended_fields = base_obj_field + client_obj_field
    setattr(client_obj_cp, field_name, appended_fields)
    return client_obj_cp

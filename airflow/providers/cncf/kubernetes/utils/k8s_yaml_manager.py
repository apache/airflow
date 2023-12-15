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
"""
from __future__ import annotations

import copy

from kubernetes.client import models as k8s


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
    client_pod_cp.spec = reconcile_pod_specs(base_pod.spec, client_pod_cp.spec)
    client_pod_cp.metadata = reconcile_metadata(base_pod.metadata, client_pod_cp.metadata)
    client_pod_cp = merge_objects(base_pod, client_pod_cp)

    return client_pod_cp


def reconcile_pod_specs(
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
        client_spec.containers = reconcile_containers(base_spec.containers, client_spec.containers)
        merged_spec = extend_object_field(base_spec, client_spec, "init_containers")
        merged_spec = extend_object_field(base_spec, merged_spec, "volumes")
        return merge_objects(base_spec, merged_spec)

    return None


def reconcile_jobs(base_job: k8s.V1Job, client_job: k8s.V1Job | None) -> k8s.V1Job:
    """
    Merge Kubernetes Job objects.

    :param base_job: has the base attributes which are overwritten if they exist
        in the client job and remain if they do not exist in the client_job
    :param client_job: the job that the client wants to create.
    :return: the merged jobs

    This can't be done recursively as certain fields are overwritten and some are concatenated.
    """
    if client_job is None:
        return base_job

    client_job_cp = copy.deepcopy(client_job)
    client_job_cp.spec = reconcile_job_specs(base_job.spec, client_job_cp.spec)
    client_job_cp.metadata = reconcile_metadata(base_job.metadata, client_job_cp.metadata)
    client_job_cp = merge_objects(base_job, client_job_cp)

    return client_job_cp


def reconcile_job_specs(
    base_spec: k8s.V1JobSpec | None, client_spec: k8s.V1JobSpec | None
) -> k8s.V1JobSpec | None:
    """
    Merge Kubernetes JobSpec objects.

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
        client_spec.containers = reconcile_containers(base_spec.containers, client_spec.containers)
        merged_spec = extend_object_field(base_spec, client_spec, "init_containers")
        merged_spec = extend_object_field(base_spec, merged_spec, "volumes")
        return merge_objects(base_spec, merged_spec)

    return None


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
        *reconcile_containers(base_containers[1:], client_containers[1:]),
    ]


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

    for base_key in base_obj.to_dict():
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
        raise ValueError(
            f"The chosen field must be a list. Got {type(base_obj_field)} base_object_field "
            f"and {type(client_obj_field)} client_object_field."
        )

    if not base_obj_field:
        return client_obj_cp
    if not client_obj_field:
        setattr(client_obj_cp, field_name, base_obj_field)
        return client_obj_cp

    appended_fields = base_obj_field + client_obj_field
    setattr(client_obj_cp, field_name, appended_fields)
    return client_obj_cp

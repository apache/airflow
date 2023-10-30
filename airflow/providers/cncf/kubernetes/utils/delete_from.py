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

# from https://github.com/tomplus/kubernetes_asyncio/pull/239/files

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from kubernetes import client

if TYPE_CHECKING:
    from kubernetes.client import ApiClient

DEFAULT_DELETION_BODY = client.V1DeleteOptions(
    propagation_policy="Background",
    grace_period_seconds=5,
)


def delete_from_dict(k8s_client, data, body, namespace, verbose=False, **kwargs):
    api_exceptions = []

    if "List" in data["kind"]:
        kind = data["kind"].replace("List", "")
        for yml_doc in data["items"]:
            if kind != "":
                yml_doc["apiVersion"] = data["apiVersion"]
                yml_doc["kind"] = kind
            try:
                _delete_from_yaml_single_item(
                    k8s_client=k8s_client,
                    yml_document=yml_doc,
                    verbose=verbose,
                    namespace=namespace,
                    body=body,
                    **kwargs,
                )
            except client.rest.ApiException as api_exception:
                api_exceptions.append(api_exception)
    else:
        try:
            _delete_from_yaml_single_item(
                k8s_client=k8s_client,
                yml_document=data,
                verbose=verbose,
                namespace=namespace,
                body=body,
                **kwargs,
            )
        except client.rest.ApiException as api_exception:
            api_exceptions.append(api_exception)

    if api_exceptions:
        raise FailToDeleteError(api_exceptions)


def delete_from_yaml(
    *,
    k8s_client: ApiClient,
    yaml_objects=None,
    verbose: bool = False,
    namespace: str = "default",
    body: dict | None = None,
    **kwargs,
):
    for yml_document in yaml_objects:
        if yml_document is not None:
            delete_from_dict(
                k8s_client=k8s_client,
                data=yml_document,
                body=body,
                namespace=namespace,
                verbose=verbose,
                **kwargs,
            )


def _delete_from_yaml_single_item(
    *,
    k8s_client: ApiClient,
    yml_document: dict,
    verbose: bool = False,
    namespace: str = "default",
    body: dict | None = None,
    **kwargs,
):
    if body is None:
        body = DEFAULT_DELETION_BODY

    # get group and version from apiVersion
    group, _, version = yml_document["apiVersion"].partition("/")
    if version == "":
        version = group
        group = "core"
    # Take care for the case e.g. api_type is "apiextensions.k8s.io"
    # Only replace the last instance
    group = "".join(group.rsplit(".k8s.io", 1))
    # convert group name from DNS subdomain format to
    # python class name convention
    group = "".join(word.capitalize() for word in group.split("."))
    fcn_to_call = f"{group}{version.capitalize()}Api"
    k8s_api = getattr(client, fcn_to_call)(k8s_client)
    # Replace CamelCased action_type into snake_case
    kind = yml_document["kind"]
    kind = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", kind)
    kind = re.sub("([a-z0-9])([A-Z])", r"\1_\2", kind).lower()

    # Decide which namespace we are going to use for deleting the object
    # IMPORTANT: the docs namespace takes precedence over the namespace in args
    #    create_from_yaml_single_item have same behaviour
    if "namespace" in yml_document["metadata"]:
        namespace = yml_document["metadata"]["namespace"]
    name = yml_document["metadata"]["name"]

    # Expect the user to delete namespaced objects more often
    resp: client.V1Status
    if hasattr(k8s_api, f"delete_namespaced_{kind}"):
        resp = getattr(k8s_api, f"delete_namespaced_{kind}")(
            name=name, namespace=namespace, body=body, **kwargs
        )
    else:
        resp = getattr(k8s_api, f"delete_{kind}")(name=name, body=body, **kwargs)
    if verbose:
        print(f"{kind} deleted. status='{resp.status}'")
    return resp


class FailToDeleteError(Exception):
    """For handling error if an error occurred when handling a yaml file during deletion of the resource."""

    def __init__(self, api_exceptions: list):
        self.api_exceptions = api_exceptions

    def __str__(self):
        msg = ""
        for api_exception in self.api_exceptions:
            msg += f"Error from server ({api_exception.reason}):{api_exception.body}\n"
        return msg

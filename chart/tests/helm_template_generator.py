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

import json
import subprocess
import sys
from functools import lru_cache
from io import StringIO
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Tuple

import jmespath
import jsonschema
import requests
import yaml
from kubernetes.client.api_client import ApiClient

api_client = ApiClient()

DEFAULT_KUBERNETES_VERSION = "1.22.0"
BASE_URL_SPEC = (
    f"https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/"
    f"v{DEFAULT_KUBERNETES_VERSION}-standalone-strict"
)

crd_lookup = {
    'keda.sh/v1alpha1::ScaledObject': 'https://raw.githubusercontent.com/kedacore/keda/v2.0.0/config/crd/bases/keda.sh_scaledobjects.yaml',  # noqa: E501
    # This object type was removed in k8s v1.22.0
    'networking.k8s.io/v1beta1::Ingress': 'https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/v1.21.0/ingress-networking-v1beta1.json',  # noqa: E501
}


def get_schema_k8s(api_version, kind, kubernetes_version):
    api_version = api_version.lower()
    kind = kind.lower()

    if '/' in api_version:
        ext, _, api_version = api_version.partition("/")
        ext = ext.split(".")[0]
        url = f'{BASE_URL_SPEC}/{kind}-{ext}-{api_version}.json'
    else:
        url = f'{BASE_URL_SPEC}/{kind}-{api_version}.json'
    request = requests.get(url)
    request.raise_for_status()
    schema = json.loads(
        request.text.replace(
            'kubernetesjsonschema.dev', 'raw.githubusercontent.com/yannh/kubernetes-json-schema/master'
        )
    )
    return schema


def get_schema_crd(api_version, kind):
    url = crd_lookup.get(f"{api_version}::{kind}")
    if not url:
        return None
    response = requests.get(url)
    yaml_schema = response.content.decode('utf-8')
    schema = yaml.safe_load(StringIO(yaml_schema))
    return schema


@lru_cache(maxsize=None)
def create_validator(api_version, kind, kubernetes_version):
    schema = get_schema_crd(api_version, kind)
    if not schema:
        schema = get_schema_k8s(api_version, kind, kubernetes_version)
    jsonschema.Draft7Validator.check_schema(schema)
    validator = jsonschema.Draft7Validator(schema)
    return validator


def validate_k8s_object(instance, kubernetes_version):
    # Skip PostgreSQL chart
    labels = jmespath.search("metadata.labels", instance)
    if "helm.sh/chart" in labels:
        chart = labels["helm.sh/chart"]
    else:
        chart = labels.get("chart")

    if chart and 'postgresql' in chart:
        return

    validate = create_validator(instance.get("apiVersion"), instance.get("kind"), kubernetes_version)
    validate.validate(instance)


def render_chart(
    name="RELEASE-NAME",
    values=None,
    show_only=None,
    chart_dir=None,
    kubernetes_version=DEFAULT_KUBERNETES_VERSION,
):
    """
    Function that renders a helm chart into dictionaries. For helm chart testing only
    """
    values = values or {}
    chart_dir = chart_dir or sys.path[0]
    with NamedTemporaryFile() as tmp_file:
        content = yaml.dump(values)
        tmp_file.write(content.encode())
        tmp_file.flush()
        command = [
            "helm",
            "template",
            name,
            chart_dir,
            '--values',
            tmp_file.name,
            '--kube-version',
            kubernetes_version,
        ]
        if show_only:
            for i in show_only:
                command.extend(["--show-only", i])
        templates = subprocess.check_output(command, stderr=subprocess.PIPE)
        k8s_objects = yaml.full_load_all(templates)
        k8s_objects = [k8s_object for k8s_object in k8s_objects if k8s_object]  # type: ignore
        for k8s_object in k8s_objects:
            validate_k8s_object(k8s_object, kubernetes_version)
        return k8s_objects


def prepare_k8s_lookup_dict(k8s_objects) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Helper to create a lookup dict from k8s_objects.
    The keys of the dict are the k8s object's kind and name
    """
    k8s_obj_by_key = {
        (k8s_object["kind"], k8s_object["metadata"]["name"]): k8s_object for k8s_object in k8s_objects
    }
    return k8s_obj_by_key


def render_k8s_object(obj, type_to_render):
    """
    Function that renders dictionaries into k8s objects. For helm chart testing only.
    """
    return api_client._ApiClient__deserialize_model(obj, type_to_render)

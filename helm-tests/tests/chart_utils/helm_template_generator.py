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

import ast
import json
import os
import subprocess
from functools import cache
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.error import HTTPError
from urllib.request import urlopen

import jmespath
import jsonschema
import yaml

AIRFLOW_ROOT = Path(__file__).resolve().parents[3]
CHART_DIR = AIRFLOW_ROOT / "chart"

SCHEMA_URL_TEMPLATE = (
    "https://airflow.apache.org/k8s-schemas/v{kubernetes_version}-standalone-strict/{filename}"
)


def _read_default_kubernetes_version() -> str:
    """Read the first ALLOWED_KUBERNETES_VERSIONS entry from global_constants.py."""
    gc_path = AIRFLOW_ROOT / "dev" / "breeze" / "src" / "airflow_breeze" / "global_constants.py"
    tree = ast.parse(gc_path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "ALLOWED_KUBERNETES_VERSIONS":
                    versions: list[str] = ast.literal_eval(node.value)
                    return versions[0].lstrip("v")
    raise RuntimeError("ALLOWED_KUBERNETES_VERSIONS not found in global_constants.py")


DEFAULT_KUBERNETES_VERSION = os.environ.get(
    "HELM_TEST_KUBERNETES_VERSION", _read_default_kubernetes_version()
)

MY_DIR = Path(__file__).parent.resolve()

crd_lookup = {
    # https://raw.githubusercontent.com/kedacore/keda/v2.0.0/config/crd/bases/keda.sh_scaledobjects.yaml
    "keda.sh/v1alpha1::ScaledObject": f"{MY_DIR.as_posix()}/keda.sh_scaledobjects.yaml"
}


@cache
def get_schema_k8s(api_version, kind, kubernetes_version):
    api_version = api_version.lower()
    kind = kind.lower()

    if "/" in api_version:
        ext, _, api_version = api_version.partition("/")
        ext = ext.split(".")[0]
        filename = f"{kind}-{ext}-{api_version}.json"
    else:
        filename = f"{kind}-{api_version}.json"

    url = SCHEMA_URL_TEMPLATE.format(kubernetes_version=kubernetes_version, filename=filename)
    try:
        resp = urlopen(url, timeout=30)
        schema = json.loads(resp.read())
    except HTTPError as e:
        if e.code == 404:
            raise FileNotFoundError(
                f"K8s JSON schema not found at {url}\n"
                f"Ensure schemas for K8s v{kubernetes_version} are published to airflow-site."
            ) from e
        raise
    return schema


@cache
def get_schema_crd(api_version, kind):
    file = crd_lookup.get(f"{api_version}::{kind}")
    if not file:
        return None
    schema = yaml.safe_load(Path(file).read_text())
    return schema


@cache
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

    if chart and "postgresql" in chart:
        return

    validate = create_validator(instance.get("apiVersion"), instance.get("kind"), kubernetes_version)
    validate.validate(instance)


class HelmFailedError(subprocess.CalledProcessError):
    def __str__(self):
        return f"Helm command failed. Args: {self.args}\nStderr: \n{self.stderr.decode('utf-8')}"


def render_chart(
    name="release-name",
    values=None,
    show_only=None,
    chart_dir=None,
    kubernetes_version=DEFAULT_KUBERNETES_VERSION,
    namespace=None,
):
    """
    Function that renders a helm chart into dictionaries. For helm chart testing only
    """
    values = values or {}
    chart_dir = chart_dir or str(CHART_DIR)
    namespace = namespace or "default"
    with NamedTemporaryFile() as tmp_file:
        content = yaml.dump(values)
        tmp_file.write(content.encode())
        tmp_file.flush()
        command = [
            "helm",
            "template",
            name,
            chart_dir,
            "--values",
            tmp_file.name,
            "--kube-version",
            kubernetes_version,
            "--namespace",
            namespace,
        ]
        if show_only:
            for i in show_only:
                command.extend(["--show-only", i])
        result = subprocess.run(command, check=False, capture_output=True, cwd=chart_dir)
        if result.returncode:
            raise HelmFailedError(result.returncode, result.args, result.stdout, result.stderr)
        templates = result.stdout
        k8s_objects = yaml.full_load_all(templates)
        k8s_objects = [k8s_object for k8s_object in k8s_objects if k8s_object]  # type: ignore
        for k8s_object in k8s_objects:
            validate_k8s_object(k8s_object, kubernetes_version)
        return k8s_objects


def prepare_k8s_lookup_dict(k8s_objects) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Helper to create a lookup dict from k8s_objects.
    The keys of the dict are the k8s object's kind and name
    """
    k8s_obj_by_key = {
        (k8s_object["kind"], k8s_object["metadata"]["name"]): k8s_object for k8s_object in k8s_objects
    }
    return k8s_obj_by_key

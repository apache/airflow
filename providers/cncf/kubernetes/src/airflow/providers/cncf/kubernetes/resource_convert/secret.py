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

from kubernetes.client import models as k8s


def convert_secret(secret_name: str) -> k8s.V1EnvFromSource:
    """
    Convert a str into an k8s object.

    :param secret_name:
    :return:
    """
    return k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=secret_name))


def convert_image_pull_secrets(image_pull_secrets: str) -> list[k8s.V1LocalObjectReference]:
    """
    Convert an image pull secret name into k8s local object reference.

    :param image_pull_secrets: comma separated string that contains secrets
    :return:
    """
    secrets = image_pull_secrets.split(",")
    return [k8s.V1LocalObjectReference(name=secret) for secret in secrets]

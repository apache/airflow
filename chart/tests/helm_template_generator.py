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

import subprocess
import sys
from tempfile import NamedTemporaryFile

import yaml
from kubernetes.client.api_client import ApiClient

api_client = ApiClient()


def render_chart(name="RELEASE-NAME", values=None, show_only=None):
    """
    Function that renders a helm chart into dictionaries. For helm chart testing only
    """
    values = values or {}
    with NamedTemporaryFile() as tmp_file:
        content = yaml.dump(values)
        tmp_file.write(content.encode())
        tmp_file.flush()
        command = ["helm", "template", name, sys.path[0], '--values', tmp_file.name]
        if show_only:
            for i in show_only:
                command.extend(["--show-only", i])
        templates = subprocess.check_output(command)
        k8s_objects = yaml.load_all(templates)
        k8s_objects = [k8s_object for k8s_object in k8s_objects if k8s_object]  # type: ignore
        return k8s_objects


def render_k8s_object(obj, type_to_render):
    """
    Function that renders dictionaries into k8s objects. For helm chart testing only.
    """
    return api_client._ApiClient__deserialize_model(obj, type_to_render)  # pylint: disable=W0212

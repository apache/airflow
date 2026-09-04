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

import textwrap

import yaml
from chart_utils.helm_template_generator import prepare_k8s_lookup_dict, render_chart

RELEASE_NAME = "test-extra-objects"


class TestExtraObjects:
    """Tests extra objects."""

    def test_no_extra_objects_by_default(self):
        k8s_objects = render_chart(RELEASE_NAME, show_only=["templates/extra-objects.yaml"])

        assert k8s_objects == []

    def test_extra_objects_as_mapping(self):
        values_str = textwrap.dedent(
            """
            extraObjects:
              - apiVersion: v1
                kind: ConfigMap
                metadata:
                  name: '{{ .Release.Name }}-extra-configmap'
                  namespace: '{{ .Release.Namespace }}'
                data:
                  HELLO_MESSAGE: "Hi!"
            """
        )
        k8s_objects = render_chart(
            RELEASE_NAME,
            values=yaml.safe_load(values_str),
            show_only=["templates/extra-objects.yaml"],
        )
        k8s_objects_by_key = prepare_k8s_lookup_dict(k8s_objects)

        assert set(k8s_objects_by_key.keys()) == {("ConfigMap", f"{RELEASE_NAME}-extra-configmap")}

        configmap_obj = k8s_objects_by_key[("ConfigMap", f"{RELEASE_NAME}-extra-configmap")]
        assert configmap_obj["metadata"]["namespace"] == "default"
        assert configmap_obj["data"] == {"HELLO_MESSAGE": "Hi!"}

    def test_extra_objects_as_string(self):
        values_str = textwrap.dedent(
            """
            extraObjects:
              - |
                apiVersion: v1
                kind: ConfigMap
                metadata:
                  name: {{ .Release.Name }}-extra-configmap
                data:
                  KUBERNETES_NAMESPACE: "{{ .Release.Namespace }}"
            """
        )
        k8s_objects = render_chart(
            RELEASE_NAME,
            values=yaml.safe_load(values_str),
            show_only=["templates/extra-objects.yaml"],
        )
        k8s_objects_by_key = prepare_k8s_lookup_dict(k8s_objects)

        assert set(k8s_objects_by_key.keys()) == {("ConfigMap", f"{RELEASE_NAME}-extra-configmap")}

        configmap_obj = k8s_objects_by_key[("ConfigMap", f"{RELEASE_NAME}-extra-configmap")]
        assert configmap_obj["data"] == {"KUBERNETES_NAMESPACE": "default"}

    def test_multiple_extra_objects(self):
        values_str = textwrap.dedent(
            """
            extraObjects:
              - apiVersion: v1
                kind: ConfigMap
                metadata:
                  name: '{{ .Release.Name }}-first'
                data:
                  KEY: "first"
              - |
                apiVersion: v1
                kind: ConfigMap
                metadata:
                  name: {{ .Release.Name }}-second
                data:
                  KEY: "second"
            """
        )
        k8s_objects = render_chart(
            RELEASE_NAME,
            values=yaml.safe_load(values_str),
            show_only=["templates/extra-objects.yaml"],
        )
        k8s_objects_by_key = prepare_k8s_lookup_dict(k8s_objects)

        assert set(k8s_objects_by_key.keys()) == {
            ("ConfigMap", f"{RELEASE_NAME}-first"),
            ("ConfigMap", f"{RELEASE_NAME}-second"),
        }

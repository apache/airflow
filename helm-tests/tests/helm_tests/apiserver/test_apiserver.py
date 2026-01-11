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

import jmespath
from chart_utils.helm_template_generator import render_chart


class TestAPIServerDeployment:
    """Tests API Server deployment."""

    def test_airflow_2(self):
        """
        API Server only supports Airflow 3.0.0 and later.
        """
        docs = render_chart(
            values={"airflowVersion": "2.10.5"},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        assert len(docs) == 0

    def test_should_not_create_api_server_configmap_when_lower_than_3(self):
        """
        API Server configmap is only created for Airflow 3.0.0 and later.
        """
        docs = render_chart(
            values={"airflowVersion": "2.10.5"},
            show_only=["templates/configmaps/api-server-configmap.yaml"],
        )
        assert len(docs) == 0

    def test_should_add_annotations_to_api_server_configmap(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "apiServer": {
                    "apiServerConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}",
                    "configMapAnnotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/configmaps/api-server-configmap.yaml"],
        )

        assert "annotations" in jmespath.search("metadata", docs[0])
        assert jmespath.search("metadata.annotations", docs[0])["test_annotation"] == "test_annotation_value"

    def test_should_add_volume_and_volume_mount_when_exist_api_server_config(self):
        docs = render_chart(
            values={"apiServer": {"apiServerConfig": "CSRF_ENABLED = True"}, "airflowVersion": "3.0.0"},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert {
            "name": "api-server-config",
            "configMap": {"name": "release-name-api-server-config"},
        } in jmespath.search("spec.template.spec.volumes", docs[0])

        assert {
            "name": "api-server-config",
            "mountPath": "/opt/airflow/webserver_config.py",
            "subPath": "webserver_config.py",
            "readOnly": True,
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])


class TestAPIServerJWTSecret:
    """Tests API Server JWT secret."""

    def test_should_add_annotations_to_jwt_secret(self):
        docs = render_chart(
            values={
                "jwtSecretAnnotations": {"test_annotation": "test_annotation_value"},
            },
            show_only=["templates/secrets/jwt-secret.yaml"],
        )[0]

        assert "annotations" in jmespath.search("metadata", docs)
        assert jmespath.search("metadata.annotations", docs)["test_annotation"] == "test_annotation_value"


class TestApiSecretKeySecret:
    """Tests api secret key secret."""

    def test_should_add_annotations_to_api_secret_key_secret(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "apiSecretAnnotations": {"test_annotation": "test_annotation_value"},
            },
            show_only=["templates/secrets/api-secret-key-secret.yaml"],
        )[0]

        assert "annotations" in jmespath.search("metadata", docs)
        assert jmespath.search("metadata.annotations", docs)["test_annotation"] == "test_annotation_value"


class TestApiserverConfigmap:
    """Tests apiserver configmap."""

    def test_no_apiserver_config_configmap_by_default(self):
        docs = render_chart(show_only=["templates/configmaps/api-server-configmap.yaml"])
        assert len(docs) == 0

    def test_no_apiserver_config_configmap_with_configmap_name(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "apiServerConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}",
                    "apiServerConfigConfigMapName": "my-configmap",
                }
            },
            show_only=["templates/configmaps/api-server-configmap.yaml"],
        )
        assert len(docs) == 0

    def test_apiserver_with_custom_configmap_name(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "apiServerConfigConfigMapName": "my-custom-configmap",
                }
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        assert (
            jmespath.search("spec.template.spec.volumes[1].configMap.name", docs[0]) == "my-custom-configmap"
        )

    def test_apiserver_config_configmap(self):
        docs = render_chart(
            values={"apiServer": {"apiServerConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}"}},
            show_only=["templates/configmaps/api-server-configmap.yaml"],
        )

        assert docs[0]["kind"] == "ConfigMap"
        assert jmespath.search("metadata.name", docs[0]) == "release-name-api-server-config"
        assert (
            jmespath.search('data."webserver_config.py"', docs[0]).strip()
            == "CSRF_ENABLED = True  # release-name"
        )

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

import jmespath
import pytest
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class TestAirflowCommon:
    """
    This class holds tests that apply to more than 1 Airflow component so
    we don't have to repeat tests everywhere

    The one general exception will be the KubernetesExecutor PodTemplateFile,
    as it requires extra test setup.
    """

    @parameterized.expand(
        [
            ({"gitSync": {"enabled": True}}, True),
            ({"persistence": {"enabled": True}}, False),
            (
                {
                    "gitSync": {"enabled": True},
                    "persistence": {"enabled": True},
                },
                True,
            ),
        ]
    )
    def test_dags_mount(self, dag_values, expected_read_only):
        docs = render_chart(
            values={
                "dags": dag_values,
                "airflowVersion": "1.10.15",
            },  # airflowVersion is present so webserver gets the mount
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
            ],
        )

        assert 3 == len(docs)
        for doc in docs:
            expected_mount = {
                "mountPath": "/opt/airflow/dags",
                "name": "dags",
                "readOnly": expected_read_only,
            }
            assert expected_mount in jmespath.search("spec.template.spec.containers[0].volumeMounts", doc)

    def test_annotations(self):
        """
        Test Annotations are correctly applied on all pods created Scheduler, Webserver & Worker
        deployments.
        """
        release_name = "TEST-BASIC"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "airflowVersion": "2.2.0",  # Needed for triggerer to be enabled.
                "airflowPodAnnotations": {"test-annotation/safe-to-evict": "true"},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
            ],
        )

        assert 5 == len(k8s_objects)

        for k8s_object in k8s_objects:
            annotations = k8s_object["spec"]["template"]["metadata"]["annotations"]
            assert "test-annotation/safe-to-evict" in annotations
            assert "true" in annotations["test-annotation/safe-to-evict"]

    @pytest.mark.parametrize(
        "use_default_image,expected_image",
        [
            (True, "apache/airflow:2.1.0"),
            (False, "apache/airflow:user-image"),
        ],
    )
    def test_should_use_correct_image(self, use_default_image, expected_image):
        docs = render_chart(
            values={
                "defaultAirflowRepository": "apache/airflow",
                "defaultAirflowTag": "2.1.0",
                "images": {
                    "airflow": {
                        "repository": "apache/airflow",
                        "tag": "user-image",
                    },
                    "useDefaultImageForMigration": use_default_image,
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
            ],
        )

        for doc in docs:
            assert expected_image == jmespath.search("spec.template.spec.initContainers[0].image", doc)

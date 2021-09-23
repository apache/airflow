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

import unittest

from tests.helm_template_generator import render_chart

# Values for each service mapped to the 'example'
# key annotation
CUSTOM_ANNOTATION_VALUES = (
    CUSTOM_SCHEDULER_ANNOTATION,
    CUSTOM_WEBSERVER_ANNOTATION,
    CUSTOM_WORKER_ANNOTATION,
    CUSTOM_CLEANUP_ANNOTATION,
    CUSTOM_FLOWER_ANNOTATION,
    CUSTOM_PGBOUNCER_ANNOTATION,
    CUSTOM_STATSD_ANNOTATION,
    CUSTOM_CREATE_USER_JOB_ANNOTATION,
    CUSTOM_MIGRATE_DATABASE_JOB_ANNOTATION,
    CUSTOM_REDIS_ANNOTATION,
) = (
    "scheduler",
    "webserver",
    "worker",
    "cleanup",
    "flower",
    "pgbouncer",
    "statsd",
    "createuser",
    "migratedb",
    "redis",
)

COMPONENTS_SUPPORTING_CUSTOM_POD_ANNOTATIONS = (
    "scheduler",
    "webserver",
    "worker",
    "flower",
    "triggerer",
)


class AnnotationsTest(unittest.TestCase):
    def test_service_account_annotations(self):
        k8s_objects = render_chart(
            values={
                "cleanup": {
                    "enabled": True,
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_CLEANUP_ANNOTATION,
                        },
                    },
                },
                "scheduler": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_SCHEDULER_ANNOTATION,
                        },
                    },
                },
                "webserver": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_WEBSERVER_ANNOTATION,
                        },
                    },
                },
                "workers": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_WORKER_ANNOTATION,
                        },
                    },
                },
                "flower": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_FLOWER_ANNOTATION,
                        },
                    },
                },
                "statsd": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_STATSD_ANNOTATION,
                        },
                    },
                },
                "redis": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_REDIS_ANNOTATION,
                        },
                    },
                },
                "pgbouncer": {
                    "enabled": True,
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_PGBOUNCER_ANNOTATION,
                        },
                    },
                },
                "createUserJob": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_CREATE_USER_JOB_ANNOTATION,
                        },
                    },
                },
                "migrateDatabaseJob": {
                    "serviceAccount": {
                        "annotations": {
                            "example": CUSTOM_MIGRATE_DATABASE_JOB_ANNOTATION,
                        },
                    },
                },
                "executor": "CeleryExecutor",  # create worker deployment
            },
        )

        list_of_annotation_values_in_objects = [
            k8s_object['metadata']['annotations']['example']
            for k8s_object in k8s_objects
            if k8s_object['kind'] == "ServiceAccount"
        ]

        self.assertCountEqual(
            list_of_annotation_values_in_objects,
            CUSTOM_ANNOTATION_VALUES,
        )

    def test_per_component_custom_annotations(self):
        release_name = "RELEASE_NAME"

        k8s_objects = render_chart(
            name=release_name,
            values={
                "scheduler": {
                    "podAnnotations": {
                        "example": "scheduler",
                    },
                },
                "webserver": {
                    "podAnnotations": {
                        "example": "webserver",
                    },
                },
                "workers": {
                    "podAnnotations": {
                        "example": "worker",
                    },
                },
                "flower": {
                    "podAnnotations": {
                        "example": "flower",
                    },
                },
                "airflowVersion": "2.2.0",  # Needed for triggerer to be enabled.
                "triggerer": {
                    "podAnnotations": {
                        "example": "triggerer",
                    },
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
            ],
        )

        # The test relies on the convention that the Deployment name
        # is always `{ Release.Name }-<component_name>`.
        obj_by_component_name = {
            obj["metadata"]["name"].replace(release_name + "-", ""): obj for obj in k8s_objects
        }

        self.assertCountEqual(obj_by_component_name, COMPONENTS_SUPPORTING_CUSTOM_POD_ANNOTATIONS)

        for component_name, obj in obj_by_component_name.items():
            self.assertIn("example", obj["spec"]["template"]["metadata"]["annotations"])
            self.assertEqual(component_name, obj["spec"]["template"]["metadata"]["annotations"]["example"])

    def test_per_component_custom_annotations_precedence(self):
        release_name = "RELEASE_NAME"

        k8s_objects = render_chart(
            name=release_name,
            values={
                "airflowPodAnnotations": {"example": "GLOBAL"},
                "scheduler": {
                    "podAnnotations": {
                        "example": "scheduler",
                    },
                },
                "webserver": {
                    "podAnnotations": {
                        "example": "webserver",
                    },
                },
                "workers": {
                    "podAnnotations": {
                        "example": "worker",
                    },
                },
                "flower": {
                    "podAnnotations": {
                        "example": "flower",
                    },
                },
                "airflowVersion": "2.2.0",  # Needed for triggerer to be enabled.
                "triggerer": {
                    "podAnnotations": {
                        "example": "triggerer",
                    },
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
            ],
        )

        # The test relies on the convention that the Deployment name
        # is always `{ Release.Name }-<component_name>`.
        obj_by_component_name = {
            obj["metadata"]["name"].replace(release_name + "-", ""): obj for obj in k8s_objects
        }

        self.assertCountEqual(obj_by_component_name, COMPONENTS_SUPPORTING_CUSTOM_POD_ANNOTATIONS)

        for component_name, obj in obj_by_component_name.items():
            self.assertIn("example", obj["spec"]["template"]["metadata"]["annotations"])
            self.assertEqual(component_name, obj["spec"]["template"]["metadata"]["annotations"]["example"])

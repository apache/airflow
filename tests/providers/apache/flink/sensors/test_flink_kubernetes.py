#
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
#

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from kubernetes.client import V1ObjectMeta, V1Pod, V1PodList
from kubernetes.client.rest import ApiException

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Connection
from airflow.providers.apache.flink.sensors.flink_kubernetes import FlinkKubernetesSensor
from airflow.utils import db, timezone

pytestmark = pytest.mark.db_test


TEST_NO_STATE_CLUSTER = {
    "apiVersion": "flink.apache.org/v1beta1",
    "kind": "FlinkDeployment",
    "metadata": {
        "creationTimestamp": "2022-09-25T16:47:16Z",
        "generation": 1,
        "managedFields": [
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:spec": {
                        ".": {},
                        "f:flinkConfiguration": {
                            ".": {},
                            "f:taskmanager.numberOfTaskSlots": {},
                        },
                        "f:flinkVersion": {},
                        "f:image": {},
                        "f:ingress": {".": {}, "f:template": {}},
                        "f:job": {
                            ".": {},
                            "f:jarURI": {},
                            "f:parallelism": {},
                            "f:state": {},
                            "f:upgradeMode": {},
                        },
                        "f:jobManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                        "f:logConfiguration": {
                            ".": {},
                            "f:log4j-console.properties": {},
                        },
                        "f:serviceAccount": {},
                        "f:taskManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                    }
                },
                "manager": "OpenAPI-Generator",
                "operation": "Update",
                "time": "2022-09-25T16:47:16Z",
            }
        ],
        "name": "flink-stream-example",
        "namespace": "default",
        "resourceVersion": "140016",
        "uid": "925eb6a0-f336-40e9-a08e-880481c73729",
    },
    "spec": {
        "flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"},
        "flinkVersion": "v1_15",
        "image": "flink:1.15",
        "ingress": {"template": "{{name}}.{{namespace}}.flink.k8s.io"},
        "job": {
            "jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
            "parallelism": 2,
            "state": "running",
            "upgradeMode": "stateless",
        },
        "jobManager": {"resource": {"cpu": 1, "memory": "2048m"}},
        "logConfiguration": {"log4j-console.properties": "rootLogger.level = DEBUG"},
        "serviceAccount": "flink",
        "taskManager": {"resource": {"cpu": 1, "memory": "2048m"}},
    },
}

TEST_DEPLOYING_CLUSTER = {
    "apiVersion": "flink.apache.org/v1beta1",
    "kind": "FlinkDeployment",
    "metadata": {
        "creationTimestamp": "2022-09-25T16:47:16Z",
        "finalizers": ["flinkdeployments.flink.apache.org/finalizer"],
        "generation": 2,
        "managedFields": [
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:spec": {
                        ".": {},
                        "f:flinkConfiguration": {
                            ".": {},
                            "f:taskmanager.numberOfTaskSlots": {},
                        },
                        "f:flinkVersion": {},
                        "f:image": {},
                        "f:ingress": {".": {}, "f:template": {}},
                        "f:job": {
                            ".": {},
                            "f:jarURI": {},
                            "f:parallelism": {},
                            "f:state": {},
                            "f:upgradeMode": {},
                        },
                        "f:jobManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                        "f:logConfiguration": {
                            ".": {},
                            "f:log4j-console.properties": {},
                        },
                        "f:serviceAccount": {},
                        "f:taskManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                    }
                },
                "manager": "OpenAPI-Generator",
                "operation": "Update",
                "time": "2022-09-25T16:47:16Z",
            },
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {
                        "f:finalizers": {
                            ".": {},
                            'v:"flinkdeployments.flink.apache.org/finalizer"': {},
                        }
                    },
                    "f:spec": {
                        "f:job": {"f:args": {}},
                        "f:jobManager": {"f:replicas": {}},
                    },
                    "f:status": {
                        ".": {},
                        "f:clusterInfo": {},
                        "f:error": {},
                        "f:jobManagerDeploymentStatus": {},
                        "f:jobStatus": {
                            ".": {},
                            "f:savepointInfo": {
                                ".": {},
                                "f:lastPeriodicSavepointTimestamp": {},
                                "f:savepointHistory": {},
                                "f:triggerId": {},
                                "f:triggerTimestamp": {},
                                "f:triggerType": {},
                            },
                            "f:state": {},
                        },
                        "f:reconciliationStatus": {
                            ".": {},
                            "f:lastReconciledSpec": {},
                            "f:reconciliationTimestamp": {},
                            "f:state": {},
                        },
                        "f:taskManager": {
                            ".": {},
                            "f:labelSelector": {},
                            "f:replicas": {},
                        },
                    },
                },
                "manager": "okhttp",
                "operation": "Update",
                "time": "2022-09-25T16:47:16Z",
            },
        ],
        "name": "flink-stream-example",
        "namespace": "default",
        "resourceVersion": "140043",
        "uid": "925eb6a0-f336-40e9-a08e-880481c73729",
    },
    "spec": {
        "flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"},
        "flinkVersion": "v1_15",
        "image": "flink:1.15",
        "ingress": {"template": "{{name}}.{{namespace}}.flink.k8s.io"},
        "job": {
            "args": [],
            "jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
            "parallelism": 2,
            "state": "running",
            "upgradeMode": "stateless",
        },
        "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2048m"}},
        "logConfiguration": {"log4j-console.properties": "rootLogger.level = DEBUG"},
        "serviceAccount": "flink",
        "taskManager": {"resource": {"cpu": 1, "memory": "2048m"}},
    },
    "status": {
        "clusterInfo": {},
        "error": "",
        "jobManagerDeploymentStatus": "DEPLOYING",
        "jobStatus": {
            "savepointInfo": {
                "lastPeriodicSavepointTimestamp": 0,
                "savepointHistory": [],
                "triggerId": "",
                "triggerTimestamp": 0,
                "triggerType": "UNKNOWN",
            },
            "state": "RECONCILING",
        },
        "reconciliationStatus": {
            "lastReconciledSpec": '{"spec":{"job":{"jarURI":"local:///opt/flink/examples/streaming/StateMachineExample.jar","parallelism":2,"entryClass":null,"args":[],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"upgradeMode":"stateless","allowNonRestoredState":null},"restartNonce":null,"flinkConfiguration":{"taskmanager.numberOfTaskSlots":"2"},"image":"flink:1.15","imagePullPolicy":null,"serviceAccount":"flink","flinkVersion":"v1_15","ingress":{"template":"{{name}}.{{namespace}}.flink.k8s.io","className":null,"annotations":null},"podTemplate":null,"jobManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":null,"podTemplate":null},"logConfiguration":{"log4j-console.properties":"rootLogger.level = DEBUG"}},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","metadata":{"generation":2}}}',
            "reconciliationTimestamp": 1664124436834,
            "state": "DEPLOYED",
        },
        "taskManager": {
            "labelSelector": "component=taskmanager,app=flink-stream-example",
            "replicas": 1,
        },
    },
}

TEST_DEPLOYED_NOT_READY_CLUSTER = {
    "apiVersion": "flink.apache.org/v1beta1",
    "kind": "FlinkDeployment",
    "metadata": {
        "creationTimestamp": "2022-09-25T16:47:16Z",
        "finalizers": ["flinkdeployments.flink.apache.org/finalizer"],
        "generation": 2,
        "managedFields": [
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:spec": {
                        ".": {},
                        "f:flinkConfiguration": {
                            ".": {},
                            "f:taskmanager.numberOfTaskSlots": {},
                        },
                        "f:flinkVersion": {},
                        "f:image": {},
                        "f:ingress": {".": {}, "f:template": {}},
                        "f:job": {
                            ".": {},
                            "f:jarURI": {},
                            "f:parallelism": {},
                            "f:state": {},
                            "f:upgradeMode": {},
                        },
                        "f:jobManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                        "f:logConfiguration": {
                            ".": {},
                            "f:log4j-console.properties": {},
                        },
                        "f:serviceAccount": {},
                        "f:taskManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                    }
                },
                "manager": "OpenAPI-Generator",
                "operation": "Update",
                "time": "2022-09-25T16:47:16Z",
            },
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {
                        "f:finalizers": {
                            ".": {},
                            'v:"flinkdeployments.flink.apache.org/finalizer"': {},
                        }
                    },
                    "f:spec": {
                        "f:job": {"f:args": {}},
                        "f:jobManager": {"f:replicas": {}},
                    },
                    "f:status": {
                        ".": {},
                        "f:clusterInfo": {},
                        "f:error": {},
                        "f:jobManagerDeploymentStatus": {},
                        "f:jobStatus": {
                            ".": {},
                            "f:savepointInfo": {
                                ".": {},
                                "f:lastPeriodicSavepointTimestamp": {},
                                "f:savepointHistory": {},
                                "f:triggerId": {},
                                "f:triggerTimestamp": {},
                                "f:triggerType": {},
                            },
                            "f:state": {},
                        },
                        "f:reconciliationStatus": {
                            ".": {},
                            "f:lastReconciledSpec": {},
                            "f:reconciliationTimestamp": {},
                            "f:state": {},
                        },
                        "f:taskManager": {
                            ".": {},
                            "f:labelSelector": {},
                            "f:replicas": {},
                        },
                    },
                },
                "manager": "okhttp",
                "operation": "Update",
                "time": "2022-09-25T16:47:16Z",
            },
        ],
        "name": "flink-stream-example",
        "namespace": "default",
        "resourceVersion": "140061",
        "uid": "925eb6a0-f336-40e9-a08e-880481c73729",
    },
    "spec": {
        "flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"},
        "flinkVersion": "v1_15",
        "image": "flink:1.15",
        "ingress": {"template": "{{name}}.{{namespace}}.flink.k8s.io"},
        "job": {
            "args": [],
            "jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
            "parallelism": 2,
            "state": "running",
            "upgradeMode": "stateless",
        },
        "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2048m"}},
        "logConfiguration": {"log4j-console.properties": "rootLogger.level = DEBUG"},
        "serviceAccount": "flink",
        "taskManager": {"resource": {"cpu": 1, "memory": "2048m"}},
    },
    "status": {
        "clusterInfo": {},
        "error": "",
        "jobManagerDeploymentStatus": "DEPLOYED_NOT_READY",
        "jobStatus": {
            "savepointInfo": {
                "lastPeriodicSavepointTimestamp": 0,
                "savepointHistory": [],
                "triggerId": "",
                "triggerTimestamp": 0,
                "triggerType": "UNKNOWN",
            },
            "state": "RECONCILING",
        },
        "reconciliationStatus": {
            "lastReconciledSpec": '{"spec":{"job":{"jarURI":"local:///opt/flink/examples/streaming/StateMachineExample.jar","parallelism":2,"entryClass":null,"args":[],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"upgradeMode":"stateless","allowNonRestoredState":null},"restartNonce":null,"flinkConfiguration":{"taskmanager.numberOfTaskSlots":"2"},"image":"flink:1.15","imagePullPolicy":null,"serviceAccount":"flink","flinkVersion":"v1_15","ingress":{"template":"{{name}}.{{namespace}}.flink.k8s.io","className":null,"annotations":null},"podTemplate":null,"jobManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":null,"podTemplate":null},"logConfiguration":{"log4j-console.properties":"rootLogger.level = DEBUG"}},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","metadata":{"generation":2}}}',
            "reconciliationTimestamp": 1664124436834,
            "state": "DEPLOYED",
        },
        "taskManager": {
            "labelSelector": "component=taskmanager,app=flink-stream-example",
            "replicas": 1,
        },
    },
}

TEST_ERROR_CLUSTER = {
    "apiVersion": "flink.apache.org/v1beta1",
    "kind": "FlinkDeployment",
    "metadata": {
        "creationTimestamp": "2022-09-25T16:40:30Z",
        "finalizers": ["flinkdeployments.flink.apache.org/finalizer"],
        "generation": 2,
        "managedFields": [
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:spec": {
                        ".": {},
                        "f:flinkConfiguration": {
                            ".": {},
                            "f:high-availability": {},
                            "f:high-availability.storageDir": {},
                            "f:state.checkpoints.dir": {},
                            "f:state.savepoints.dir": {},
                            "f:taskmanager.numberOfTaskSlots": {},
                        },
                        "f:flinkVersion": {},
                        "f:image": {},
                        "f:ingress": {".": {}, "f:template": {}},
                        "f:job": {
                            ".": {},
                            "f:jarURI": {},
                            "f:parallelism": {},
                            "f:savepointTriggerNonce": {},
                            "f:state": {},
                            "f:upgradeMode": {},
                        },
                        "f:jobManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                        "f:serviceAccount": {},
                        "f:taskManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                    }
                },
                "manager": "OpenAPI-Generator",
                "operation": "Update",
                "time": "2022-09-25T16:40:30Z",
            },
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {
                        "f:finalizers": {
                            ".": {},
                            'v:"flinkdeployments.flink.apache.org/finalizer"': {},
                        }
                    },
                    "f:spec": {
                        "f:job": {"f:args": {}},
                        "f:jobManager": {"f:replicas": {}},
                    },
                    "f:status": {
                        ".": {},
                        "f:clusterInfo": {},
                        "f:error": {},
                        "f:jobManagerDeploymentStatus": {},
                        "f:jobStatus": {
                            ".": {},
                            "f:savepointInfo": {
                                ".": {},
                                "f:lastPeriodicSavepointTimestamp": {},
                                "f:savepointHistory": {},
                                "f:triggerId": {},
                                "f:triggerTimestamp": {},
                                "f:triggerType": {},
                            },
                            "f:state": {},
                        },
                        "f:reconciliationStatus": {
                            ".": {},
                            "f:lastReconciledSpec": {},
                            "f:reconciliationTimestamp": {},
                            "f:state": {},
                        },
                        "f:taskManager": {
                            ".": {},
                            "f:labelSelector": {},
                            "f:replicas": {},
                        },
                    },
                },
                "manager": "okhttp",
                "operation": "Update",
                "time": "2022-09-25T16:40:30Z",
            },
        ],
        "name": "flink-stream-example",
        "namespace": "default",
        "resourceVersion": "139635",
        "uid": "4b18c5c0-38a4-4a7a-8c8f-036e47774c12",
    },
    "spec": {
        "flinkConfiguration": {
            "high-availability": "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory",
            "high-availability.storageDir": "file:///flink-data/ha",
            "state.checkpoints.dir": "file:///flink-data/checkpoints",
            "state.savepoints.dir": "file:///flink-data/savepoints",
            "taskmanager.numberOfTaskSlots": "2",
        },
        "flinkVersion": "v1_15",
        "image": "flink:1.15",
        "ingress": {"template": "{{name}}.{{namespace}}.flink.k8s.io"},
        "job": {
            "args": [],
            "jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
            "parallelism": 2,
            "savepointTriggerNonce": 0,
            "state": "running",
            "upgradeMode": "stateless",
        },
        "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2048m"}},
        "serviceAccount": "flink",
        "taskManager": {"resource": {"cpu": 1, "memory": "2048m"}},
    },
    "status": {
        "clusterInfo": {},
        "error": "back-off 20s restarting failed container=flink-main-container"
        " pod=flink-stream-example-c6c65d85b-ffbs7_default(ff52c4fd-88b6-468f-9914-c2a22c11b7a9)",
        "jobManagerDeploymentStatus": "ERROR",
        "jobStatus": {
            "savepointInfo": {
                "lastPeriodicSavepointTimestamp": 0,
                "savepointHistory": [],
                "triggerId": "",
                "triggerTimestamp": 0,
                "triggerType": "UNKNOWN",
            },
            "state": "RECONCILING",
        },
        "reconciliationStatus": {
            "lastReconciledSpec": '{"spec":{"job":{"jarURI":"local:///opt/flink/examples/streaming/StateMachineExample.jar","parallelism":2,"entryClass":null,"args":[],"state":"running","savepointTriggerNonce":0,"initialSavepointPath":null,"upgradeMode":"stateless","allowNonRestoredState":null},"restartNonce":null,"flinkConfiguration":{"high-availability":"org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory","high-availability.storageDir":"file:///flink-data/ha","state.checkpoints.dir":"file:///flink-data/checkpoints","state.savepoints.dir":"file:///flink-data/savepoints","taskmanager.numberOfTaskSlots":"2"},"image":"flink:1.15","imagePullPolicy":null,"serviceAccount":"flink","flinkVersion":"v1_15","ingress":{"template":"{{name}}.{{namespace}}.flink.k8s.io","className":null,"annotations":null},"podTemplate":null,"jobManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":null,"podTemplate":null},"logConfiguration":null},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","metadata":{"generation":2}}}',
            "reconciliationTimestamp": 1664124030853,
            "state": "DEPLOYED",
        },
        "taskManager": {
            "labelSelector": "component=taskmanager,app=flink-stream-example",
            "replicas": 1,
        },
    },
}

TEST_READY_CLUSTER = {
    "apiVersion": "flink.apache.org/v1beta1",
    "kind": "FlinkDeployment",
    "metadata": {
        "creationTimestamp": "2022-09-25T16:47:16Z",
        "finalizers": ["flinkdeployments.flink.apache.org/finalizer"],
        "generation": 2,
        "managedFields": [
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:spec": {
                        ".": {},
                        "f:flinkConfiguration": {
                            ".": {},
                            "f:taskmanager.numberOfTaskSlots": {},
                        },
                        "f:flinkVersion": {},
                        "f:image": {},
                        "f:ingress": {".": {}, "f:template": {}},
                        "f:job": {
                            ".": {},
                            "f:jarURI": {},
                            "f:parallelism": {},
                            "f:state": {},
                            "f:upgradeMode": {},
                        },
                        "f:jobManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                        "f:logConfiguration": {
                            ".": {},
                            "f:log4j-console.properties": {},
                        },
                        "f:serviceAccount": {},
                        "f:taskManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                    }
                },
                "manager": "OpenAPI-Generator",
                "operation": "Update",
                "time": "2022-09-25T16:47:16Z",
            },
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {
                        "f:finalizers": {
                            ".": {},
                            'v:"flinkdeployments.flink.apache.org/finalizer"': {},
                        }
                    },
                    "f:spec": {
                        "f:job": {"f:args": {}},
                        "f:jobManager": {"f:replicas": {}},
                    },
                    "f:status": {
                        ".": {},
                        "f:clusterInfo": {
                            ".": {},
                            "f:flink-revision": {},
                            "f:flink-version": {},
                        },
                        "f:error": {},
                        "f:jobManagerDeploymentStatus": {},
                        "f:jobStatus": {
                            ".": {},
                            "f:jobId": {},
                            "f:jobName": {},
                            "f:savepointInfo": {
                                ".": {},
                                "f:lastPeriodicSavepointTimestamp": {},
                                "f:savepointHistory": {},
                                "f:triggerId": {},
                                "f:triggerTimestamp": {},
                                "f:triggerType": {},
                            },
                            "f:startTime": {},
                            "f:state": {},
                            "f:updateTime": {},
                        },
                        "f:reconciliationStatus": {
                            ".": {},
                            "f:lastReconciledSpec": {},
                            "f:reconciliationTimestamp": {},
                            "f:state": {},
                        },
                        "f:taskManager": {
                            ".": {},
                            "f:labelSelector": {},
                            "f:replicas": {},
                        },
                    },
                },
                "manager": "okhttp",
                "operation": "Update",
                "time": "2022-09-25T16:47:34Z",
            },
        ],
        "name": "flink-stream-example",
        "namespace": "default",
        "resourceVersion": "140076",
        "uid": "925eb6a0-f336-40e9-a08e-880481c73729",
    },
    "spec": {
        "flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"},
        "flinkVersion": "v1_15",
        "image": "flink:1.15",
        "ingress": {"template": "{{name}}.{{namespace}}.flink.k8s.io"},
        "job": {
            "args": [],
            "jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
            "parallelism": 2,
            "state": "running",
            "upgradeMode": "stateless",
        },
        "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2048m"}},
        "logConfiguration": {"log4j-console.properties": "rootLogger.level = DEBUG"},
        "serviceAccount": "flink",
        "taskManager": {"resource": {"cpu": 1, "memory": "2048m"}},
    },
    "status": {
        "clusterInfo": {
            "flink-revision": "f494be6 @ 2022-06-20T14:40:28 02:00",
            "flink-version": "1.15.1",
        },
        "error": "",
        "jobManagerDeploymentStatus": "READY",
        "jobStatus": {
            "jobId": "5d6feb14e05a3b00cbc55c2c8331fb7f",
            "jobName": "State machine job",
            "savepointInfo": {
                "lastPeriodicSavepointTimestamp": 0,
                "savepointHistory": [],
                "triggerId": "",
                "triggerTimestamp": 0,
                "triggerType": "UNKNOWN",
            },
            "startTime": "1664124442817",
            "state": "CREATED",
            "updateTime": "1664124454261",
        },
        "reconciliationStatus": {
            "lastReconciledSpec": '{"spec":{"job":{"jarURI":"local:///opt/flink/examples/streaming/StateMachineExample.jar","parallelism":2,"entryClass":null,"args":[],"state":"running","savepointTriggerNonce":null,"initialSavepointPath":null,"upgradeMode":"stateless","allowNonRestoredState":null},"restartNonce":null,"flinkConfiguration":{"taskmanager.numberOfTaskSlots":"2"},"image":"flink:1.15","imagePullPolicy":null,"serviceAccount":"flink","flinkVersion":"v1_15","ingress":{"template":"{{name}}.{{namespace}}.flink.k8s.io","className":null,"annotations":null},"podTemplate":null,"jobManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":null,"podTemplate":null},"logConfiguration":{"log4j-console.properties":"rootLogger.level = DEBUG"}},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","metadata":{"generation":2}}}',
            "reconciliationTimestamp": 1664124436834,
            "state": "DEPLOYED",
        },
        "taskManager": {
            "labelSelector": "component=taskmanager,app=flink-stream-example",
            "replicas": 1,
        },
    },
}

TEST_MISSING_CLUSTER = {
    "apiVersion": "flink.apache.org/v1beta1",
    "kind": "FlinkDeployment",
    "metadata": {
        "creationTimestamp": "2022-09-25T18:49:59Z",
        "finalizers": ["flinkdeployments.flink.apache.org/finalizer"],
        "generation": 2,
        "managedFields": [
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:spec": {
                        ".": {},
                        "f:flinkConfiguration": {
                            ".": {},
                            "f:taskmanager.numberOfTaskSlots": {},
                        },
                        "f:flinkVersion": {},
                        "f:image": {},
                        "f:ingress": {".": {}, "f:template": {}},
                        "f:job": {
                            ".": {},
                            "f:jarURI": {},
                            "f:parallelism": {},
                            "f:state": {},
                            "f:upgradeMode": {},
                        },
                        "f:jobManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                        "f:logConfiguration": {
                            ".": {},
                            "f:log4j-console.properties": {},
                        },
                        "f:serviceAccount": {},
                        "f:taskManager": {
                            ".": {},
                            "f:resource": {".": {}, "f:cpu": {}, "f:memory": {}},
                        },
                    }
                },
                "manager": "OpenAPI-Generator",
                "operation": "Update",
                "time": "2022-09-25T18:49:59Z",
            },
            {
                "apiVersion": "flink.apache.org/v1beta1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:metadata": {
                        "f:finalizers": {
                            ".": {},
                            'v:"flinkdeployments.flink.apache.org/finalizer"': {},
                        }
                    },
                    "f:spec": {
                        "f:job": {"f:args": {}},
                        "f:jobManager": {"f:replicas": {}},
                    },
                    "f:status": {
                        ".": {},
                        "f:clusterInfo": {},
                        "f:error": {},
                        "f:jobManagerDeploymentStatus": {},
                        "f:jobStatus": {
                            ".": {},
                            "f:savepointInfo": {
                                ".": {},
                                "f:lastPeriodicSavepointTimestamp": {},
                                "f:savepointHistory": {},
                                "f:triggerId": {},
                                "f:triggerTimestamp": {},
                                "f:triggerType": {},
                            },
                        },
                        "f:reconciliationStatus": {
                            ".": {},
                            "f:lastReconciledSpec": {},
                            "f:reconciliationTimestamp": {},
                            "f:state": {},
                        },
                        "f:taskManager": {
                            ".": {},
                            "f:labelSelector": {},
                            "f:replicas": {},
                        },
                    },
                },
                "manager": "okhttp",
                "operation": "Update",
                "time": "2022-09-25T18:50:00Z",
            },
        ],
        "name": "flink-sm-ex1",
        "namespace": "default",
        "resourceVersion": "145575",
        "uid": "6ccd47cf-c763-496e-b02e-28146b628646",
    },
    "spec": {
        "flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"},
        "flinkVersion": "v1_15",
        "image": "flink:1.15",
        "ingress": {"template": "{{name}}.{{namespace}}.flink.k8s.io"},
        "job": {
            "args": [],
            "jarURI": "local:///opt/flink/examples/streaming/TopSpeedWindowing.jar",
            "parallelism": 4,
            "state": "running",
            "upgradeMode": "stateless",
        },
        "jobManager": {"replicas": 1, "resource": {"cpu": 1, "memory": "2048m"}},
        "logConfiguration": {"log4j-console.properties": "rootLogger.level = DEBUG\n"},
        "serviceAccount": "flink",
        "taskManager": {"resource": {"cpu": 1, "memory": "2048m"}},
    },
    "status": {
        "clusterInfo": {},
        "error": "",
        "jobManagerDeploymentStatus": "MISSING",
        "jobStatus": {
            "savepointInfo": {
                "lastPeriodicSavepointTimestamp": 0,
                "savepointHistory": [],
                "triggerId": "",
                "triggerTimestamp": 0,
                "triggerType": "UNKNOWN",
            }
        },
        "reconciliationStatus": {
            "lastReconciledSpec": '{"spec":{"job":{"jarURI":"local:///opt/flink/examples/streaming/TopSpeedWindowing.jar","parallelism":4,"entryClass":null,"args":[],"state":"suspended","savepointTriggerNonce":null,"initialSavepointPath":null,"upgradeMode":"stateless","allowNonRestoredState":null},"restartNonce":null,"flinkConfiguration":{"taskmanager.numberOfTaskSlots":"2"},"image":"flink:1.15","imagePullPolicy":null,"serviceAccount":"flink","flinkVersion":"v1_15","ingress":{"template":"{{name}}.{{namespace}}.flink.k8s.io","className":null,"annotations":null},"podTemplate":null,"jobManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":1,"podTemplate":null},"taskManager":{"resource":{"cpu":1.0,"memory":"2048m"},"replicas":null,"podTemplate":null},"logConfiguration":{"log4j-console.properties":"rootLogger.level = DEBUG\\n"}},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","metadata":{"generation":2}}}',
            "reconciliationTimestamp": 1664131800071,
            "state": "UPGRADING",
        },
        "taskManager": {"labelSelector": "", "replicas": 0},
    },
}

TEST_POD_LOGS = [
    b"2022-09-25 19:01:47,027 INFO  KubernetesTaskExecutorRunner [] - ---------\n",
    b"Successful registration at resource manager akka.tcp://flink@basic-example.default:6\n",
    b"Receive slot request 80a7ef3e8cc1a2c09b4e3b2fbd70b28d for job 363eed78e59\n",
    b"Checkpoint storage is set to 'jobmanager'\n",
    b"Flat Map -> Sink: Print to Std. Out (2/2)#0 (e7492dc4a) switched to RUNNING.",
]
TEST_POD_LOG_RESULT = (
    "2022-09-25 19:01:47,027 INFO  KubernetesTaskExecutorRunner [] - ---------\n"
    "Successful registration at resource manager akka.tcp://flink@basic-example.default:6\n"
    "Receive slot request 80a7ef3e8cc1a2c09b4e3b2fbd70b28d for job 363eed78e59\n"
    "Checkpoint storage is set to 'jobmanager'\n"
    "Flat Map -> Sink: Print to Std. Out (2/2)#0 (e7492dc4a) switched to RUNNING."
)

TASK_MANAGER_POD = V1Pod(
    api_version="V1", metadata=V1ObjectMeta(namespace="default", name="basic-example-taskmanager-1-1")
)
TASK_MANAGER_POD_LIST = V1PodList(api_version="v1", items=[TASK_MANAGER_POD], kind="PodList")


@patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_conn")
class TestFlinkKubernetesSensor:
    def setup_method(self):
        db.merge_conn(Connection(conn_id="kubernetes_default", conn_type="kubernetes", extra=json.dumps({})))
        db.merge_conn(
            Connection(
                conn_id="kubernetes_default",
                conn_type="kubernetes",
                extra=json.dumps({}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id="kubernetes_with_namespace",
                conn_type="kubernetes",
                extra=json.dumps({"extra__kubernetes__namespace": "mock_namespace"}),
            )
        )
        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_READY_CLUSTER,
    )
    def test_cluster_ready_state(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example", dag=self.dag, task_id="test_task_id"
        )
        assert sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_ERROR_CLUSTER,
    )
    def test_cluster_error_state(
        self, mock_get_namespaced_crd, mock_kubernetes_hook, soft_fail, expected_exception
    ):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example", dag=self.dag, task_id="test_task_id", soft_fail=soft_fail
        )
        with pytest.raises(expected_exception):
            sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_NO_STATE_CLUSTER,
    )
    def test_new_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example", dag=self.dag, task_id="test_task_id"
        )

        assert not sensor.poke(None)

        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_DEPLOYED_NOT_READY_CLUSTER,
    )
    def test_deployed_not_ready_cluster(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example", dag=self.dag, task_id="test_task_id"
        )
        assert not sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_DEPLOYING_CLUSTER,
    )
    def test_deploying_cluster(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example", dag=self.dag, task_id="test_task_id"
        )
        assert not sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_MISSING_CLUSTER,
    )
    def test_missing_cluster(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example", dag=self.dag, task_id="test_task_id"
        )
        with pytest.raises(AirflowException):
            sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_READY_CLUSTER,
    )
    def test_namespace_from_sensor(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example",
            dag=self.dag,
            kubernetes_conn_id="kubernetes_with_namespace",
            namespace="sensor_namespace",
            task_id="test_task_id",
        )
        sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="sensor_namespace",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_READY_CLUSTER,
    )
    def test_api_group_and_version_from_sensor(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        api_group = "flink.apache.org"
        api_version = "v1beta1"
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example",
            dag=self.dag,
            kubernetes_conn_id="kubernetes_with_namespace",
            task_id="test_task_id",
            api_group=api_group,
            api_version=api_version,
        )
        sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group=api_group,
            name="flink-stream-example",
            namespace="mock_namespace",
            plural="flinkdeployments",
            version=api_version,
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_READY_CLUSTER,
    )
    def test_namespace_from_connection(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example",
            dag=self.dag,
            kubernetes_conn_id="kubernetes_with_namespace",
            task_id="test_task_id",
        )
        sensor.poke(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            name="flink-stream-example",
            namespace="mock_namespace",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_ERROR_CLUSTER,
    )
    @patch("logging.Logger.error")
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod_logs",
        return_value=TEST_POD_LOGS,
    )
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_namespaced_pod_list",
        return_value=TASK_MANAGER_POD_LIST,
    )
    def test_driver_logging_failure(
        self, mock_namespaced_pod_list, mock_pod_logs, error_log_call, mock_namespaced_crd, mock_kube_conn
    ):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example",
            attach_log=True,
            dag=self.dag,
            task_id="test_task_id",
        )
        with pytest.raises(AirflowException):
            sensor.poke(None)
        mock_namespaced_pod_list.assert_called_once_with(
            namespace="default", watch=False, label_selector="component=taskmanager,app=flink-stream-example"
        )
        mock_pod_logs.assert_called_once_with("basic-example-taskmanager-1-1", namespace="default")
        error_log_call.assert_called_once_with(TEST_POD_LOG_RESULT)
        mock_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            version="v1beta1",
            namespace="default",
            plural="flinkdeployments",
            name="flink-stream-example",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_READY_CLUSTER,
    )
    @patch("logging.Logger.info")
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod_logs",
        return_value=TEST_POD_LOGS,
    )
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_namespaced_pod_list",
        return_value=TASK_MANAGER_POD_LIST,
    )
    def test_driver_logging_completed(
        self, mock_namespaced_pod_list, mock_pod_logs, info_log_call, mock_namespaced_crd, mock_kube_conn
    ):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example",
            attach_log=True,
            dag=self.dag,
            task_id="test_task_id",
        )
        sensor.poke(None)

        mock_namespaced_pod_list.assert_called_once_with(
            namespace="default", watch=False, label_selector="component=taskmanager,app=flink-stream-example"
        )
        mock_pod_logs.assert_called_once_with("basic-example-taskmanager-1-1", namespace="default")
        log_info_call = info_log_call.mock_calls[4]
        log_value = log_info_call[1][0]

        assert log_value == TEST_POD_LOG_RESULT

        mock_namespaced_crd.assert_called_once_with(
            group="flink.apache.org",
            version="v1beta1",
            namespace="default",
            plural="flinkdeployments",
            name="flink-stream-example",
        )

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_READY_CLUSTER,
    )
    @patch("logging.Logger.warning")
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod_logs",
        side_effect=ApiException("Test api exception"),
    )
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_namespaced_pod_list",
        return_value=TASK_MANAGER_POD_LIST,
    )
    def test_driver_logging_error(
        self, mock_namespaced_pod_list, mock_pod_logs, warn_log_call, mock_namespaced_crd, mock_kube_conn
    ):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example",
            attach_log=True,
            dag=self.dag,
            task_id="test_task_id",
        )
        sensor.poke(None)
        warn_log_call.assert_called_once()

    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_ERROR_CLUSTER,
    )
    @patch("logging.Logger.warning")
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_pod_logs",
        side_effect=ApiException("Test api exception"),
    )
    @patch(
        "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_namespaced_pod_list",
        return_value=TASK_MANAGER_POD_LIST,
    )
    def test_driver_logging_error_missing_state(
        self, mock_namespaced_pod_list, mock_pod_logs, warn_log_call, mock_namespaced_crd, mock_kube_conn
    ):
        sensor = FlinkKubernetesSensor(
            application_name="flink-stream-example",
            attach_log=True,
            dag=self.dag,
            task_id="test_task_id",
        )
        with pytest.raises(AirflowException):
            sensor.poke(None)
        warn_log_call.assert_called_once()

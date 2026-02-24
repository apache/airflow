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
import pytest
from chart_utils.helm_template_generator import render_chart


class TestWorkerSets:
    """Tests worker sets."""

    def test_enable_default_worker_set_default(self):
        docs = render_chart(show_only=["templates/workers/worker-deployment.yaml"])
        assert len(docs) == 1

    @pytest.mark.parametrize(("enable_default", "objects_number"), [(True, 1), (False, 0)])
    def test_enable_default_worker_set(self, enable_default, objects_number):
        docs = render_chart(
            values={"workers": {"celery": {"enableDefault": enable_default}}},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert objects_number == len(docs)

    @pytest.mark.parametrize(
        ("enable_default", "expected"),
        [
            (True, ["test-worker", "test-worker-set1", "test-worker-set2"]),
            (False, ["test-worker-set1", "test-worker-set2"]),
        ],
    )
    def test_create_multiple_worker_sets(self, enable_default, expected):
        docs = render_chart(
            name="test",
            values={
                "workers": {
                    "celery": {
                        "enableDefault": enable_default,
                        "sets": [
                            {"name": "set1"},
                            {"name": "set2"},
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("[*].metadata.name", docs) == expected

    @pytest.mark.parametrize(
        "values",
        [
            {"celery": {"enableDefault": False, "sets": [{"name": "set1", "replicas": 3}]}},
            {"replicas": 2, "celery": {"enableDefault": False, "sets": [{"name": "set1", "replicas": 3}]}},
            {
                "replicas": 2,
                "celery": {
                    "enableDefault": False,
                    "replicas": None,
                    "sets": [{"name": "set1", "replicas": 3}],
                },
            },
            {"celery": {"enableDefault": False, "replicas": None, "sets": [{"name": "set1", "replicas": 3}]}},
            {"celery": {"enableDefault": False, "replicas": 2, "sets": [{"name": "set1", "replicas": 3}]}},
        ],
    )
    def test_overwrite_replicas(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.replicas", docs[0]) == 3

    @pytest.mark.parametrize(
        "values",
        [
            {"celery": {"enableDefault": False, "sets": [{"name": "set1", "revisionHistoryLimit": 3}]}},
            {
                "revisionHistoryLimit": 2,
                "celery": {"enableDefault": False, "sets": [{"name": "set1", "revisionHistoryLimit": 3}]},
            },
            {
                "celery": {
                    "enableDefault": False,
                    "revisionHistoryLimit": 2,
                    "sets": [{"name": "set1", "revisionHistoryLimit": 3}],
                }
            },
        ],
    )
    def test_overwrite_revision_history_limit(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == 3

    @pytest.mark.parametrize(
        "values",
        [
            {"celery": {"enableDefault": False, "sets": [{"name": "set1", "command": ["test"]}]}},
            {
                "command": ["t"],
                "celery": {"enableDefault": False, "sets": [{"name": "set1", "command": ["test"]}]},
            },
            {
                "celery": {
                    "enableDefault": False,
                    "command": ["t"],
                    "sets": [{"name": "set1", "command": ["test"]}],
                }
            },
        ],
    )
    def test_overwrite_command(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[?name=='worker'] | [0].command", docs[0]) == [
            "test"
        ]

    @pytest.mark.parametrize(
        "values",
        [
            {"celery": {"enableDefault": False, "sets": [{"name": "set1", "args": ["test"]}]}},
            {"args": ["t"], "celery": {"enableDefault": False, "sets": [{"name": "set1", "args": ["test"]}]}},
            {
                "args": ["t"],
                "celery": {
                    "enableDefault": False,
                    "args": None,
                    "sets": [{"name": "set1", "args": ["test"]}],
                },
            },
            {"celery": {"enableDefault": False, "args": None, "sets": [{"name": "set1", "args": ["test"]}]}},
            {"celery": {"enableDefault": False, "args": ["t"], "sets": [{"name": "set1", "args": ["test"]}]}},
        ],
    )
    def test_overwrite_args(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[?name=='worker'] | [0].args", docs[0]) == [
            "test"
        ]

    def test_disable_livenessprobe(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "set1", "livenessProbe": {"enabled": False}}],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[?name=='worker'] | [0].livenessProbe", docs[0])
            is None
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "livenessProbe": {"enabled": False},
                    "sets": [{"name": "set1", "livenessProbe": {"enabled": True}}],
                }
            },
            {
                "livenessProbe": {"enabled": False},
                "celery": {
                    "enableDefault": False,
                    "livenessProbe": {"enabled": None},
                    "sets": [{"name": "set1", "livenessProbe": {"enabled": True}}],
                },
            },
        ],
    )
    def test_overwrite_livenessprobe_enabled(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[?name=='worker'] | [0].livenessProbe", docs[0])
            is not None
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "livenessProbe": {
                        "initialDelaySeconds": 1,
                        "timeoutSeconds": 2,
                        "failureThreshold": 3,
                        "periodSeconds": 4,
                        "command": ["test"],
                    },
                    "sets": [
                        {
                            "name": "set1",
                            "livenessProbe": {
                                "initialDelaySeconds": 111,
                                "timeoutSeconds": 222,
                                "failureThreshold": 333,
                                "periodSeconds": 444,
                                "command": ["sh", "-c", "echo", "test"],
                            },
                        }
                    ],
                }
            },
            {
                "livenessProbe": {
                    "initialDelaySeconds": 1,
                    "timeoutSeconds": 2,
                    "failureThreshold": 3,
                    "periodSeconds": 4,
                    "command": ["test"],
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "livenessProbe": {
                                "initialDelaySeconds": 111,
                                "timeoutSeconds": 222,
                                "failureThreshold": 333,
                                "periodSeconds": 444,
                                "command": ["sh", "-c", "echo", "test"],
                            },
                        }
                    ],
                },
            },
            {
                "livenessProbe": {
                    "initialDelaySeconds": 1,
                    "timeoutSeconds": 2,
                    "failureThreshold": 3,
                    "periodSeconds": 4,
                    "command": ["test"],
                },
                "celery": {
                    "enableDefault": False,
                    "livenessProbe": {
                        "initialDelaySeconds": None,
                        "timeoutSeconds": None,
                        "failureThreshold": None,
                        "periodSeconds": None,
                    },
                    "sets": [
                        {
                            "name": "set1",
                            "livenessProbe": {
                                "initialDelaySeconds": 111,
                                "timeoutSeconds": 222,
                                "failureThreshold": 333,
                                "periodSeconds": 444,
                                "command": ["sh", "-c", "echo", "test"],
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_livenessprobe_values(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.containers[?name=='worker'] | [0].livenessProbe", docs[0]
        ) == {
            "initialDelaySeconds": 111,
            "timeoutSeconds": 222,
            "failureThreshold": 333,
            "periodSeconds": 444,
            "exec": {
                "command": ["sh", "-c", "echo", "test"],
            },
        }

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "updateStrategy": {"rollingUpdate": {"partition": 0}}}],
                }
            },
            {
                "updateStrategy": {"rollingUpdate": {"partition": 1}},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "updateStrategy": {"rollingUpdate": {"partition": 0}}}],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "updateStrategy": {"rollingUpdate": {"partition": 1}},
                    "sets": [{"name": "set1", "updateStrategy": {"rollingUpdate": {"partition": 0}}}],
                }
            },
        ],
    )
    def test_overwrite_update_strategy(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.updateStrategy", docs[0]) == {"rollingUpdate": {"partition": 0}}

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"enabled": False},
                            "strategy": {"rollingUpdate": {"maxSurge": "50%", "maxUnavailable": "100%"}},
                        }
                    ],
                },
            },
            {
                "strategy": {"rollingUpdate": {"maxSurge": "10%", "maxUnavailable": "90%"}},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"enabled": False},
                            "strategy": {"rollingUpdate": {"maxSurge": "50%", "maxUnavailable": "100%"}},
                        }
                    ],
                },
            },
            {
                "strategy": {"rollingUpdate": {"maxSurge": "10%", "maxUnavailable": "90%"}},
                "celery": {
                    "enableDefault": False,
                    "strategy": None,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"enabled": False},
                            "strategy": {"rollingUpdate": {"maxSurge": "50%", "maxUnavailable": "100%"}},
                        }
                    ],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "strategy": None,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"enabled": False},
                            "strategy": {"rollingUpdate": {"maxSurge": "50%", "maxUnavailable": "100%"}},
                        }
                    ],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "strategy": {"rollingUpdate": {"maxSurge": "10%", "maxUnavailable": "90%"}},
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"enabled": False},
                            "strategy": {"rollingUpdate": {"maxSurge": "50%", "maxUnavailable": "100%"}},
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_strategy(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == {
            "rollingUpdate": {"maxSurge": "50%", "maxUnavailable": "100%"}
        }

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "podManagementPolicy": "Parallel"}],
                }
            },
            {
                "podManagementPolicy": "OrderedReady",
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "podManagementPolicy": "Parallel"}],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "podManagementPolicy": "OrderedReady",
                    "sets": [{"name": "set1", "podManagementPolicy": "Parallel"}],
                }
            },
        ],
    )
    def test_overwrite_pod_management_policy(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.podManagementPolicy", docs[0]) == "Parallel"

    def test_disable_persistence(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "set1", "persistence": {"enabled": False}}],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "Deployment"

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"enabled": False},
                    "sets": [{"name": "set1", "persistence": {"enabled": True}}],
                }
            },
            {
                "persistence": {"enabled": False},
                "celery": {
                    "enableDefault": False,
                    "persistence": {"enabled": None},
                    "sets": [{"name": "set1", "persistence": {"enabled": True}}],
                },
            },
        ],
    )
    def test_overwrite_persistence_enabled(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "StatefulSet"

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {
                                "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}
                            },
                        }
                    ],
                }
            },
            {
                "persistence": {"persistentVolumeClaimRetentionPolicy": {"whenScaled": "Delete"}},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {
                                "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}
                            },
                        }
                    ],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"persistentVolumeClaimRetentionPolicy": {"whenScaled": "Delete"}},
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {
                                "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}
                            },
                        }
                    ],
                }
            },
        ],
    )
    def test_overwrite_persistence_persistent_volume_claim_retention_policy(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.persistentVolumeClaimRetentionPolicy", docs[0]) == {
            "whenDeleted": "Delete"
        }

    @pytest.mark.parametrize(
        "values",
        [
            {"celery": {"enableDefault": False, "sets": [{"name": "set1", "persistence": {"size": "10Gi"}}]}},
            {
                "persistence": {"size": "1Gi"},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "persistence": {"size": "10Gi"}}],
                },
            },
            {
                "persistence": {"size": "1Gi"},
                "celery": {
                    "enableDefault": False,
                    "persistence": {"size": None},
                    "sets": [{"name": "set1", "persistence": {"size": "10Gi"}}],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"size": None},
                    "sets": [{"name": "set1", "persistence": {"size": "10Gi"}}],
                }
            },
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"size": "1Gi"},
                    "sets": [{"name": "set1", "persistence": {"size": "10Gi"}}],
                }
            },
        ],
    )
    def test_overwrite_persistence_size(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.volumeClaimTemplates[0].spec.resources.requests.storage", docs[0]) == "10Gi"
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"storageClassName": "{{ .Release.Name }}-storage-class"},
                        }
                    ],
                }
            },
            {
                "persistence": {"storageClassName": "t"},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"storageClassName": "{{ .Release.Name }}-storage-class"},
                        }
                    ],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"storageClassName": "t"},
                    "sets": [
                        {
                            "name": "set1",
                            "persistence": {"storageClassName": "{{ .Release.Name }}-storage-class"},
                        }
                    ],
                }
            },
        ],
    )
    def test_overwrite_persistence_storage_class_name(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.volumeClaimTemplates[0].spec.storageClassName", docs[0])
            == "release-name-storage-class"
        )

    def test_enable_persistence_fix_permissions(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "set1", "persistence": {"fixPermissions": True}}],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            len(jmespath.search("spec.template.spec.initContainers[?name=='volume-permissions']", docs[0]))
            == 1
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"fixPermissions": False},
                    "sets": [{"name": "set1", "persistence": {"fixPermissions": True}}],
                }
            },
            {
                "persistence": {"fixPermissions": False},
                "celery": {
                    "enableDefault": False,
                    "persistence": {"fixPermissions": None},
                    "sets": [{"name": "set1", "persistence": {"fixPermissions": True}}],
                },
            },
        ],
    )
    def test_overwrite_persistence_fix_permissions(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            len(jmespath.search("spec.template.spec.initContainers[?name=='volume-permissions']", docs[0]))
            == 1
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "persistence": {"annotations": {"foo": "bar"}}}],
                }
            },
            {
                "persistence": {"annotations": {"a": "b"}},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "persistence": {"annotations": {"foo": "bar"}}}],
                },
            },
            {
                "celery": {
                    "enableDefault": False,
                    "persistence": {"annotations": {"a": "b"}},
                    "sets": [{"name": "set1", "persistence": {"annotations": {"foo": "bar"}}}],
                }
            },
        ],
    )
    def test_overwrite_persistence_annotations(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.volumeClaimTemplates[0].metadata.annotations", docs[0]) == {"foo": "bar"}

    def test_overwrite_kerberos_init_container_enabled(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "kerberosInitContainer": {"enabled": True}}],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.initContainers[?name=='kerberos-init']", docs[0]) is not None
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "kerberosInitContainer": {"enabled": True},
                    "sets": [{"name": "test", "kerberosInitContainer": {"enabled": False}}],
                }
            },
            {
                "kerberosInitContainer": {"enabled": True},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "test", "kerberosInitContainer": {"enabled": False}}],
                },
            },
        ],
    )
    def test_overwrite_kerberos_init_container_disable(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.initContainers[?name=='kerberos-init'] | [0]", docs[0])
            is None
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosInitContainer": {
                                "enabled": True,
                                "resources": {
                                    "limits": {"cpu": "3m", "memory": "4Mi"},
                                },
                            },
                        }
                    ],
                }
            },
            {
                "kerberosInitContainer": {
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "20Mi"},
                    }
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosInitContainer": {
                                "enabled": True,
                                "resources": {
                                    "limits": {"cpu": "3m", "memory": "4Mi"},
                                },
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_kerberos_init_container_resources(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.initContainers[?name=='kerberos-init'] | [0].resources", docs[0]
        ) == {
            "limits": {"cpu": "3m", "memory": "4Mi"},
        }

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosInitContainer": {
                                "enabled": True,
                                "securityContexts": {
                                    "container": {"runAsUser": 10},
                                },
                            },
                        }
                    ],
                }
            },
            {
                "kerberosInitContainer": {
                    "securityContexts": {
                        "container": {"allowPrivilegeEscalation": False},
                    }
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosInitContainer": {
                                "enabled": True,
                                "securityContexts": {
                                    "container": {"runAsUser": 10},
                                },
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_kerberos_init_container_security_context(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.initContainers[?name=='kerberos-init'] | [0].securityContext", docs[0]
        ) == {"runAsUser": 10}

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosInitContainer": {
                                "enabled": True,
                                "containerLifecycleHooks": {
                                    "postStart": {"exec": {"command": ["echo", "{{ .Release.Name }}"]}},
                                },
                            },
                        }
                    ],
                }
            },
            {
                "kerberosInitContainer": {
                    "containerLifecycleHooks": {"preStop": {"exec": {"command": ["echo", "test"]}}}
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosInitContainer": {
                                "enabled": True,
                                "containerLifecycleHooks": {
                                    "postStart": {"exec": {"command": ["echo", "{{ .Release.Name }}"]}},
                                },
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_kerberos_init_container_lifecycle_hooks(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.initContainers[?name=='kerberos-init'] | [0].lifecycle", docs[0]
        ) == {"postStart": {"exec": {"command": ["echo", "release-name"]}}}

    def test_overwrite_container_lifecycle_hooks(self):
        docs = render_chart(
            values={
                "workers": {
                    "containerLifecycleHooks": {"preStop": {"exec": {"command": ["echo", "test"]}}},
                    "celery": {
                        "enableDefault": False,
                        "sets": [
                            {
                                "name": "test",
                                "containerLifecycleHooks": {
                                    "postStart": {"exec": {"command": ["echo", "{{ .Release.Name }}"]}}
                                },
                            }
                        ],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].lifecycle", docs[0]) == {
            "postStart": {"exec": {"command": ["echo", "release-name"]}}
        }

    @pytest.mark.parametrize(("enable_default", "objects_number"), [(True, 1), (False, 0)])
    def test_enable_default_pod_disruption_budget(self, enable_default, objects_number):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {"enableDefault": enable_default},
                    "podDisruptionBudget": {"enabled": True},
                }
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert objects_number == len(docs)

    @pytest.mark.parametrize(
        ("enable_default", "expected"),
        [
            (True, ["test-worker-pdb", "test-worker-pdb-set1", "test-worker-pdb-set2"]),
            (False, ["test-worker-pdb-set1", "test-worker-pdb-set2"]),
        ],
    )
    def test_create_pod_disruption_budget_sets(self, enable_default, expected):
        docs = render_chart(
            name="test",
            values={
                "workers": {
                    "podDisruptionBudget": {"enabled": True},
                    "celery": {
                        "enableDefault": enable_default,
                        "sets": [
                            {"name": "set1"},
                            {"name": "set2"},
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert jmespath.search("[*].metadata.name", docs) == expected

    def test_overwrite_pod_disruption_budget_enabled(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "podDisruptionBudget": {"enabled": True}}],
                    },
                }
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert docs[0] is not None

    def test_overwrite_pod_disruption_budget_disable(self):
        docs = render_chart(
            values={
                "workers": {
                    "podDisruptionBudget": {"enabled": True},
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "podDisruptionBudget": {"enabled": False}}],
                    },
                }
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert len(docs) == 0

    def test_overwrite_pod_disruption_budget_config(self):
        docs = render_chart(
            values={
                "workers": {
                    "podDisruptionBudget": {"enabled": True, "config": {"maxUnavailable": 1}},
                    "celery": {
                        "enableDefault": False,
                        "sets": [
                            {
                                "name": "test",
                                "podDisruptionBudget": {"enabled": True, "config": {"minAvailable": 1}},
                            }
                        ],
                    },
                }
            },
            show_only=["templates/workers/worker-poddisruptionbudget.yaml"],
        )

        assert jmespath.search("spec.maxUnavailable", docs[0]) is None
        assert jmespath.search("spec.minAvailable", docs[0]) == 1

    @pytest.mark.parametrize(("enable_default", "objects_number"), [(True, 1), (False, 0)])
    def test_enable_default_service_account(self, enable_default, objects_number):
        docs = render_chart(
            values={"workers": {"celery": {"enableDefault": enable_default}}},
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )

        assert objects_number == len(docs)

    @pytest.mark.parametrize(
        ("enable_default", "expected"),
        [
            (True, ["test-airflow-worker", "test-airflow-worker-set1", "test-airflow-worker-set2"]),
            (False, ["test-airflow-worker-set1", "test-airflow-worker-set2"]),
        ],
    )
    def test_create_service_account_sets(self, enable_default, expected):
        docs = render_chart(
            name="test",
            values={
                "workers": {
                    "celery": {
                        "enableDefault": enable_default,
                        "sets": [
                            {"name": "set1"},
                            {"name": "set2"},
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )

        assert jmespath.search("[*].metadata.name", docs) == expected

    def test_overwrite_service_account_automount_service_account_token_disable(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "serviceAccount": {"automountServiceAccountToken": False}}],
                    },
                }
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )

        assert jmespath.search("automountServiceAccountToken", docs[0]) is False

    def test_overwrite_service_account_create_disable(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "serviceAccount": {"create": False}}],
                    },
                }
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )

        assert len(docs) == 0

    def test_overwrite_service_account_name(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "serviceAccount": {"name": "test"}}],
                    },
                }
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )

        assert jmespath.search("metadata.name", docs[0]) == "test"

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "serviceAccount": {"annotations": {"test": "echo"}},
                "celery": {"enableDefault": False, "sets": [{"name": "test"}]},
            },
            {
                "serviceAccount": {"annotations": {"echo": "test"}},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "test", "serviceAccount": {"annotations": {"test": "echo"}}}],
                },
            },
        ],
    )
    def test_overwrite_service_account_annotations(self, workers_values):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )

        assert jmespath.search("metadata.annotations", docs[0]) == {"test": "echo"}

    @pytest.mark.parametrize(("enable_default", "objects_number"), [(True, 1), (False, 0)])
    def test_enable_default_keda(self, enable_default, objects_number):
        docs = render_chart(
            values={"workers": {"celery": {"enableDefault": enable_default}, "keda": {"enabled": True}}},
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert objects_number == len(docs)

    @pytest.mark.parametrize(
        ("enable_default", "expected"),
        [
            (True, ["test-worker", "test-worker-set1", "test-worker-set2"]),
            (False, ["test-worker-set1", "test-worker-set2"]),
        ],
    )
    def test_create_keda_sets(self, enable_default, expected):
        docs = render_chart(
            name="test",
            values={
                "workers": {
                    "keda": {"enabled": True},
                    "celery": {
                        "enableDefault": enable_default,
                        "sets": [
                            {"name": "set1"},
                            {"name": "set2"},
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("[*].metadata.name", docs) == expected

    def test_overwrite_keda_enabled(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {"enableDefault": False, "sets": [{"name": "test", "keda": {"enabled": True}}]},
                }
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert len(docs) == 1

    def test_overwrite_keda_disable(self):
        docs = render_chart(
            values={
                "workers": {
                    "keda": {"enabled": True},
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": False}}],
                    },
                }
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert len(docs) == 0

    def test_overwrite_keda_pooling_interval(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": True, "pollingInterval": 10}}],
                    },
                }
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.pollingInterval", docs[0]) == 10

    def test_overwrite_keda_cooldown_period(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": True, "cooldownPeriod": 10}}],
                    },
                }
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.cooldownPeriod", docs[0]) == 10

    def test_overwrite_keda_min_replica_count(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": True, "minReplicaCount": 10}}],
                    },
                }
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.minReplicaCount", docs[0]) == 10

    def test_overwrite_keda_max_replica_count(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": True, "maxReplicaCount": 5}}],
                    },
                }
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.maxReplicaCount", docs[0]) == 5

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "keda": {
                                "enabled": True,
                                "advanced": {
                                    "horizontalPodAutoscalerConfig": {
                                        "behavior": {"scaleDown": {"stabilizationWindowSeconds": 300}}
                                    }
                                },
                            },
                        }
                    ],
                }
            },
            {
                "keda": {
                    "advanced": {
                        "horizontalPodAutoscalerConfig": {
                            "behavior": {
                                "scaleDown": {
                                    "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}]
                                }
                            }
                        }
                    }
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "keda": {
                                "enabled": True,
                                "advanced": {
                                    "horizontalPodAutoscalerConfig": {
                                        "behavior": {"scaleDown": {"stabilizationWindowSeconds": 300}}
                                    }
                                },
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_keda_advanced(self, workers_values):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.advanced", docs[0]) == {
            "horizontalPodAutoscalerConfig": {"behavior": {"scaleDown": {"stabilizationWindowSeconds": 300}}}
        }

    def test_overwrite_keda_query(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": True, "query": "test"}}],
                    },
                }
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.triggers[0].metadata.query", docs[0]) == "test"

    def test_overwrite_keda_use_pgbouncer_enable(self):
        docs = render_chart(
            values={
                "pgbouncer": {"enabled": True},
                "workers": {
                    "keda": {"usePgbouncer": False},
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": True, "usePgbouncer": True}}],
                    },
                },
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert (
            jmespath.search("spec.triggers[0].metadata.connectionFromEnv", docs[0])
            == "AIRFLOW_CONN_AIRFLOW_DB"
        )

    def test_overwrite_keda_use_pgbouncer_disable(self):
        docs = render_chart(
            values={
                "pgbouncer": {"enabled": True},
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "keda": {"enabled": True, "usePgbouncer": False}}],
                    },
                },
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.triggers[0].metadata.connectionFromEnv", docs[0]) == "KEDA_DB_CONN"

    def test_overwrite_queue(self):
        docs = render_chart(
            values={
                "pgbouncer": {"enabled": True},
                "workers": {
                    "celery": {"enableDefault": False, "sets": [{"name": "test", "queue": "test"}]},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "bash",
            "-c",
            "exec \\\nairflow celery worker -q test",
        ]

    @pytest.mark.parametrize(("enable_default", "objects_number"), [(True, 1), (False, 0)])
    def test_enable_default_hpa(self, enable_default, objects_number):
        docs = render_chart(
            values={"workers": {"celery": {"enableDefault": enable_default}, "hpa": {"enabled": True}}},
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert objects_number == len(docs)

    @pytest.mark.parametrize(
        ("enable_default", "expected"),
        [
            (True, ["test-worker", "test-worker-set1", "test-worker-set2"]),
            (False, ["test-worker-set1", "test-worker-set2"]),
        ],
    )
    def test_create_hpa_sets(self, enable_default, expected):
        docs = render_chart(
            name="test",
            values={
                "workers": {
                    "hpa": {"enabled": True},
                    "celery": {
                        "enableDefault": enable_default,
                        "sets": [
                            {"name": "set1"},
                            {"name": "set2"},
                        ],
                    },
                },
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert jmespath.search("[*].metadata.name", docs) == expected

    def test_overwrite_hpa_enabled(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {"enableDefault": False, "sets": [{"name": "test", "hpa": {"enabled": True}}]},
                }
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert len(docs) == 1

    def test_overwrite_hpa_disable(self):
        docs = render_chart(
            values={
                "workers": {
                    "hpa": {"enabled": True},
                    "celery": {"enableDefault": False, "sets": [{"name": "test", "hpa": {"enabled": False}}]},
                }
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert len(docs) == 0

    def test_overwrite_hpa_min_replica_count(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "hpa": {"enabled": True, "minReplicaCount": 10}}],
                    },
                }
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert jmespath.search("spec.minReplicas", docs[0]) == 10

    def test_overwrite_hpa_max_replica_count(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "hpa": {"enabled": True, "maxReplicaCount": 10}}],
                    },
                }
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert jmespath.search("spec.maxReplicas", docs[0]) == 10

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "hpa": {
                                "enabled": True,
                                "metrics": [
                                    {
                                        "type": "Resource",
                                        "resource": {
                                            "name": "cpu",
                                            "target": {"type": "Utilization", "averageUtilization": 80},
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                }
            },
            {
                "hpa": {
                    "metrics": [
                        {
                            "type": "Resource",
                            "resource": {
                                "name": "memory",
                                "target": {"type": "Utilization", "averageUtilization": 1},
                            },
                        }
                    ],
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "hpa": {
                                "enabled": True,
                                "metrics": [
                                    {
                                        "type": "Resource",
                                        "resource": {
                                            "name": "cpu",
                                            "target": {"type": "Utilization", "averageUtilization": 80},
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_hpa_metrics(self, workers_values):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert jmespath.search("spec.metrics", docs[0]) == [
            {
                "type": "Resource",
                "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 80}},
            }
        ]

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "hpa": {"enabled": True, "behavior": {"scaleDown": {"selectPolicy": "Max"}}},
                        }
                    ],
                }
            },
            {
                "hpa": {"behavior": {"scaleUp": {"selectPolicy": "Min"}}},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "hpa": {"enabled": True, "behavior": {"scaleDown": {"selectPolicy": "Max"}}},
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_hpa_behavior(self, workers_values):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert jmespath.search("spec.behavior", docs[0]) == {"scaleDown": {"selectPolicy": "Max"}}

    def test_overwrite_kerberos_sidecar_enabled(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "test", "kerberosSidecar": {"enabled": True}}],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[?name=='worker-kerberos']", docs[0]) is not None

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "kerberosSidecar": {"enabled": True},
                    "sets": [{"name": "test", "kerberosSidecar": {"enabled": False}}],
                }
            },
            {
                "kerberosSidecar": {"enabled": True},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "test", "kerberosSidecar": {"enabled": False}}],
                },
            },
        ],
    )
    def test_overwrite_kerberos_sidecar_disable(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[?name=='worker-kerberos'] | [0]", docs[0]) is None
        )

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosSidecar": {
                                "enabled": True,
                                "resources": {
                                    "limits": {"cpu": "3m", "memory": "4Mi"},
                                },
                            },
                        }
                    ],
                }
            },
            {
                "kerberosSidecar": {
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "20Mi"},
                    }
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosSidecar": {
                                "enabled": True,
                                "resources": {
                                    "limits": {"cpu": "3m", "memory": "4Mi"},
                                },
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_kerberos_sidecar_resources(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.containers[?name=='worker-kerberos'] | [0].resources", docs[0]
        ) == {
            "limits": {"cpu": "3m", "memory": "4Mi"},
        }

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosSidecar": {
                                "enabled": True,
                                "securityContexts": {
                                    "container": {"runAsUser": 10},
                                },
                            },
                        }
                    ],
                }
            },
            {
                "kerberosSidecar": {
                    "securityContexts": {
                        "container": {"allowPrivilegeEscalation": False},
                    }
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosSidecar": {
                                "enabled": True,
                                "securityContexts": {
                                    "container": {"runAsUser": 10},
                                },
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_kerberos_sidecar_security_context_container(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.containers[?name=='worker-kerberos'] | [0].securityContext", docs[0]
        ) == {"runAsUser": 10}

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosSidecar": {
                                "enabled": True,
                                "containerLifecycleHooks": {
                                    "postStart": {"exec": {"command": ["echo", "{{ .Release.Name }}"]}},
                                },
                            },
                        }
                    ],
                }
            },
            {
                "kerberosSidecar": {
                    "containerLifecycleHooks": {"preStop": {"exec": {"command": ["echo", "test"]}}}
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "kerberosSidecar": {
                                "enabled": True,
                                "containerLifecycleHooks": {
                                    "postStart": {"exec": {"command": ["echo", "{{ .Release.Name }}"]}},
                                },
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_kerberos_sidecar_container_lifecycle_hooks(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.containers[?name=='worker-kerberos'] | [0].lifecycle", docs[0]
        ) == {"postStart": {"exec": {"command": ["echo", "release-name"]}}}

    @pytest.mark.parametrize(
        "values",
        [
            {
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "resources": {
                                "limits": {"cpu": "3m", "memory": "4Mi"},
                            },
                        }
                    ],
                }
            },
            {
                "resources": {
                    "requests": {"cpu": "10m", "memory": "20Mi"},
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "test",
                            "resources": {
                                "limits": {"cpu": "3m", "memory": "4Mi"},
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_resources(self, values):
        docs = render_chart(
            values={"workers": values},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[?name=='worker'] | [0].resources", docs[0]) == {
            "limits": {"cpu": "3m", "memory": "4Mi"},
        }

    def test_overwrite_termination_grace_period_seconds(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [
                            {
                                "name": "test",
                                "terminationGracePeriodSeconds": 5,
                            }
                        ],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.terminationGracePeriodSeconds", docs[0]) == 5

    def test_overwrite_safe_to_evict_enable(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "set1", "safeToEvict": True}],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.metadata.annotations", docs[0])[
                "cluster-autoscaler.kubernetes.io/safe-to-evict"
            ]
            == "true"
        )

    def test_overwrite_safe_to_evict_disable(self):
        docs = render_chart(
            values={
                "workers": {
                    "safeToEvict": True,
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "set1", "safeToEvict": False}],
                    },
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.metadata.annotations", docs[0])[
                "cluster-autoscaler.kubernetes.io/safe-to-evict"
            ]
            == "false"
        )

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "extraContainers": [
                    {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                ],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "extraContainers": [{"name": "test", "image": "test"}],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "extraContainers": [
                                {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                            ],
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_extra_containers(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        containers = jmespath.search("spec.template.spec.containers", docs[0])

        assert len(containers) == 3  # worker, worker-log-groomer, extra
        assert containers[-1] == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "extraInitContainers": [
                    {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                ],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "extraInitContainers": [{"name": "test", "image": "test"}],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "extraInitContainers": [
                                {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                            ],
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_extra_init_containers(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        containers = jmespath.search("spec.template.spec.initContainers", docs[0])

        assert len(containers) == 2  # wait-for-airflow-migrations, extra
        assert containers[-1] == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "extraVolumes": [{"name": "test", "emptyDir": {}}],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_extra_volumes(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[0]", docs[0]) == {
            "name": "test-volume-airflow",
            "emptyDir": {},
        }

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "extraVolumeMounts": [
                    {"name": "test-volume-mount-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                ],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "extraVolumeMounts": [{"name": "test", "mountPath": "/opt"}],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "extraVolumeMounts": [
                                {"name": "test-volume-mount-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                            ],
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_extra_volume_mounts(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].volumeMounts[0]", docs[0]) == {
            "name": "test-volume-mount-airflow",
            "mountPath": "/opt/test",
        }

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "extraPorts": [{"name": "test-extra-port", "containerPort": 10}],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "extraPorts": [{"name": "test", "containerPort": 1}],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {"name": "set1", "extraPorts": [{"name": "test-extra-port", "containerPort": 10}]}
                    ],
                },
            },
        ],
    )
    def test_overwrite_extra_ports(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].ports[:-1]", docs[0]) == [
            {"name": "test-extra-port", "containerPort": 10}
        ]

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "nodeSelector": {"name": "test-node"},
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "nodeSelector": {"test": "name"},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "nodeSelector": {"name": "test-node"}}],
                },
            },
        ],
    )
    def test_overwrite_node_selector(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.nodeSelector", docs[0]) == {"name": "test-node"}

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "runtimeClassName": "test-class",
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "runtimeClassName": "test",
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "runtimeClassName": "test-class"}],
                },
            },
        ],
    )
    def test_overwrite_runtime_class_name(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.runtimeClassName", docs[0]) == "test-class"

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "priorityClassName": "test-class",
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "priorityClassName": "test",
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "priorityClassName": "test-class"}],
                },
            },
        ],
    )
    def test_overwrite_priority_class_name(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.priorityClassName", docs[0]) == "test-class"

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "affinity": {
                    "nodeAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "weight": 1,
                                "preference": {
                                    "matchExpressions": [
                                        {"key": "not-me", "operator": "In", "values": ["true"]},
                                    ]
                                },
                            }
                        ]
                    }
                },
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "affinity": {
                    "podAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "podAffinityTerm": {
                                    "topologyKey": "foo",
                                    "labelSelector": {"matchLabels": {"tier": "airflow"}},
                                },
                                "weight": 1,
                            }
                        ]
                    }
                },
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "affinity": {
                                "nodeAffinity": {
                                    "preferredDuringSchedulingIgnoredDuringExecution": [
                                        {
                                            "weight": 1,
                                            "preference": {
                                                "matchExpressions": [
                                                    {"key": "not-me", "operator": "In", "values": ["true"]},
                                                ]
                                            },
                                        }
                                    ]
                                }
                            },
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_affinity(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.affinity", docs[0]) == {
            "nodeAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "weight": 1,
                        "preference": {
                            "matchExpressions": [
                                {"key": "not-me", "operator": "In", "values": ["true"]},
                            ]
                        },
                    }
                ]
            }
        }

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "tolerations": [
                    {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                ],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "tolerations": [
                    {"key": "not-me", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                ],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "tolerations": [
                                {
                                    "key": "dynamic-pods",
                                    "operator": "Equal",
                                    "value": "true",
                                    "effect": "NoSchedule",
                                }
                            ],
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_tolerations(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.tolerations", docs[0]) == [
            {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
        ]

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "topologySpreadConstraints": [
                    {
                        "maxSkew": 1,
                        "topologyKey": "foo",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"tier": "airflow"}},
                    }
                ],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "topologySpreadConstraints": [
                    {
                        "maxSkew": 1,
                        "topologyKey": "not-me",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"tier": "airflow"}},
                    }
                ],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "topologySpreadConstraints": [
                                {
                                    "maxSkew": 1,
                                    "topologyKey": "foo",
                                    "whenUnsatisfiable": "ScheduleAnyway",
                                    "labelSelector": {"matchLabels": {"tier": "airflow"}},
                                }
                            ],
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_topology_spread_constraints(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.topologySpreadConstraints", docs[0]) == [
            {
                "maxSkew": 1,
                "topologyKey": "foo",
                "whenUnsatisfiable": "ScheduleAnyway",
                "labelSelector": {"matchLabels": {"tier": "airflow"}},
            }
        ]

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "hostAliases": [{"ip": "127.0.0.2", "hostnames": ["test.hostname"]}],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "hostAliases": [{"ip": "192.168.0.0", "hostnames": ["test"]}],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {"name": "set1", "hostAliases": [{"ip": "127.0.0.2", "hostnames": ["test.hostname"]}]}
                    ],
                },
            },
        ],
    )
    def test_overwrite_host_aliases(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.hostAliases", docs[0]) == [
            {"ip": "127.0.0.2", "hostnames": ["test.hostname"]}
        ]

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"annotations": {"test": "echo"}, "celery": {"enableDefault": False, "sets": [{"name": "set1"}]}},
            {
                "annotations": {"echo": "test"},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "annotations": {"test": "echo"}}],
                },
            },
        ],
    )
    def test_overwrite_annotations(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("metadata.annotations", docs[0]) == {"test": "echo"}

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "podAnnotations": {"test": "echo"},
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "podAnnotations": {"echo": "test"},
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "podAnnotations": {"test": "echo"}}],
                },
            },
        ],
    )
    def test_overwrite_pod_annotations(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.metadata.annotations", docs[0])["test"] == "echo"

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"labels": {"test": "echo"}, "celery": {"enableDefault": False, "sets": [{"name": "set1"}]}},
            {
                "labels": {"echo": "test"},
                "celery": {"enableDefault": False, "sets": [{"name": "set1", "labels": {"test": "echo"}}]},
            },
        ],
    )
    def test_overwrite_labels(self, workers_values):
        docs = render_chart(
            values={
                "labels": {"global-test": "global-echo"},
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        labels = jmespath.search("spec.template.metadata.labels", docs[0])

        assert labels["global-test"] == "global-echo"
        assert labels["test"] == "echo"
        assert labels.get("echo") is None

    def test_overwrite_wait_for_migration_disable(self):
        docs = render_chart(
            values={
                "workers": {
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "set1", "waitForMigrations": {"enabled": False}}],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert (
            jmespath.search(
                "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']", docs[0]
            )
            is None
        )

    def test_overwrite_wait_for_migration_enable(self):
        docs = render_chart(
            values={
                "workers": {
                    "waitForMigrations": {"enabled": False},
                    "celery": {
                        "enableDefault": False,
                        "sets": [{"name": "set1", "waitForMigrations": {"enabled": True}}],
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert (
            jmespath.search(
                "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']", docs[0]
            )
            is not None
        )

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "waitForMigrations": {"env": [{"name": "TEST_ENV_1", "value": "test_env_1"}]},
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "waitForMigrations": {"env": [{"name": "TEST", "value": "test"}]},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "waitForMigrations": {"env": [{"name": "TEST_ENV_1", "value": "test_env_1"}]},
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_wait_for_migration_env(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        envs = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations'].env | [0]", docs[0]
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in envs
        assert {"name": "TEST", "value": "test"} not in envs

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "waitForMigrations": {"securityContexts": {"container": {"runAsUser": 10}}},
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "waitForMigrations": {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}},
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "waitForMigrations": {"securityContexts": {"container": {"runAsUser": 10}}},
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_wait_for_migration_security_context_container(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations'].securityContext | [0]",
            docs[0],
        ) == {"runAsUser": 10}

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "env": [{"name": "TEST", "value": "test"}],
                "celery": {
                    "enableDefault": False,
                    "sets": [{"name": "set1", "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}]}],
                },
            },
        ],
    )
    def test_overwrite_env(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        envs = jmespath.search("spec.template.spec.containers[?name=='worker'].env | [0]", docs[0])

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in envs
        assert {"name": "TEST", "value": "test"} not in envs

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "volumeClaimTemplates": [
                    {
                        "metadata": {"name": "test-volume-airflow-1"},
                        "spec": {
                            "storageClassName": "storage-class-1",
                            "accessModes": ["ReadWriteOnce"],
                            "resources": {"requests": {"storage": "10Gi"}},
                        },
                    }
                ],
                "celery": {"enableDefault": False, "sets": [{"name": "set1"}]},
            },
            {
                "volumeClaimTemplates": [
                    {
                        "metadata": {"name": "test-volume"},
                        "spec": {
                            "storageClassName": "class",
                            "accessModes": ["ReadOnce"],
                            "resources": {"requests": {"storage": "1Gi"}},
                        },
                    }
                ],
                "celery": {
                    "enableDefault": False,
                    "sets": [
                        {
                            "name": "set1",
                            "volumeClaimTemplates": [
                                {
                                    "metadata": {"name": "test-volume-airflow-1"},
                                    "spec": {
                                        "storageClassName": "storage-class-1",
                                        "accessModes": ["ReadWriteOnce"],
                                        "resources": {"requests": {"storage": "10Gi"}},
                                    },
                                }
                            ],
                        }
                    ],
                },
            },
        ],
    )
    def test_overwrite_volume_claim_templates(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.volumeClaimTemplates[1:]", docs[0]) == [
            {
                "metadata": {"name": "test-volume-airflow-1"},
                "spec": {
                    "storageClassName": "storage-class-1",
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "10Gi"}},
                },
            }
        ]

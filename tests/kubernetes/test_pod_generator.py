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
import sys
import unittest
import uuid
from unittest import mock

from dateutil import parser
from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowConfigException
from airflow.kubernetes.k8s_model import append_to_pod
from airflow.kubernetes.pod import Resources
from airflow.kubernetes.pod_generator import (
    PodDefaults, PodGenerator, datetime_to_label_safe_datestring, extend_object_field, merge_objects,
)
from airflow.kubernetes.secret import Secret


class TestPodGenerator(unittest.TestCase):

    def setUp(self):
        self.static_uuid = uuid.UUID('cf4a56d2-8101-4217-b027-2af6216feb48')
        self.deserialize_result = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': 'memory-demo', 'namespace': 'mem-example'},
            'spec': {
                'containers': [{
                    'args': ['--vm', '1', '--vm-bytes', '150M', '--vm-hang', '1'],
                    'command': ['stress'],
                    'image': 'polinux/stress',
                    'name': 'memory-demo-ctr',
                    'resources': {
                        'limits': {'memory': '200Mi'},
                        'requests': {'memory': '100Mi'}
                    }
                }]
            }
        }

        self.envs = {
            'ENVIRONMENT': 'prod',
            'LOG_LEVEL': 'warning'
        }
        self.secrets = [
            # This should be a secretRef
            Secret('env', None, 'secret_a'),
            # This should be a single secret mounted in volumeMounts
            Secret('volume', '/etc/foo', 'secret_b'),
            # This should produce a single secret mounted in env
            Secret('env', 'TARGET', 'secret_b', 'source_b'),
        ]

        self.execution_date = parser.parse('2020-08-24 00:00:00.000000')
        self.execution_date_label = datetime_to_label_safe_datestring(self.execution_date)
        self.dag_id = 'dag_id'
        self.task_id = 'task_id'
        self.try_number = 3
        self.labels = {
            'airflow-worker': 'uuid',
            'dag_id': self.dag_id,
            'execution_date': self.execution_date_label,
            'task_id': self.task_id,
            'try_number': str(self.try_number),
            'airflow_version': mock.ANY,
            'kubernetes_executor': 'True'
        }
        self.annotations = {
            'dag_id': self.dag_id,
            'task_id': self.task_id,
            'execution_date': self.execution_date.isoformat(),
            'try_number': str(self.try_number),
        }
        self.metadata = {
            'labels': self.labels,
            'name': 'pod_id-' + self.static_uuid.hex,
            'namespace': 'namespace',
            'annotations': self.annotations,
        }

        self.resources = Resources('1Gi', 1, '2Gi', '2Gi', 2, 1, '4Gi')
        self.k8s_client = ApiClient()
        self.expected = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': 'myapp-pod-' + self.static_uuid.hex,
                'labels': {'app': 'myapp'},
                'namespace': 'default'
            },
            'spec': {
                'containers': [{
                    'name': 'base',
                    'image': 'busybox',
                    'args': [],
                    'command': [
                        'sh', '-c', 'echo Hello Kubernetes!'
                    ],
                    'env': [{
                        'name': 'ENVIRONMENT',
                        'value': 'prod'
                    }, {
                        'name': 'LOG_LEVEL',
                        'value': 'warning'
                    }, {
                        'name': 'TARGET',
                        'valueFrom': {
                            'secretKeyRef': {
                                'name': 'secret_b',
                                'key': 'source_b'
                            }
                        }
                    }],
                    'envFrom': [{
                        'configMapRef': {
                            'name': 'configmap_a'
                        }
                    }, {
                        'configMapRef': {
                            'name': 'configmap_b'
                        }
                    }, {
                        'secretRef': {
                            'name': 'secret_a'
                        }
                    }],
                    'resources': {
                        'requests': {
                            'memory': '1Gi',
                            'cpu': 1,
                            'ephemeral-storage': '2Gi'
                        },
                        'limits': {
                            'memory': '2Gi',
                            'cpu': 2,
                            'nvidia.com/gpu': 1,
                            'ephemeral-storage': '4Gi'
                        },
                    },
                    'ports': [{'name': 'foo', 'containerPort': 1234}],
                    'volumeMounts': [{
                        'mountPath': '/etc/foo',
                        'name': 'secretvol' + str(self.static_uuid),
                        'readOnly': True
                    }]
                }],
                'volumes': [{
                    'name': 'secretvol' + str(self.static_uuid),
                    'secret': {
                        'secretName': 'secret_b'
                    }
                }],
                'hostNetwork': False,
                'imagePullSecrets': [
                    {'name': 'pull_secret_a'},
                    {'name': 'pull_secret_b'}
                ],
                'securityContext': {
                    'runAsUser': 1000,
                    'fsGroup': 2000,
                },
            }
        }

    @mock.patch('uuid.uuid4')
    def test_gen_pod(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        pod_generator = PodGenerator(
            labels={'app': 'myapp'},
            name='myapp-pod',
            image_pull_secrets='pull_secret_a,pull_secret_b',
            image='busybox',
            envs=self.envs,
            cmds=['sh', '-c', 'echo Hello Kubernetes!'],
            security_context=k8s.V1PodSecurityContext(
                run_as_user=1000,
                fs_group=2000,
            ),
            namespace='default',
            ports=[k8s.V1ContainerPort(name='foo', container_port=1234)],
            configmaps=['configmap_a', 'configmap_b']
        )
        result = pod_generator.gen_pod()
        result = append_to_pod(result, self.secrets)
        result = self.resources.attach_to_pod(result)
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        # sort
        result_dict['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        result_dict['spec']['containers'][0]['envFrom'].sort(
            key=lambda x: list(x.values())[0]['name']
        )
        self.assertDictEqual(self.expected, result_dict)

    @mock.patch('uuid.uuid4')
    def test_gen_pod_extract_xcom(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        pod_generator = PodGenerator(
            labels={'app': 'myapp'},
            name='myapp-pod',
            image_pull_secrets='pull_secret_a,pull_secret_b',
            image='busybox',
            envs=self.envs,
            cmds=['sh', '-c', 'echo Hello Kubernetes!'],
            namespace='default',
            security_context=k8s.V1PodSecurityContext(
                run_as_user=1000,
                fs_group=2000,
            ),
            ports=[k8s.V1ContainerPort(name='foo', container_port=1234)],
            configmaps=['configmap_a', 'configmap_b'],
            extract_xcom=True
        )
        result = pod_generator.gen_pod()
        result = append_to_pod(result, self.secrets)
        result = self.resources.attach_to_pod(result)
        result_dict = self.k8s_client.sanitize_for_serialization(result)
        container_two = {
            'name': 'airflow-xcom-sidecar',
            'image': "alpine",
            'command': ['sh', '-c', PodDefaults.XCOM_CMD],
            'volumeMounts': [
                {
                    'name': 'xcom',
                    'mountPath': '/airflow/xcom'
                }
            ],
            'resources': {'requests': {'cpu': '1m'}},
        }
        self.expected['spec']['containers'].append(container_two)
        self.expected['spec']['containers'][0]['volumeMounts'].insert(0, {
            'name': 'xcom',
            'mountPath': '/airflow/xcom'
        })
        self.expected['spec']['volumes'].insert(0, {
            'name': 'xcom', 'emptyDir': {}
        })
        result_dict['spec']['containers'][0]['env'].sort(key=lambda x: x['name'])
        self.assertEqual(result_dict, self.expected)

    def test_from_obj(self):
        result = PodGenerator.from_obj({
            "KubernetesExecutor": {
                "annotations": {"test": "annotation"},
                "volumes": [
                    {
                        "name": "example-kubernetes-test-volume",
                        "hostPath": {"path": "/tmp/"},
                    },
                ],
                "volume_mounts": [
                    {
                        "mountPath": "/foo/",
                        "name": "example-kubernetes-test-volume",
                    },
                ],
            }
        })
        result = self.k8s_client.sanitize_for_serialization(result)

        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'annotations': {'test': 'annotation'},
            },
            'spec': {
                'containers': [{
                    'args': [],
                    'command': [],
                    'env': [],
                    'envFrom': [],
                    'name': 'base',
                    'ports': [],
                    'volumeMounts': [{
                        'mountPath': '/foo/',
                        'name': 'example-kubernetes-test-volume'
                    }],
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'volumes': [{
                    'hostPath': {'path': '/tmp/'},
                    'name': 'example-kubernetes-test-volume'
                }],
            }
        }, result)

    @mock.patch('uuid.uuid4')
    def test_reconcile_pods_empty_mutator_pod(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        base_pod = PodGenerator(
            image='image1',
            name='name1',
            envs={'key1': 'val1'},
            cmds=['/bin/command1.sh', 'arg1'],
            ports=[k8s.V1ContainerPort(name='port', container_port=2118)],
            volumes=[{
                'hostPath': {'path': '/tmp/'},
                'name': 'example-kubernetes-test-volume1'
            }],
            volume_mounts=[{
                'mountPath': '/foo/',
                'name': 'example-kubernetes-test-volume1'
            }],
        ).gen_pod()

        mutator_pod = None
        name = 'name1-' + self.static_uuid.hex

        base_pod.metadata.name = name

        result = PodGenerator.reconcile_pods(base_pod, mutator_pod)
        self.assertEqual(base_pod, result)

        mutator_pod = k8s.V1Pod()
        result = PodGenerator.reconcile_pods(base_pod, mutator_pod)
        self.assertEqual(base_pod, result)

    @mock.patch('uuid.uuid4')
    def test_reconcile_pods(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        base_pod = PodGenerator(
            image='image1',
            name='name1',
            envs={'key1': 'val1'},
            cmds=['/bin/command1.sh', 'arg1'],
            ports=[k8s.V1ContainerPort(name='port', container_port=2118)],
            volumes=[{
                'hostPath': {'path': '/tmp/'},
                'name': 'example-kubernetes-test-volume1'
            }],
            labels={"foo": "bar"},
            volume_mounts=[{
                'mountPath': '/foo/',
                'name': 'example-kubernetes-test-volume1'
            }],
        ).gen_pod()

        mutator_pod = PodGenerator(
            envs={'key2': 'val2'},
            image='',
            name='name2',
            labels={"bar": "baz"},
            cmds=['/bin/command2.sh', 'arg2'],
            volumes=[{
                'hostPath': {'path': '/tmp/'},
                'name': 'example-kubernetes-test-volume2'
            }],
            volume_mounts=[{
                'mountPath': '/foo/',
                'name': 'example-kubernetes-test-volume2'
            }]
        ).gen_pod()

        result = PodGenerator.reconcile_pods(base_pod, mutator_pod)
        result = self.k8s_client.sanitize_for_serialization(result)
        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'name': 'name2-' + self.static_uuid.hex,
                         'labels': {'foo': 'bar', "bar": "baz"}},
            'spec': {
                'containers': [{
                    'args': [],
                    'command': ['/bin/command2.sh', 'arg2'],
                    'env': [
                        {'name': 'key1', 'value': 'val1'},
                        {'name': 'key2', 'value': 'val2'}
                    ],
                    'envFrom': [],
                    'image': 'image1',
                    'name': 'base',
                    'ports': [{
                        'containerPort': 2118,
                        'name': 'port',
                    }],
                    'volumeMounts': [{
                        'mountPath': '/foo/',
                        'name': 'example-kubernetes-test-volume1'
                    }, {
                        'mountPath': '/foo/',
                        'name': 'example-kubernetes-test-volume2'
                    }]
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'volumes': [{
                    'hostPath': {'path': '/tmp/'},
                    'name': 'example-kubernetes-test-volume1'
                }, {
                    'hostPath': {'path': '/tmp/'},
                    'name': 'example-kubernetes-test-volume2'
                }]
            }
        }, result)

    @mock.patch('uuid.uuid4')
    def test_construct_pod_empty_worker_config(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        executor_config = k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='',
                        resources=k8s.V1ResourceRequirements(
                            limits={
                                'cpu': '1m',
                                'memory': '1G'
                            }
                        )
                    )
                ]
            )
        )
        worker_config = k8s.V1Pod()

        result = PodGenerator.construct_pod(
            self.dag_id,
            self.task_id,
            'pod_id',
            self.try_number,
            self.execution_date,
            ['command'],
            executor_config,
            worker_config,
            'namespace',
            'uuid',
        )
        sanitized_result = self.k8s_client.sanitize_for_serialization(result)

        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': self.metadata,
            'spec': {
                'containers': [{
                    'args': [],
                    'command': ['command'],
                    'env': [],
                    'envFrom': [],
                    'name': 'base',
                    'ports': [],
                    'resources': {
                        'limits': {
                            'cpu': '1m',
                            'memory': '1G'
                        }
                    },
                    'volumeMounts': []
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'volumes': []
            }
        }, sanitized_result)

    @mock.patch('uuid.uuid4')
    def test_construct_pod_empty_execuctor_config(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        worker_config = k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='',
                        resources=k8s.V1ResourceRequirements(
                            limits={
                                'cpu': '1m',
                                'memory': '1G'
                            }
                        )
                    )
                ]
            )
        )
        executor_config = None

        result = PodGenerator.construct_pod(
            self.dag_id,
            self.task_id,
            'pod_id',
            self.try_number,
            self.execution_date,
            ['command'],
            executor_config,
            worker_config,
            'namespace',
            'uuid',
        )
        sanitized_result = self.k8s_client.sanitize_for_serialization(result)

        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': self.metadata,
            'spec': {
                'containers': [{
                    'args': [],
                    'command': ['command'],
                    'env': [],
                    'envFrom': [],
                    'name': 'base',
                    'ports': [],
                    'resources': {
                        'limits': {
                            'cpu': '1m',
                            'memory': '1G'
                        }
                    },
                    'volumeMounts': []
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'volumes': []
            }
        }, sanitized_result)

    @mock.patch('uuid.uuid4')
    def test_construct_pod(self, mock_uuid):
        mock_uuid.return_value = self.static_uuid
        worker_config = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name='gets-overridden-by-dynamic-args',
                annotations={
                    'should': 'stay'
                }
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='doesnt-override',
                        resources=k8s.V1ResourceRequirements(
                            limits={
                                'cpu': '1m',
                                'memory': '1G'
                            }
                        ),
                        security_context=k8s.V1SecurityContext(
                            run_as_user=1
                        )
                    )
                ]
            )
        )
        executor_config = k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='doesnt-override-either',
                        resources=k8s.V1ResourceRequirements(
                            limits={
                                'cpu': '2m',
                                'memory': '2G'
                            }
                        )
                    )
                ]
            )
        )

        result = PodGenerator.construct_pod(
            self.dag_id,
            self.task_id,
            'pod_id',
            self.try_number,
            self.execution_date,
            ['command'],
            executor_config,
            worker_config,
            'namespace',
            'uuid',
        )
        sanitized_result = self.k8s_client.sanitize_for_serialization(result)

        expected_metadata = dict(self.metadata)
        expected_metadata['annotations'].update({'should': 'stay'})

        self.assertEqual({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': expected_metadata,
            'spec': {
                'containers': [{
                    'args': [],
                    'command': ['command'],
                    'env': [],
                    'envFrom': [],
                    'name': 'base',
                    'ports': [],
                    'resources': {
                        'limits': {
                            'cpu': '2m',
                            'memory': '2G'
                        }
                    },
                    'volumeMounts': [],
                    'securityContext': {'runAsUser': 1}
                }],
                'hostNetwork': False,
                'imagePullSecrets': [],
                'volumes': []
            }
        }, sanitized_result)

    def test_merge_objects_empty(self):
        annotations = {'foo1': 'bar1'}
        base_obj = k8s.V1ObjectMeta(annotations=annotations)
        client_obj = None
        res = merge_objects(base_obj, client_obj)
        self.assertEqual(base_obj, res)

        client_obj = k8s.V1ObjectMeta()
        res = merge_objects(base_obj, client_obj)
        self.assertEqual(base_obj, res)

        client_obj = k8s.V1ObjectMeta(annotations=annotations)
        base_obj = None
        res = merge_objects(base_obj, client_obj)
        self.assertEqual(client_obj, res)

        base_obj = k8s.V1ObjectMeta()
        res = merge_objects(base_obj, client_obj)
        self.assertEqual(client_obj, res)

    def test_merge_objects(self):
        base_annotations = {'foo1': 'bar1'}
        base_labels = {'foo1': 'bar1'}
        client_annotations = {'foo2': 'bar2'}
        base_obj = k8s.V1ObjectMeta(
            annotations=base_annotations,
            labels=base_labels
        )
        client_obj = k8s.V1ObjectMeta(annotations=client_annotations)
        res = merge_objects(base_obj, client_obj)
        client_obj.labels = base_labels
        self.assertEqual(client_obj, res)

    def test_extend_object_field_empty(self):
        ports = [k8s.V1ContainerPort(container_port=1, name='port')]
        base_obj = k8s.V1Container(name='base_container', ports=ports)
        client_obj = k8s.V1Container(name='client_container')
        res = extend_object_field(base_obj, client_obj, 'ports')
        client_obj.ports = ports
        self.assertEqual(client_obj, res)

        base_obj = k8s.V1Container(name='base_container')
        client_obj = k8s.V1Container(name='base_container', ports=ports)
        res = extend_object_field(base_obj, client_obj, 'ports')
        self.assertEqual(client_obj, res)

    def test_extend_object_field_not_list(self):
        base_obj = k8s.V1Container(name='base_container', image='image')
        client_obj = k8s.V1Container(name='client_container')
        with self.assertRaises(ValueError):
            extend_object_field(base_obj, client_obj, 'image')
        base_obj = k8s.V1Container(name='base_container')
        client_obj = k8s.V1Container(name='client_container', image='image')
        with self.assertRaises(ValueError):
            extend_object_field(base_obj, client_obj, 'image')

    def test_extend_object_field(self):
        base_ports = [k8s.V1ContainerPort(container_port=1, name='base_port')]
        base_obj = k8s.V1Container(name='base_container', ports=base_ports)
        client_ports = [k8s.V1ContainerPort(container_port=1, name='client_port')]
        client_obj = k8s.V1Container(name='client_container', ports=client_ports)
        res = extend_object_field(base_obj, client_obj, 'ports')
        client_obj.ports = base_ports + client_ports
        self.assertEqual(client_obj, res)

    def test_reconcile_containers_empty(self):
        base_objs = [k8s.V1Container(name='base_container')]
        client_objs = []
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        self.assertEqual(base_objs, res)

        client_objs = [k8s.V1Container(name='client_container')]
        base_objs = []
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        self.assertEqual(client_objs, res)

        res = PodGenerator.reconcile_containers([], [])
        self.assertEqual(res, [])

    def test_reconcile_containers(self):
        base_ports = [k8s.V1ContainerPort(container_port=1, name='base_port')]
        base_objs = [
            k8s.V1Container(name='base_container1', ports=base_ports),
            k8s.V1Container(name='base_container2', image='base_image'),
        ]
        client_ports = [k8s.V1ContainerPort(container_port=2, name='client_port')]
        client_objs = [
            k8s.V1Container(name='client_container1', ports=client_ports),
            k8s.V1Container(name='client_container2', image='client_image'),
        ]
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        client_objs[0].ports = base_ports + client_ports
        self.assertEqual(client_objs, res)

        base_ports = [k8s.V1ContainerPort(container_port=1, name='base_port')]
        base_objs = [
            k8s.V1Container(name='base_container1', ports=base_ports),
            k8s.V1Container(name='base_container2', image='base_image'),
        ]
        client_ports = [k8s.V1ContainerPort(container_port=2, name='client_port')]
        client_objs = [
            k8s.V1Container(name='client_container1', ports=client_ports),
            k8s.V1Container(name='client_container2', stdin=True),
        ]
        res = PodGenerator.reconcile_containers(base_objs, client_objs)
        client_objs[0].ports = base_ports + client_ports
        client_objs[1].image = 'base_image'
        self.assertEqual(client_objs, res)

    def test_reconcile_specs_empty(self):
        base_spec = k8s.V1PodSpec(containers=[])
        client_spec = None
        res = PodGenerator.reconcile_specs(base_spec, client_spec)
        self.assertEqual(base_spec, res)

        base_spec = None
        client_spec = k8s.V1PodSpec(containers=[])
        res = PodGenerator.reconcile_specs(base_spec, client_spec)
        self.assertEqual(client_spec, res)

    def test_reconcile_specs(self):
        base_objs = [k8s.V1Container(name='base_container1', image='base_image')]
        client_objs = [k8s.V1Container(name='client_container1')]
        base_spec = k8s.V1PodSpec(priority=1, active_deadline_seconds=100, containers=base_objs)
        client_spec = k8s.V1PodSpec(priority=2, hostname='local', containers=client_objs)
        res = PodGenerator.reconcile_specs(base_spec, client_spec)
        client_spec.containers = [k8s.V1Container(name='client_container1', image='base_image')]
        client_spec.active_deadline_seconds = 100
        self.assertEqual(client_spec, res)

    def test_deserialize_model_file(self):

        fixture = sys.path[0] + '/tests/kubernetes/pod.yaml'
        result = PodGenerator.deserialize_model_file(fixture)
        sanitized_res = self.k8s_client.sanitize_for_serialization(result)
        self.assertEqual(sanitized_res, self.deserialize_result)

    def test_deserialize_model_string(self):
        fixture = """
apiVersion: v1
kind: Pod
metadata:
  name: memory-demo
  namespace: mem-example
spec:
  containers:
    - name: memory-demo-ctr
      image: polinux/stress
      resources:
        limits:
          memory: "200Mi"
        requests:
          memory: "100Mi"
      command: ["stress"]
      args: ["--vm", "1", "--vm-bytes", "150M", "--vm-hang", "1"]
        """
        result = PodGenerator.deserialize_model_file(fixture)
        sanitized_res = self.k8s_client.sanitize_for_serialization(result)
        self.assertEqual(sanitized_res, self.deserialize_result)

    def test_validate_pod_generator(self):
        with self.assertRaises(AirflowConfigException):
            PodGenerator(image='k', pod=k8s.V1Pod())
        with self.assertRaises(AirflowConfigException):
            PodGenerator(pod=k8s.V1Pod(), pod_template_file='k')
        with self.assertRaises(AirflowConfigException):
            PodGenerator(image='k', pod_template_file='k')

        PodGenerator(image='k')
        PodGenerator(pod_template_file='tests/kubernetes/pod.yaml')
        PodGenerator(pod=k8s.V1Pod())

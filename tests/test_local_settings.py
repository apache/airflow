# -*- coding: utf-8 -*-
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
import os
import sys
import tempfile
import unittest
from kubernetes.client import ApiClient
import kubernetes.client.models as k8s #noqa
from tests.compat import MagicMock, Mock, call, patch


SETTINGS_FILE_POLICY = """
def test_policy(task_instance):
    task_instance.run_as_user = "myself"
"""

SETTINGS_FILE_POLICY_WITH_DUNDER_ALL = """
__all__ = ["test_policy"]

def test_policy(task_instance):
    task_instance.run_as_user = "myself"

def not_policy():
    print("This shouldn't be imported")
"""

SETTINGS_FILE_POD_MUTATION_HOOK = """
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.pod import Port, Resources

def pod_mutation_hook(pod):
    pod.namespace = 'airflow-tests'
    pod.image = 'my_image'
    pod.volumes.append(Volume(name="bar", configs={}))
    pod.ports = [Port(container_port=8080)]
    pod.resources = Resources(
                    request_memory="2G",
                    request_cpu="200Mi",
                    limit_gpu="200G"
                )

"""

SETTINGS_FILE_POD_MUTATION_HOOK_V1_POD = """
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.pod import Port, Resources

def pod_mutation_hook(pod):
    pod.spec.containers[0].image = "test-image"

"""


def pod_mutation_hook(v1pod):
    """

    @param v1pod:
    :type v1pod: k8s.V1Pod
    @return:
    """
    v1pod.spec.containers[0].image = "test-image"

class SettingsContext:
    def __init__(self, content, module_name):
        self.content = content
        self.settings_root = tempfile.mkdtemp()
        filename = "{}.py".format(module_name)
        self.settings_file = os.path.join(self.settings_root, filename)

    def __enter__(self):
        with open(self.settings_file, 'w') as handle:
            handle.writelines(self.content)
        sys.path.append(self.settings_root)
        return self.settings_file

    def __exit__(self, *exc_info):
        sys.path.remove(self.settings_root)


class LocalSettingsTest(unittest.TestCase):
    # Make sure that the configure_logging is not cached
    def setUp(self):
        self.old_modules = dict(sys.modules)

    def tearDown(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

    @patch("airflow.settings.import_local_settings")
    @patch("airflow.settings.prepare_syspath")
    def test_initialize_order(self, prepare_syspath, import_local_settings):
        """
        Tests that import_local_settings is called after prepare_classpath
        """
        mock = Mock()
        mock.attach_mock(prepare_syspath, "prepare_syspath")
        mock.attach_mock(import_local_settings, "import_local_settings")

        import airflow.settings
        airflow.settings.initialize()

        mock.assert_has_calls([call.prepare_syspath(), call.import_local_settings()])

    def test_import_with_dunder_all_not_specified(self):
        """
        Tests that if __all__ is specified in airflow_local_settings,
        only module attributes specified within are imported.
        """
        with SettingsContext(SETTINGS_FILE_POLICY_WITH_DUNDER_ALL, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            with self.assertRaises(AttributeError):
                settings.not_policy()

    def test_import_with_dunder_all(self):
        """
        Tests that if __all__ is specified in airflow_local_settings,
        only module attributes specified within are imported.
        """
        with SettingsContext(SETTINGS_FILE_POLICY_WITH_DUNDER_ALL, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            task_instance = MagicMock()
            settings.test_policy(task_instance)

            assert task_instance.run_as_user == "myself"

    @patch("airflow.settings.log.debug")
    def test_import_local_settings_without_syspath(self, log_mock):
        """
        Tests that an ImportError is raised in import_local_settings
        if there is no airflow_local_settings module on the syspath.
        """
        from airflow import settings
        settings.import_local_settings()
        log_mock.assert_called_with("Failed to import airflow_local_settings.", exc_info=True)

    def test_policy_function(self):
        """
        Tests that task instances are mutated by the policy
        function in airflow_local_settings.
        """
        with SettingsContext(SETTINGS_FILE_POLICY, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            task_instance = MagicMock()
            settings.test_policy(task_instance)

            assert task_instance.run_as_user == "myself"

    def test_pod_mutation_hook(self):
        """
        Tests that pods are mutated by the pod_mutation_hook
        function in airflow_local_settings.
        """
        with SettingsContext(SETTINGS_FILE_POD_MUTATION_HOOK, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore

            pod = MagicMock()
            settings.pod_mutation_hook(pod)

            assert pod.namespace == 'airflow-tests'
            self.assertEqual(pod.volumes[0].name, "foo")

    def test_pod_mutation_to_k8s_pod(self):
        with SettingsContext(SETTINGS_FILE_POD_MUTATION_HOOK, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore
            from airflow.kubernetes_deprecated.pod import Pod as DeprecatedPod
            from airflow.kubernetes.volume import Volume
            from airflow.kubernetes.volume_mount import VolumeMount
            from airflow.kubernetes.pod import Resources
            pod = DeprecatedPod(
                image="foo",
                name="bar",
                namespace="baz",
                image_pull_policy="Never",
                envs=[],
                cmds=["airflow"],
                resources=Resources(
                    request_memory="1G",
                    request_cpu="100Mi",
                    limit_gpu="100G"
                ),
                volumes=[Volume(name="foo", configs={})],
                volume_mounts=[VolumeMount(name="foo", mount_path="/mnt", sub_path="/", read_only=True)]
            )

            # pod = Pod(image="", cmds=[], envs={}, name="my-name")
            settings.pod_mutation_hook(pod)
            result = pod.to_v1_kubernetes_pod()
            k8s_client = ApiClient()

            result = k8s_client.sanitize_for_serialization(result)
            expected = \
                {
                    'metadata':
                        {
                            'labels': {},
                            'name': 'bar',
                            'namespace': 'airflow-tests'
                        },
                    'spec':
                        {'containers':
                            [
                                {
                                    'args': [],
                                    'command': ['airflow'],
                                    'env': {},
                                    'image': 'my_image',
                                    'imagePullPolicy': 'Never',
                                    'ports': [{'containerPort': 8080}],
                                    'name': 'base',
                                    'volumeMounts':
                                        [
                                            {
                                                'mountPath': '/mnt',
                                                'name': 'foo',
                                                'readOnly': True, 'subPath': '/'
                                            }
                                        ],  # noqa
                                    'resources':
                                        {
                                            'limits':
                                                {
                                                    'cpu': None,
                                                    'memory': None,
                                                    'nvidia.com/gpu': '200G',
                                                    'ephemeral-storage': None
                                                },
                                            'requests':
                                                {
                                                    'cpu': '200Mi',
                                                    'memory': '2G',
                                                    'ephemeral-storage': None
                                                }
                                        }  # noqa
                                }
                            ],
                            'hostNetwork': False,
                            'tolerations': [],
                            'volumes': [
                                {'name': 'foo'},
                                {'name': 'bar'}
                            ]
                        }  # noqa
                }
            self.assertEquals(expected, result)

    def test_pod_mutation_v1_pod(self):
        with SettingsContext(SETTINGS_FILE_POD_MUTATION_HOOK_V1_POD, "airflow_local_settings"):
            from airflow import settings
            settings.import_local_settings()  # pylint: ignore
            from airflow.kubernetes_deprecated.pod import Pod as DeprecatedPod
            from airflow.kubernetes.volume import Volume
            from airflow.kubernetes.volume_mount import VolumeMount
            from airflow.kubernetes.pod import Resources
            pod = DeprecatedPod(
                image="foo",
                name="bar",
                namespace="baz",
                image_pull_policy="Never",
                envs=[],
                cmds=["airflow"],
                resources=Resources(
                    request_memory="1G",
                    request_cpu="100Mi",
                    limit_gpu="100G"
                ),
                volumes=[Volume(name="foo", configs={})],
                volume_mounts=[VolumeMount(name="foo", mount_path="/mnt", sub_path="/", read_only=True)]
            )

            with self.assertRaises(AttributeError) as context:
                self.assertRaises(settings.pod_mutation_hook(pod))

class TestStatsWithAllowList(unittest.TestCase):

    def setUp(self):
        from airflow.settings import SafeStatsdLogger, AllowListValidator
        self.statsd_client = Mock()
        self.stats = SafeStatsdLogger(self.statsd_client, AllowListValidator("stats_one, stats_two"))

    def test_increment_counter_with_allowed_key(self):
        self.stats.incr('stats_one')
        self.statsd_client.incr.assert_called_once_with('stats_one', 1, 1)

    def test_increment_counter_with_allowed_prefix(self):
        self.stats.incr('stats_two.bla')
        self.statsd_client.incr.assert_called_once_with('stats_two.bla', 1, 1)

    def test_not_increment_counter_if_not_allowed(self):
        self.stats.incr('stats_three')
        self.statsd_client.assert_not_called()

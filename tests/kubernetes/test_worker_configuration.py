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

import unittest
from unittest.mock import ANY

import mock
from parameterized import parameterized

from tests.test_utils.config import conf_vars

try:
    from airflow.executors.kubernetes_executor import AirflowKubernetesScheduler
    from airflow.executors.kubernetes_executor import KubeConfig
    from airflow.kubernetes.worker_configuration import WorkerConfiguration
    from airflow.kubernetes.pod_generator import PodGenerator
    from airflow.exceptions import AirflowConfigException
    from airflow.kubernetes.secret import Secret
    from airflow.version import version as airflow_version
    import kubernetes.client.models as k8s
    from kubernetes.client.api_client import ApiClient
except ImportError:
    AirflowKubernetesScheduler = None  # type: ignore


class TestKubernetesWorkerConfiguration(unittest.TestCase):
    """
    Tests that if dags_volume_subpath/logs_volume_subpath configuration
    options are passed to worker pod config
    """

    affinity_config = {
        'podAntiAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': [
                {
                    'topologyKey': 'kubernetes.io/hostname',
                    'labelSelector': {
                        'matchExpressions': [
                            {
                                'key': 'app',
                                'operator': 'In',
                                'values': ['airflow']
                            }
                        ]
                    }
                }
            ]
        }
    }

    tolerations_config = [
        {
            'key': 'dedicated',
            'operator': 'Equal',
            'value': 'airflow'
        },
        {
            'key': 'prod',
            'operator': 'Exists'
        }
    ]

    worker_annotations_config = {
        'iam.amazonaws.com/role': 'role-arn',
        'other/annotation': 'value'
    }

    def setUp(self):
        if AirflowKubernetesScheduler is None:
            self.skipTest("kubernetes python package is not installed")

        self.kube_config = mock.MagicMock()
        self.kube_config.airflow_home = '/'
        self.kube_config.airflow_dags = 'dags'
        self.kube_config.airflow_logs = 'logs'
        self.kube_config.dags_volume_subpath = None
        self.kube_config.dags_volume_mount_point = None
        self.kube_config.logs_volume_subpath = None
        self.kube_config.dags_in_image = False
        self.kube_config.dags_folder = None
        self.kube_config.git_dags_folder_mount_point = None
        self.kube_config.kube_labels = {'dag_id': 'original_dag_id', 'my_label': 'label_id'}
        self.kube_config.pod_template_file = ''
        self.kube_config.restart_policy = ''
        self.kube_config.image_pull_policy = ''
        self.api_client = ApiClient()

    def tearDown(self) -> None:
        self.kube_config = None

    def test_worker_configuration_no_subpaths(self):
        self.kube_config.dags_volume_claim = 'airflow-dags'
        self.kube_config.dags_folder = 'dags'
        worker_config = WorkerConfiguration(self.kube_config)
        volumes = worker_config._get_volumes()
        volume_mounts = worker_config._get_volume_mounts()
        for volume_or_mount in volumes + volume_mounts:
            if volume_or_mount.name not in ['airflow-config', 'airflow-local-settings']:
                self.assertNotIn(
                    'subPath', self.api_client.sanitize_for_serialization(volume_or_mount),
                    "subPath shouldn't be defined"
                )

    @conf_vars({
        ('kubernetes', 'git_ssh_known_hosts_configmap_name'): 'airflow-configmap',
        ('kubernetes', 'git_ssh_key_secret_name'): 'airflow-secrets',
        ('kubernetes', 'git_user'): 'some-user',
        ('kubernetes', 'git_password'): 'some-password',
        ('kubernetes', 'git_repo'): 'git@github.com:apache/airflow.git',
        ('kubernetes', 'git_branch'): 'master',
        ('kubernetes', 'git_dags_folder_mount_point'): '/usr/local/airflow/dags',
        ('kubernetes', 'delete_worker_pods'): 'True',
        ('kubernetes', 'kube_client_request_args'): '{"_request_timeout" : [60,360]}',
    })
    def test_worker_configuration_auth_both_ssh_and_user(self):
        with self.assertRaisesRegex(AirflowConfigException,
                                    'either `git_user` and `git_password`.*'
                                    'or `git_ssh_key_secret_name`.*'
                                    'but not both$'):
            KubeConfig()

    @parameterized.expand([
        ('{"grace_period_seconds": 10}', {"grace_period_seconds": 10}),
        ("", {})
    ])
    def test_delete_option_kwargs_config(self, config, expected_value):
        with conf_vars({
            ('kubernetes', 'delete_option_kwargs'): config,
        }):
            self.assertEqual(KubeConfig().delete_option_kwargs, expected_value)

    def test_worker_with_subpaths(self):
        self.kube_config.dags_volume_subpath = 'dags'
        self.kube_config.logs_volume_subpath = 'logs'
        self.kube_config.dags_volume_claim = 'dags'
        self.kube_config.dags_folder = 'dags'
        worker_config = WorkerConfiguration(self.kube_config)
        volumes = worker_config._get_volumes()
        volume_mounts = worker_config._get_volume_mounts()

        for volume in volumes:
            self.assertNotIn(
                'subPath', self.api_client.sanitize_for_serialization(volume),
                "subPath isn't valid configuration for a volume"
            )

        for volume_mount in volume_mounts:
            if volume_mount.name != 'airflow-config':
                self.assertIn(
                    'subPath', self.api_client.sanitize_for_serialization(volume_mount),
                    "subPath should've been passed to volumeMount configuration"
                )

    def test_worker_generate_dag_volume_mount_path(self):
        self.kube_config.git_dags_folder_mount_point = '/root/airflow/git/dags'
        self.kube_config.dags_folder = '/root/airflow/dags'
        worker_config = WorkerConfiguration(self.kube_config)

        self.kube_config.dags_volume_claim = 'airflow-dags'
        self.kube_config.dags_volume_host = ''
        dag_volume_mount_path = worker_config.generate_dag_volume_mount_path()
        self.assertEqual(dag_volume_mount_path, self.kube_config.dags_folder)

        self.kube_config.dags_volume_mount_point = '/root/airflow/package'
        dag_volume_mount_path = worker_config.generate_dag_volume_mount_path()
        self.assertEqual(dag_volume_mount_path, '/root/airflow/package')
        self.kube_config.dags_volume_mount_point = ''

        self.kube_config.dags_volume_claim = ''
        self.kube_config.dags_volume_host = '/host/airflow/dags'
        dag_volume_mount_path = worker_config.generate_dag_volume_mount_path()
        self.assertEqual(dag_volume_mount_path, self.kube_config.dags_folder)

        self.kube_config.dags_volume_claim = ''
        self.kube_config.dags_volume_host = ''
        dag_volume_mount_path = worker_config.generate_dag_volume_mount_path()
        self.assertEqual(dag_volume_mount_path,
                         self.kube_config.git_dags_folder_mount_point)

    def test_worker_environment_no_dags_folder(self):
        self.kube_config.airflow_configmap = ''
        self.kube_config.git_dags_folder_mount_point = ''
        self.kube_config.dags_folder = ''
        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertNotIn('AIRFLOW__CORE__DAGS_FOLDER', env)

    def test_worker_environment_when_dags_folder_specified(self):
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_dags_folder_mount_point = ''
        dags_folder = '/workers/path/to/dags'
        self.kube_config.dags_folder = dags_folder

        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertEqual(dags_folder, env['AIRFLOW__CORE__DAGS_FOLDER'])

    def test_worker_environment_dags_folder_using_git_sync(self):
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_sync_dest = 'repo'
        self.kube_config.git_subpath = 'dags'
        self.kube_config.git_dags_folder_mount_point = '/workers/path/to/dags'

        dags_folder = '{}/{}/{}'.format(self.kube_config.git_dags_folder_mount_point,
                                        self.kube_config.git_sync_dest,
                                        self.kube_config.git_subpath)

        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()

        self.assertEqual(dags_folder, env['AIRFLOW__CORE__DAGS_FOLDER'])

    def test_init_environment_using_git_sync_ssh_without_known_hosts(self):
        # Tests the init environment created with git-sync SSH authentication option is correct
        # without known hosts file
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_ssh_secret_name = 'airflow-secrets'
        self.kube_config.git_ssh_known_hosts_configmap_name = None
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None

        worker_config = WorkerConfiguration(self.kube_config)
        init_containers = worker_config._get_init_containers()

        self.assertTrue(init_containers)  # check not empty
        env = init_containers[0].env

        self.assertIn(k8s.V1EnvVar(name='GIT_SSH_KEY_FILE', value='/etc/git-secret/ssh'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_ADD_USER', value='true'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_KNOWN_HOSTS', value='false'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_SSH', value='true'), env)

    def test_init_environment_using_git_sync_ssh_with_known_hosts(self):
        # Tests the init environment created with git-sync SSH authentication option is correct
        # with known hosts file
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_ssh_key_secret_name = 'airflow-secrets'
        self.kube_config.git_ssh_known_hosts_configmap_name = 'airflow-configmap'
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None

        worker_config = WorkerConfiguration(self.kube_config)
        init_containers = worker_config._get_init_containers()

        self.assertTrue(init_containers)  # check not empty
        env = init_containers[0].env

        self.assertIn(k8s.V1EnvVar(name='GIT_SSH_KEY_FILE', value='/etc/git-secret/ssh'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_ADD_USER', value='true'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_KNOWN_HOSTS', value='true'), env)
        self.assertIn(k8s.V1EnvVar(
            name='GIT_SSH_KNOWN_HOSTS_FILE',
            value='/etc/git-secret/known_hosts'
        ), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_SSH', value='true'), env)

    def test_init_environment_using_git_sync_user_without_known_hosts(self):
        # Tests the init environment created with git-sync User authentication option is correct
        # without known hosts file
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_user = 'git_user'
        self.kube_config.git_password = 'git_password'
        self.kube_config.git_ssh_known_hosts_configmap_name = None
        self.kube_config.git_ssh_key_secret_name = None
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None

        worker_config = WorkerConfiguration(self.kube_config)
        init_containers = worker_config._get_init_containers()

        self.assertTrue(init_containers)  # check not empty
        env = init_containers[0].env

        self.assertNotIn(k8s.V1EnvVar(name='GIT_SSH_KEY_FILE', value='/etc/git-secret/ssh'), env)
        self.assertNotIn(k8s.V1EnvVar(name='GIT_SYNC_ADD_USER', value='true'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_USERNAME', value='git_user'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_PASSWORD', value='git_password'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_KNOWN_HOSTS', value='false'), env)
        self.assertNotIn(k8s.V1EnvVar(
            name='GIT_SSH_KNOWN_HOSTS_FILE',
            value='/etc/git-secret/known_hosts'
        ), env)
        self.assertNotIn(k8s.V1EnvVar(name='GIT_SYNC_SSH', value='true'), env)

    def test_init_environment_using_git_sync_user_with_known_hosts(self):
        # Tests the init environment created with git-sync User authentication option is correct
        # with known hosts file
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_user = 'git_user'
        self.kube_config.git_password = 'git_password'
        self.kube_config.git_ssh_known_hosts_configmap_name = 'airflow-configmap'
        self.kube_config.git_ssh_key_secret_name = None
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None

        worker_config = WorkerConfiguration(self.kube_config)
        init_containers = worker_config._get_init_containers()

        self.assertTrue(init_containers)  # check not empty
        env = init_containers[0].env

        self.assertNotIn(k8s.V1EnvVar(name='GIT_SSH_KEY_FILE', value='/etc/git-secret/ssh'), env)
        self.assertNotIn(k8s.V1EnvVar(name='GIT_SYNC_ADD_USER', value='true'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_USERNAME', value='git_user'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_SYNC_PASSWORD', value='git_password'), env)
        self.assertIn(k8s.V1EnvVar(name='GIT_KNOWN_HOSTS', value='true'), env)
        self.assertIn(k8s.V1EnvVar(
            name='GIT_SSH_KNOWN_HOSTS_FILE',
            value='/etc/git-secret/known_hosts'
        ), env)
        self.assertNotIn(k8s.V1EnvVar(name='GIT_SYNC_SSH', value='true'), env)

    def test_init_environment_using_git_sync_run_as_user_empty(self):
        # Tests if git_syn_run_as_user is none, then no securityContext created in init container

        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None
        self.kube_config.git_sync_run_as_user = ''

        worker_config = WorkerConfiguration(self.kube_config)
        init_containers = worker_config._get_init_containers()
        self.assertTrue(init_containers)  # check not empty

        self.assertIsNone(init_containers[0].security_context)

    def test_init_environment_using_git_sync_run_as_user_root(self):
        # Tests if git_syn_run_as_user is '0', securityContext is created with
        # the right uid

        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None
        self.kube_config.git_sync_run_as_user = 0

        worker_config = WorkerConfiguration(self.kube_config)
        init_containers = worker_config._get_init_containers()
        self.assertTrue(init_containers)  # check not empty

        self.assertEqual(0, init_containers[0].security_context.run_as_user)

    def test_make_pod_run_as_user_0(self):
        # Tests the pod created with run-as-user 0 actually gets that in it's config
        self.kube_config.worker_run_as_user = 0
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None
        self.kube_config.worker_fs_group = None
        self.kube_config.git_dags_folder_mount_point = 'dags'
        self.kube_config.git_sync_dest = 'repo'
        self.kube_config.git_subpath = 'path'

        worker_config = WorkerConfiguration(self.kube_config)
        pod = worker_config.as_pod()

        self.assertEqual(0, pod.spec.security_context.run_as_user)

    def test_make_pod_assert_labels(self):
        # Tests the pod created has all the expected labels set
        self.kube_config.dags_folder = 'dags'

        worker_config = WorkerConfiguration(self.kube_config)
        pod = PodGenerator.construct_pod(
            "test_dag_id",
            "test_task_id",
            "test_pod_id",
            1,
            "2019-11-21 11:08:22.920875",
            ["bash -c 'ls /'"],
            None,
            worker_config.as_pod(),
            "default",
            "sample-uuid",
        )
        expected_labels = {
            'airflow-worker': 'sample-uuid',
            'airflow_version': airflow_version.replace('+', '-'),
            'dag_id': 'test_dag_id',
            'execution_date': '2019-11-21 11:08:22.920875',
            'kubernetes_executor': 'True',
            'task_id': 'test_task_id',
            'try_number': '1'
        }
        self.assertEqual(pod.metadata.labels, expected_labels)

    def test_make_pod_git_sync_ssh_without_known_hosts(self):
        # Tests the pod created with git-sync SSH authentication option is correct without known hosts
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_ssh_key_secret_name = 'airflow-secrets'
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None
        self.kube_config.worker_fs_group = None
        self.kube_config.git_dags_folder_mount_point = 'dags'
        self.kube_config.git_sync_dest = 'repo'
        self.kube_config.git_subpath = 'path'

        worker_config = WorkerConfiguration(self.kube_config)

        pod = worker_config.as_pod()

        init_containers = worker_config._get_init_containers()
        git_ssh_key_file = next((x.value for x in init_containers[0].env
                                if x.name == 'GIT_SSH_KEY_FILE'), None)
        volume_mount_ssh_key = next((x.mount_path for x in init_containers[0].volume_mounts
                                    if x.name == worker_config.git_sync_ssh_secret_volume_name),
                                    None)
        self.assertTrue(git_ssh_key_file)
        self.assertTrue(volume_mount_ssh_key)
        self.assertEqual(65533, pod.spec.security_context.fs_group)
        self.assertEqual(git_ssh_key_file,
                         volume_mount_ssh_key,
                         'The location where the git ssh secret is mounted'
                         ' needs to be the same as the GIT_SSH_KEY_FILE path')

    def test_make_pod_git_sync_credentials_secret(self):
        # Tests the pod created with git_sync_credentials_secret will get into the init container
        self.kube_config.git_sync_credentials_secret = 'airflow-git-creds-secret'
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None
        self.kube_config.worker_fs_group = None
        self.kube_config.git_dags_folder_mount_point = 'dags'
        self.kube_config.git_sync_dest = 'repo'
        self.kube_config.git_subpath = 'path'

        worker_config = WorkerConfiguration(self.kube_config)

        pod = worker_config.as_pod()

        username_env = k8s.V1EnvVar(
            name='GIT_SYNC_USERNAME',
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=self.kube_config.git_sync_credentials_secret,
                    key='GIT_SYNC_USERNAME')
            )
        )
        password_env = k8s.V1EnvVar(
            name='GIT_SYNC_PASSWORD',
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=self.kube_config.git_sync_credentials_secret,
                    key='GIT_SYNC_PASSWORD')
            )
        )

        self.assertIn(username_env, pod.spec.init_containers[0].env,
                      'The username env for git credentials did not get into the init container')

        self.assertIn(password_env, pod.spec.init_containers[0].env,
                      'The password env for git credentials did not get into the init container')

    def test_make_pod_git_sync_rev(self):
        # Tests the pod created with git_sync_credentials_secret will get into the init container
        self.kube_config.git_sync_rev = 'sampletag'
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None
        self.kube_config.worker_fs_group = None
        self.kube_config.git_dags_folder_mount_point = 'dags'
        self.kube_config.git_sync_dest = 'repo'
        self.kube_config.git_subpath = 'path'

        worker_config = WorkerConfiguration(self.kube_config)

        pod = worker_config.as_pod()

        rev_env = k8s.V1EnvVar(
            name='GIT_SYNC_REV',
            value=self.kube_config.git_sync_rev,
        )

        self.assertIn(rev_env, pod.spec.init_containers[0].env,
                      'The git_sync_rev env did not get into the init container')

    def test_make_pod_git_sync_ssh_with_known_hosts(self):
        # Tests the pod created with git-sync SSH authentication option is correct with known hosts
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.git_ssh_secret_name = 'airflow-secrets'
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None

        worker_config = WorkerConfiguration(self.kube_config)

        init_containers = worker_config._get_init_containers()
        git_ssh_known_hosts_file = next((x.value for x in init_containers[0].env
                                         if x.name == 'GIT_SSH_KNOWN_HOSTS_FILE'), None)

        volume_mount_ssh_known_hosts_file = next(
            (x.mount_path for x in init_containers[0].volume_mounts
             if x.name == worker_config.git_sync_ssh_known_hosts_volume_name),
            None)
        self.assertTrue(git_ssh_known_hosts_file)
        self.assertTrue(volume_mount_ssh_known_hosts_file)
        self.assertEqual(git_ssh_known_hosts_file,
                         volume_mount_ssh_known_hosts_file,
                         'The location where the git known hosts file is mounted'
                         ' needs to be the same as the GIT_SSH_KNOWN_HOSTS_FILE path')

    def test_make_pod_with_empty_executor_config(self):
        self.kube_config.kube_affinity = self.affinity_config
        self.kube_config.kube_tolerations = self.tolerations_config
        self.kube_config.kube_annotations = self.worker_annotations_config
        self.kube_config.dags_folder = 'dags'
        worker_config = WorkerConfiguration(self.kube_config)
        pod = worker_config.as_pod()

        self.assertTrue(pod.spec.affinity['podAntiAffinity'] is not None)
        self.assertEqual('app',
                         pod.spec.affinity['podAntiAffinity']
                         ['requiredDuringSchedulingIgnoredDuringExecution'][0]
                         ['labelSelector']
                         ['matchExpressions'][0]
                         ['key'])

        self.assertEqual(2, len(pod.spec.tolerations))
        self.assertEqual('prod', pod.spec.tolerations[1]['key'])
        self.assertEqual('role-arn', pod.metadata.annotations['iam.amazonaws.com/role'])
        self.assertEqual('value', pod.metadata.annotations['other/annotation'])

    def test_make_pod_with_executor_config(self):
        self.kube_config.dags_folder = 'dags'
        worker_config = WorkerConfiguration(self.kube_config)
        config_pod = PodGenerator(
            image='',
            affinity=self.affinity_config,
            tolerations=self.tolerations_config,
        ).gen_pod()

        pod = worker_config.as_pod()

        result = PodGenerator.reconcile_pods(pod, config_pod)

        self.assertTrue(result.spec.affinity['podAntiAffinity'] is not None)
        self.assertEqual('app',
                         result.spec.affinity['podAntiAffinity']
                         ['requiredDuringSchedulingIgnoredDuringExecution'][0]
                         ['labelSelector']
                         ['matchExpressions'][0]
                         ['key'])

        self.assertEqual(2, len(result.spec.tolerations))
        self.assertEqual('prod', result.spec.tolerations[1]['key'])

    def test_worker_pvc_dags(self):
        # Tests persistence volume config created when `dags_volume_claim` is set
        self.kube_config.dags_volume_claim = 'airflow-dags'
        self.kube_config.dags_folder = 'dags'
        worker_config = WorkerConfiguration(self.kube_config)
        volumes = worker_config._get_volumes()
        volume_mounts = worker_config._get_volume_mounts()

        init_containers = worker_config._get_init_containers()

        dag_volume = [volume for volume in volumes if volume.name == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts if mount.name == 'airflow-dags']

        self.assertEqual('airflow-dags', dag_volume[0].persistent_volume_claim.claim_name)
        self.assertEqual(1, len(dag_volume_mount))
        self.assertTrue(dag_volume_mount[0].read_only)
        self.assertEqual(0, len(init_containers))

    def test_worker_git_dags(self):
        # Tests persistence volume config created when `git_repo` is set
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_folder = '/usr/local/airflow/dags'
        self.kube_config.worker_dags_folder = '/usr/local/airflow/dags'

        self.kube_config.git_sync_container_repository = 'gcr.io/google-containers/git-sync-amd64'
        self.kube_config.git_sync_container_tag = 'v2.0.5'
        self.kube_config.git_sync_container = 'gcr.io/google-containers/git-sync-amd64:v2.0.5'
        self.kube_config.git_sync_init_container_name = 'git-sync-clone'
        self.kube_config.git_subpath = 'dags_folder'
        self.kube_config.git_sync_root = '/git'
        self.kube_config.git_sync_run_as_user = 65533
        self.kube_config.git_dags_folder_mount_point = '/usr/local/airflow/dags/repo/dags_folder'

        worker_config = WorkerConfiguration(self.kube_config)
        volumes = worker_config._get_volumes()
        volume_mounts = worker_config._get_volume_mounts()

        dag_volume = [volume for volume in volumes if volume.name == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts if mount.name == 'airflow-dags']

        self.assertIsNotNone(dag_volume[0].empty_dir)
        self.assertEqual(self.kube_config.git_dags_folder_mount_point, dag_volume_mount[0].mount_path)
        self.assertTrue(dag_volume_mount[0].read_only)

        init_container = worker_config._get_init_containers()[0]
        init_container_volume_mount = [mount for mount in init_container.volume_mounts
                                       if mount.name == 'airflow-dags']

        self.assertEqual('git-sync-clone', init_container.name)
        self.assertEqual('gcr.io/google-containers/git-sync-amd64:v2.0.5', init_container.image)
        self.assertEqual(1, len(init_container_volume_mount))
        self.assertFalse(init_container_volume_mount[0].read_only)
        self.assertEqual(65533, init_container.security_context.run_as_user)

    def test_worker_container_dags(self):
        # Tests that the 'airflow-dags' persistence volume is NOT created when `dags_in_image` is set
        self.kube_config.dags_in_image = True
        self.kube_config.dags_folder = 'dags'
        worker_config = WorkerConfiguration(self.kube_config)
        volumes = worker_config._get_volumes()
        volume_mounts = worker_config._get_volume_mounts()

        dag_volume = [volume for volume in volumes if volume.name == 'airflow-dags']
        dag_volume_mount = [mount for mount in volume_mounts if mount.name == 'airflow-dags']

        init_containers = worker_config._get_init_containers()

        self.assertEqual(0, len(dag_volume))
        self.assertEqual(0, len(dag_volume_mount))
        self.assertEqual(0, len(init_containers))

    def test_set_airflow_config_configmap(self):
        """
        Test that airflow.cfg can be set via configmap by
        checking volume & volume-mounts are set correctly.
        """
        self.kube_config.airflow_home = '/usr/local/airflow'
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.airflow_local_settings_configmap = None
        self.kube_config.dags_folder = '/workers/path/to/dags'

        worker_config = WorkerConfiguration(self.kube_config)
        pod = worker_config.as_pod()

        pod_spec_dict = pod.spec.to_dict()

        airflow_config_volume = [
            volume for volume in pod_spec_dict['volumes'] if volume["name"] == 'airflow-config'
        ]
        # Test that volume_name is found
        self.assertEqual(1, len(airflow_config_volume))

        # Test that config map exists
        self.assertEqual(
            {'default_mode': None, 'items': None, 'name': 'airflow-configmap', 'optional': None},
            airflow_config_volume[0]['config_map']
        )

        # Test that only 1 Volume Mounts exists with 'airflow-config' name
        # One for airflow.cfg
        volume_mounts = [
            volume_mount for volume_mount in pod_spec_dict['containers'][0]['volume_mounts']
            if volume_mount['name'] == 'airflow-config'
        ]

        self.assertEqual(
            [
                {
                    'mount_path': '/usr/local/airflow/airflow.cfg',
                    'mount_propagation': None,
                    'name': 'airflow-config',
                    'read_only': True,
                    'sub_path': 'airflow.cfg',
                    'sub_path_expr': None
                }
            ],
            volume_mounts
        )

    def test_set_airflow_local_settings_configmap(self):
        """
        Test that airflow_local_settings.py can be set via configmap by
        checking volume & volume-mounts are set correctly.
        """
        self.kube_config.airflow_home = '/usr/local/airflow'
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.airflow_local_settings_configmap = 'airflow-configmap'
        self.kube_config.dags_folder = '/workers/path/to/dags'

        worker_config = WorkerConfiguration(self.kube_config)
        pod = worker_config.as_pod()

        pod_spec_dict = pod.spec.to_dict()

        airflow_config_volume = [
            volume for volume in pod_spec_dict['volumes'] if volume["name"] == 'airflow-config'
        ]
        # Test that volume_name is found
        self.assertEqual(1, len(airflow_config_volume))

        # Test that config map exists
        self.assertEqual(
            {'default_mode': None, 'items': None, 'name': 'airflow-configmap', 'optional': None},
            airflow_config_volume[0]['config_map']
        )

        # Test that 2 Volume Mounts exists and has 2 different mount-paths
        # One for airflow.cfg
        # Second for airflow_local_settings.py
        volume_mounts = [
            volume_mount for volume_mount in pod_spec_dict['containers'][0]['volume_mounts']
            if volume_mount['name'] == 'airflow-config'
        ]
        self.assertEqual(2, len(volume_mounts))

        self.assertEqual(
            [
                {
                    'mount_path': '/usr/local/airflow/airflow.cfg',
                    'mount_propagation': None,
                    'name': 'airflow-config',
                    'read_only': True,
                    'sub_path': 'airflow.cfg',
                    'sub_path_expr': None
                },
                {
                    'mount_path': '/usr/local/airflow/config/airflow_local_settings.py',
                    'mount_propagation': None,
                    'name': 'airflow-config',
                    'read_only': True,
                    'sub_path': 'airflow_local_settings.py',
                    'sub_path_expr': None
                }
            ],
            volume_mounts
        )

    def test_set_airflow_configmap_different_for_local_setting(self):
        """
        Test that airflow_local_settings.py can be set via configmap by
        checking volume & volume-mounts are set correctly when using a different
        configmap than airflow_configmap (airflow.cfg)
        """
        self.kube_config.airflow_home = '/usr/local/airflow'
        self.kube_config.airflow_configmap = 'airflow-configmap'
        self.kube_config.airflow_local_settings_configmap = 'airflow-ls-configmap'
        self.kube_config.dags_folder = '/workers/path/to/dags'

        worker_config = WorkerConfiguration(self.kube_config)
        pod = worker_config.as_pod()

        pod_spec_dict = pod.spec.to_dict()

        airflow_local_settings_volume = [
            volume for volume in pod_spec_dict['volumes'] if volume["name"] == 'airflow-local-settings'
        ]
        # Test that volume_name is found
        self.assertEqual(1, len(airflow_local_settings_volume))

        # Test that config map exists
        self.assertEqual(
            {'default_mode': None, 'items': None, 'name': 'airflow-ls-configmap', 'optional': None},
            airflow_local_settings_volume[0]['config_map']
        )

        # Test that 2 Volume Mounts exists and has 2 different mount-paths
        # One for airflow.cfg
        # Second for airflow_local_settings.py
        airflow_cfg_volume_mount = [
            volume_mount for volume_mount in pod_spec_dict['containers'][0]['volume_mounts']
            if volume_mount['name'] == 'airflow-config'
        ]

        local_setting_volume_mount = [
            volume_mount for volume_mount in pod_spec_dict['containers'][0]['volume_mounts']
            if volume_mount['name'] == 'airflow-local-settings'
        ]
        self.assertEqual(1, len(airflow_cfg_volume_mount))
        self.assertEqual(1, len(local_setting_volume_mount))

        self.assertEqual(
            [
                {
                    'mount_path': '/usr/local/airflow/config/airflow_local_settings.py',
                    'mount_propagation': None,
                    'name': 'airflow-local-settings',
                    'read_only': True,
                    'sub_path': 'airflow_local_settings.py',
                    'sub_path_expr': None
                }
            ],
            local_setting_volume_mount
        )

        self.assertEqual(
            [
                {
                    'mount_path': '/usr/local/airflow/airflow.cfg',
                    'mount_propagation': None,
                    'name': 'airflow-config',
                    'read_only': True,
                    'sub_path': 'airflow.cfg',
                    'sub_path_expr': None
                }
            ],
            airflow_cfg_volume_mount
        )

    def test_kubernetes_environment_variables(self):
        # Tests the kubernetes environment variables get copied into the worker pods
        input_environment = {
            'ENVIRONMENT': 'prod',
            'LOG_LEVEL': 'warning'
        }
        self.kube_config.kube_env_vars = input_environment
        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()
        for key in input_environment:
            self.assertIn(key, env)
            self.assertIn(input_environment[key], env.values())

        core_executor = 'AIRFLOW__CORE__EXECUTOR'
        input_environment = {
            core_executor: 'NotLocalExecutor'
        }
        self.kube_config.kube_env_vars = input_environment
        worker_config = WorkerConfiguration(self.kube_config)
        env = worker_config._get_environment()
        self.assertEqual(env[core_executor], 'LocalExecutor')

    def test_get_secrets(self):
        # Test when secretRef is None and kube_secrets is not empty
        self.kube_config.kube_secrets = {
            'AWS_SECRET_KEY': 'airflow-secret=aws_secret_key',
            'POSTGRES_PASSWORD': 'airflow-secret=postgres_credentials'
        }
        self.kube_config.env_from_secret_ref = None
        worker_config = WorkerConfiguration(self.kube_config)
        secrets = worker_config._get_secrets()
        secrets.sort(key=lambda secret: secret.deploy_target)
        expected = [
            Secret('env', 'AWS_SECRET_KEY', 'airflow-secret', 'aws_secret_key'),
            Secret('env', 'POSTGRES_PASSWORD', 'airflow-secret', 'postgres_credentials')
        ]
        self.assertListEqual(expected, secrets)

        # Test when secret is not empty and kube_secrets is empty dict
        self.kube_config.kube_secrets = {}
        self.kube_config.env_from_secret_ref = 'secret_a,secret_b'
        worker_config = WorkerConfiguration(self.kube_config)
        secrets = worker_config._get_secrets()
        expected = [Secret('env', None, 'secret_a'), Secret('env', None, 'secret_b')]
        self.assertListEqual(expected, secrets)

    def test_get_env_from(self):
        # Test when configmap is empty
        self.kube_config.env_from_configmap_ref = ''
        worker_config = WorkerConfiguration(self.kube_config)
        configmaps = worker_config._get_env_from()
        self.assertListEqual([], configmaps)

        # test when configmap is not empty
        self.kube_config.env_from_configmap_ref = 'configmap_a,configmap_b'
        self.kube_config.env_from_secret_ref = 'secretref_a,secretref_b'
        worker_config = WorkerConfiguration(self.kube_config)
        configmaps = worker_config._get_env_from()
        self.assertListEqual([
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='configmap_a')),
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='configmap_b')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='secretref_a')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='secretref_b'))
        ], configmaps)

    def test_pod_template_file(self):
        fixture = 'tests/kubernetes/pod.yaml'
        self.kube_config.pod_template_file = fixture
        worker_config = WorkerConfiguration(self.kube_config)
        result = worker_config.as_pod()
        expected = PodGenerator.deserialize_model_file(fixture)
        expected.metadata.name = ANY
        self.assertEqual(expected, result)

    def test_get_labels(self):
        worker_config = WorkerConfiguration(self.kube_config)
        labels = worker_config._get_labels({'my_kube_executor_label': 'kubernetes'}, {
            'dag_id': 'override_dag_id',
        })
        self.assertEqual({
            'my_label': 'label_id',
            'dag_id': 'override_dag_id',
            'my_kube_executor_label': 'kubernetes',
        }, labels)

    def test_make_pod_with_image_pull_secrets(self):
        # Tests the pod created with image_pull_secrets actually gets that in it's config
        self.kube_config.dags_volume_claim = None
        self.kube_config.dags_volume_host = None
        self.kube_config.dags_in_image = None
        self.kube_config.git_dags_folder_mount_point = 'dags'
        self.kube_config.git_sync_dest = 'repo'
        self.kube_config.git_subpath = 'path'
        self.kube_config.image_pull_secrets = 'image_pull_secret1,image_pull_secret2'

        worker_config = WorkerConfiguration(self.kube_config)
        pod = worker_config.as_pod()

        self.assertEqual(2, len(pod.spec.image_pull_secrets))

    def test_get_resources(self):
        self.kube_config.worker_resources = {'limit_cpu': 0.25, 'limit_memory': '64Mi', 'request_cpu': '250m',
                                             'request_memory': '64Mi'}

        worker_config = WorkerConfiguration(self.kube_config)
        resources = worker_config._get_resources()
        self.assertEqual(resources.limits["cpu"], 0.25)
        self.assertEqual(resources.limits["memory"], "64Mi")
        self.assertEqual(resources.requests["cpu"], "250m")
        self.assertEqual(resources.requests["memory"], "64Mi")

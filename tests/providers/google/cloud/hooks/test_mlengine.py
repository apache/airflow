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
import json
import unittest
from copy import deepcopy
from unittest import mock
from unittest.mock import PropertyMock

import httplib2
from googleapiclient.errors import HttpError

from airflow.providers.google.cloud.hooks import mlengine as hook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
)


class TestMLEngineHook(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.hook = hook.MLEngineHook()

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.build")
    def test_mle_engine_client_creation(self, mock_build, mock_authorize):
        result = self.hook.get_conn()

        self.assertEqual(mock_build.return_value, result)
        mock_build.assert_called_with('ml', 'v1', http=mock_authorize.return_value, cache_discovery=False)

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_version(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        version_name = 'test-version'
        version = {'name': version_name, 'labels': {'other-label': 'test-value'}}
        version_with_airflow_version = {
            'name': 'test-version',
            'labels': {'other-label': 'test-value', 'airflow-version': hook._AIRFLOW_VERSION},
        }
        operation_path = f'projects/{project_id}/operations/test-operation'
        model_path = f'projects/{project_id}/models/{model_name}'
        operation_done = {'name': operation_path, 'done': True}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            create.return_value.
            execute.return_value
        ) = version
        (
            mock_get_conn.return_value.
            projects.return_value.
            operations.return_value.
            get.return_value.
            execute.return_value
        ) = {'name': operation_path, 'done': True}
        # fmt: on
        create_version_response = self.hook.create_version(
            project_id=project_id, model_name=model_name, version_spec=deepcopy(version)
        )

        self.assertEqual(create_version_response, operation_done)

        mock_get_conn.assert_has_calls(
            [
                mock.call()
                .projects()
                .models()
                .versions()
                .create(body=version_with_airflow_version, parent=model_path),
                mock.call().projects().models().versions().create().execute(num_retries=5),
                mock.call().projects().operations().get(name=version_name),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_version_with_labels(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        version_name = 'test-version'
        version = {'name': version_name}
        version_with_airflow_version = {
            'name': 'test-version',
            'labels': {'airflow-version': hook._AIRFLOW_VERSION},
        }
        operation_path = f'projects/{project_id}/operations/test-operation'
        model_path = f'projects/{project_id}/models/{model_name}'
        operation_done = {'name': operation_path, 'done': True}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            create.return_value.
            execute.return_value
        ) = version
        (
            mock_get_conn.return_value.
            projects.return_value.
            operations.return_value.
            get.return_value.
            execute.return_value
        ) = {'name': operation_path, 'done': True}

        create_version_response = self.hook.create_version(
            project_id=project_id,
            model_name=model_name,
            version_spec=deepcopy(version)
        )
        # fmt: on

        self.assertEqual(create_version_response, operation_done)

        mock_get_conn.assert_has_calls(
            [
                mock.call()
                .projects()
                .models()
                .versions()
                .create(body=version_with_airflow_version, parent=model_path),
                mock.call().projects().models().versions().create().execute(num_retries=5),
                mock.call().projects().operations().get(name=version_name),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_set_default_version(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        version_name = 'test-version'
        operation_path = f'projects/{project_id}/operations/test-operation'
        version_path = f'projects/{project_id}/models/{model_name}/versions/{version_name}'
        operation_done = {'name': operation_path, 'done': True}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            setDefault.return_value.
            execute.return_value
        ) = operation_done
        # fmt: on
        set_default_version_response = self.hook.set_default_version(
            project_id=project_id, model_name=model_name, version_name=version_name
        )

        self.assertEqual(set_default_version_response, operation_done)

        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().versions().setDefault(body={}, name=version_path),
                mock.call().projects().models().versions().setDefault().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.time.sleep")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_list_versions(self, mock_get_conn, mock_sleep):
        project_id = 'test-project'
        model_name = 'test-model'
        model_path = f'projects/{project_id}/models/{model_name}'
        version_names = [f'ver_{ix}' for ix in range(3)]
        response_bodies = [
            {'nextPageToken': f"TOKEN-{ix}", 'versions': [ver]} for ix, ver in enumerate(version_names)
        ]
        response_bodies[-1].pop('nextPageToken')

        pages_requests = [mock.Mock(**{'execute.return_value': body}) for body in response_bodies]
        versions_mock = mock.Mock(
            **{'list.return_value': pages_requests[0], 'list_next.side_effect': pages_requests[1:] + [None]}
        )
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value
        ) = versions_mock

        list_versions_response = self.hook.list_versions(
            project_id=project_id, model_name=model_name)
        # fmt: on
        self.assertEqual(list_versions_response, version_names)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().versions().list(pageSize=100, parent=model_path),
                mock.call().projects().models().versions().list().execute(num_retries=5),
            ]
            + [
                mock.call()
                .projects()
                .models()
                .versions()
                .list_next(previous_request=pages_requests[i], previous_response=response_bodies[i])
                for i in range(3)
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_delete_version(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        version_name = 'test-version'
        operation_path = f'projects/{project_id}/operations/test-operation'
        version_path = f'projects/{project_id}/models/{model_name}/versions/{version_name}'
        version = {'name': operation_path}
        operation_not_done = {'name': operation_path, 'done': False}
        operation_done = {'name': operation_path, 'done': True}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            operations.return_value.
            get.return_value.
            execute.side_effect
        ) = [operation_not_done, operation_done]

        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            delete.return_value.
            execute.return_value
        ) = version
        # fmt: on
        delete_version_response = self.hook.delete_version(
            project_id=project_id, model_name=model_name, version_name=version_name
        )

        self.assertEqual(delete_version_response, operation_done)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().versions().delete(name=version_path),
                mock.call().projects().models().versions().delete().execute(num_retries=5),
                mock.call().projects().operations().get(name=operation_path),
                mock.call().projects().operations().get().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_model(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        model = {
            'name': model_name,
        }
        model_with_airflow_version = {
            'name': model_name,
            'labels': {'airflow-version': hook._AIRFLOW_VERSION},
        }
        project_path = f'projects/{project_id}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            create.return_value.
            execute.return_value
        ) = model
        # fmt: on
        create_model_response = self.hook.create_model(project_id=project_id, model=deepcopy(model))

        self.assertEqual(create_model_response, model)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().create(body=model_with_airflow_version, parent=project_path),
                mock.call().projects().models().create().execute(num_retries=5),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_model_idempotency(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        model = {
            'name': model_name,
        }
        model_with_airflow_version = {
            'name': model_name,
            'labels': {'airflow-version': hook._AIRFLOW_VERSION},
        }
        project_path = f'projects/{project_id}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            create.return_value.
            execute.side_effect
        ) = [
            HttpError(
                resp=httplib2.Response({"status": 409}),
                content=json.dumps(
                    {
                        "error": {
                            "code": 409,
                            "message": "Field: model.name Error: A model with the same name already exists.",
                            "status": "ALREADY_EXISTS",
                            "details": [
                                {
                                    "@type": "type.googleapis.com/google.rpc.BadRequest",
                                    "fieldViolations": [
                                        {
                                            "field": "model.name",
                                            "description": "A model with the same name already exists."
                                        }
                                    ],
                                }
                            ],
                        }
                    }
                ).encode(),
            )
        ]

        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            get.return_value.
            execute.return_value
        ) = deepcopy(model)
        # fmt: on
        create_model_response = self.hook.create_model(project_id=project_id, model=deepcopy(model))

        self.assertEqual(create_model_response, model)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().create(body=model_with_airflow_version, parent=project_path),
                mock.call().projects().models().create().execute(num_retries=5),
            ]
        )
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().get(name='projects/test-project/models/test-model'),
                mock.call().projects().models().get().execute(num_retries=5),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_model_with_labels(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        model = {'name': model_name, 'labels': {'other-label': 'test-value'}}
        model_with_airflow_version = {
            'name': model_name,
            'labels': {'other-label': 'test-value', 'airflow-version': hook._AIRFLOW_VERSION},
        }
        project_path = f'projects/{project_id}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            create.return_value.
            execute.return_value
        ) = model

        create_model_response = self.hook.create_model(
            project_id=project_id, model=deepcopy(model)
        )
        # fmt: on
        self.assertEqual(create_model_response, model)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().create(body=model_with_airflow_version, parent=project_path),
                mock.call().projects().models().create().execute(num_retries=5),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_get_model(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        model = {'model': model_name}
        model_path = f'projects/{project_id}/models/{model_name}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            get.return_value.
            execute.return_value
        ) = model
        # fmt: on
        get_model_response = self.hook.get_model(project_id=project_id, model_name=model_name)

        self.assertEqual(get_model_response, model)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().get(name=model_path),
                mock.call().projects().models().get().execute(num_retries=5),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_delete_model(self, mock_get_conn):
        project_id = 'test-project'
        model_name = 'test-model'
        model = {'model': model_name}
        model_path = f'projects/{project_id}/models/{model_name}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            delete.return_value.
            execute.return_value
        ) = model
        # fmt: on
        self.hook.delete_model(project_id=project_id, model_name=model_name)

        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().delete(name=model_path),
                mock.call().projects().models().delete().execute(num_retries=5),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.log")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_delete_model_when_not_exists(self, mock_get_conn, mock_log):
        project_id = 'test-project'
        model_name = 'test-model'
        model_path = f'projects/{project_id}/models/{model_name}'
        http_error = HttpError(
            resp=mock.MagicMock(status=404, reason="Model not found."), content=b'Model not found.'
        )
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            delete.return_value.
            execute.side_effect
        ) = [http_error]
        # fmt: on
        self.hook.delete_model(project_id=project_id, model_name=model_name)

        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().delete(name=model_path),
                mock.call().projects().models().delete().execute(num_retries=5),
            ]
        )
        mock_log.error.assert_called_once_with('Model was not found: %s', http_error)

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.time.sleep")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_delete_model_with_contents(self, mock_get_conn, mock_sleep):
        project_id = 'test-project'
        model_name = 'test-model'
        model_path = f'projects/{project_id}/models/{model_name}'
        operation_path = f'projects/{project_id}/operations/test-operation'
        operation_done = {'name': operation_path, 'done': True}
        version_names = ["AAA", "BBB", "CCC"]
        versions = [
            {
                'name': f'projects/{project_id}/models/{model_name}/versions/{version_name}',
                "isDefault": i == 0,
            }
            for i, version_name in enumerate(version_names)
        ]
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            operations.return_value.
            get.return_value.
            execute.return_value
        ) = operation_done
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            list.return_value.
            execute.return_value
        ) = {"versions": versions}
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            list_next.return_value
        ) = None
        # fmt: on
        self.hook.delete_model(project_id=project_id, model_name=model_name, delete_contents=True)

        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().delete(name=model_path),
                mock.call().projects().models().delete().execute(num_retries=5),
            ]
            + [
                mock.call()
                .projects()
                .models()
                .versions()
                .delete(
                    name=f'projects/{project_id}/models/{model_name}/versions/{version_name}',
                )
                for version_name in version_names
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.time.sleep")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_mlengine_job(self, mock_get_conn, mock_sleep):
        project_id = 'test-project'
        job_id = 'test-job-id'
        project_path = f'projects/{project_id}'
        job_path = f'projects/{project_id}/jobs/{job_id}'
        new_job = {
            'jobId': job_id,
            'foo': 4815162342,
        }
        new_job_with_airflow_version = {
            'jobId': job_id,
            'foo': 4815162342,
            'labels': {'airflow-version': hook._AIRFLOW_VERSION},
        }

        job_succeeded = {
            'jobId': job_id,
            'state': 'SUCCEEDED',
        }
        job_queued = {
            'jobId': job_id,
            'state': 'QUEUED',
        }
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            create.return_value.
            execute.return_value
        ) = job_queued
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            get.return_value.
            execute.side_effect
        ) = [job_queued, job_succeeded]
        # fmt: on
        create_job_response = self.hook.create_job(project_id=project_id, job=deepcopy(new_job))

        self.assertEqual(create_job_response, job_succeeded)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().jobs().create(body=new_job_with_airflow_version, parent=project_path),
                mock.call().projects().jobs().get(name=job_path),
                mock.call().projects().jobs().get().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.time.sleep")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_mlengine_job_with_labels(self, mock_get_conn, mock_sleep):
        project_id = 'test-project'
        job_id = 'test-job-id'
        project_path = f'projects/{project_id}'
        job_path = f'projects/{project_id}/jobs/{job_id}'
        new_job = {'jobId': job_id, 'foo': 4815162342, 'labels': {'other-label': 'test-value'}}
        new_job_with_airflow_version = {
            'jobId': job_id,
            'foo': 4815162342,
            'labels': {'other-label': 'test-value', 'airflow-version': hook._AIRFLOW_VERSION},
        }

        job_succeeded = {
            'jobId': job_id,
            'state': 'SUCCEEDED',
        }
        job_queued = {
            'jobId': job_id,
            'state': 'QUEUED',
        }
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            create.return_value.
            execute.return_value
        ) = job_queued
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            get.return_value.
            execute.side_effect
        ) = [job_queued, job_succeeded]

        create_job_response = self.hook.create_job(
            project_id=project_id, job=deepcopy(new_job)
        )
        # fmt: on
        self.assertEqual(create_job_response, job_succeeded)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().jobs().create(body=new_job_with_airflow_version, parent=project_path),
                mock.call().projects().jobs().get(name=job_path),
                mock.call().projects().jobs().get().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_mlengine_job_reuse_existing_job_by_default(self, mock_get_conn):
        project_id = 'test-project'
        job_id = 'test-job-id'
        project_path = f'projects/{project_id}'
        job_path = f'projects/{project_id}/jobs/{job_id}'
        job_succeeded = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
        }
        error_job_exists = HttpError(resp=mock.MagicMock(status=409), content=b'Job already exists')
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            create.return_value.
            execute.side_effect
        ) = error_job_exists
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            get.return_value.
            execute.return_value
        ) = job_succeeded
        # fmt: on
        create_job_response = self.hook.create_job(project_id=project_id, job=job_succeeded)

        self.assertEqual(create_job_response, job_succeeded)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().jobs().create(body=job_succeeded, parent=project_path),
                mock.call().projects().jobs().create().execute(num_retries=5),
                mock.call().projects().jobs().get(name=job_path),
                mock.call().projects().jobs().get().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_mlengine_job_check_existing_job_failed(self, mock_get_conn):
        project_id = 'test-project'
        job_id = 'test-job-id'
        my_job = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
            'someInput': {'input': 'someInput'},
        }
        different_job = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
            'someInput': {'input': 'someDifferentInput'},
        }
        error_job_exists = HttpError(resp=mock.MagicMock(status=409), content=b'Job already exists')

        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            create.return_value.
            execute.side_effect
        ) = error_job_exists
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            get.return_value.
            execute.return_value
        ) = different_job

        # fmt: on
        def check_input(existing_job):
            return existing_job.get('someInput') == my_job['someInput']

        with self.assertRaises(HttpError):
            self.hook.create_job(project_id=project_id, job=my_job, use_existing_job_fn=check_input)

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_mlengine_job_check_existing_job_success(self, mock_get_conn):
        project_id = 'test-project'
        job_id = 'test-job-id'
        my_job = {
            'jobId': job_id,
            'foo': 4815162342,
            'state': 'SUCCEEDED',
            'someInput': {'input': 'someInput'},
        }
        error_job_exists = HttpError(resp=mock.MagicMock(status=409), content=b'Job already exists')
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            create.return_value.
            execute.side_effect
        ) = error_job_exists
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            get.return_value.
            execute.return_value
        ) = my_job

        # fmt: on
        def check_input(existing_job):
            return existing_job.get('someInput') == my_job['someInput']

        create_job_response = self.hook.create_job(
            project_id=project_id, job=my_job, use_existing_job_fn=check_input
        )

        self.assertEqual(create_job_response, my_job)

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_cancel_mlengine_job(self, mock_get_conn):
        project_id = "test-project"
        job_id = 'test-job-id'
        job_path = f'projects/{project_id}/jobs/{job_id}'

        job_cancelled = {}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            cancel.return_value.
            execute.return_value
        ) = job_cancelled
        # fmt: on
        cancel_job_response = self.hook.cancel_job(job_id=job_id, project_id=project_id)

        self.assertEqual(cancel_job_response, job_cancelled)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().jobs().cancel(name=job_path),
                mock.call().projects().jobs().cancel().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_cancel_mlengine_job_nonexistent_job(self, mock_get_conn):
        project_id = "test-project"
        job_id = 'test-job-id'
        job_cancelled = {}

        error_job_does_not_exist = HttpError(resp=mock.MagicMock(status=404), content=b'Job does not exist')
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            cancel.return_value.
            execute.side_effect
        ) = error_job_does_not_exist
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            cancel.return_value.
            execute.return_value
        ) = job_cancelled
        # fmt: on
        with self.assertRaises(HttpError):
            self.hook.cancel_job(job_id=job_id, project_id=project_id)

    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_cancel_mlengine_job_completed_job(self, mock_get_conn):
        project_id = "test-project"
        job_id = 'test-job-id'
        job_path = f'projects/{project_id}/jobs/{job_id}'
        job_cancelled = {}

        error_job_already_completed = HttpError(
            resp=mock.MagicMock(status=400), content=b'Job already completed'
        )
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            cancel.return_value.
            execute.side_effect
        ) = error_job_already_completed
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            cancel.return_value.
            execute.return_value
        ) = job_cancelled
        # fmt: on
        cancel_job_response = self.hook.cancel_job(job_id=job_id, project_id=project_id)

        self.assertEqual(cancel_job_response, job_cancelled)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().jobs().cancel(name=job_path),
                mock.call().projects().jobs().cancel().execute(num_retries=5),
            ],
            any_order=True,
        )


class TestMLEngineHookWithDefaultProjectId(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        with mock.patch(
            'airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = hook.MLEngineHook()

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_version(self, mock_get_conn, mock_project_id):
        model_name = 'test-model'
        version_name = 'test-version'
        version = {'name': version_name}
        operation_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/operations/test-operation'
        model_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/models/{model_name}'
        operation_done = {'name': operation_path, 'done': True}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            create.return_value.
            execute.return_value
        ) = version
        (
            mock_get_conn.return_value.
            projects.return_value.
            operations.return_value.
            get.return_value.
            execute.return_value
        ) = {'name': operation_path, 'done': True}
        # fmt: on
        create_version_response = self.hook.create_version(
            model_name=model_name, version_spec=version, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )

        self.assertEqual(create_version_response, operation_done)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().versions().create(body=version, parent=model_path),
                mock.call().projects().models().versions().create().execute(num_retries=5),
                mock.call().projects().operations().get(name=version_name),
                mock.call().projects().operations().get().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_set_default_version(self, mock_get_conn, mock_project_id):
        model_name = 'test-model'
        version_name = 'test-version'
        operation_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/operations/test-operation'
        version_path = 'projects/{}/models/{}/versions/{}'.format(
            GCP_PROJECT_ID_HOOK_UNIT_TEST, model_name, version_name
        )
        operation_done = {'name': operation_path, 'done': True}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            setDefault.return_value.
            execute.return_value
        ) = operation_done
        # fmt: on
        set_default_version_response = self.hook.set_default_version(
            model_name=model_name,
            version_name=version_name,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )

        self.assertEqual(set_default_version_response, operation_done)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().versions().setDefault(body={}, name=version_path),
                mock.call().projects().models().versions().setDefault().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.time.sleep")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_list_versions(self, mock_get_conn, mock_sleep, mock_project_id):
        model_name = 'test-model'
        model_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/models/{model_name}'
        version_names = [f'ver_{ix}' for ix in range(3)]
        response_bodies = [
            {'nextPageToken': f"TOKEN-{ix}", 'versions': [ver]} for ix, ver in enumerate(version_names)
        ]
        response_bodies[-1].pop('nextPageToken')

        pages_requests = [mock.Mock(**{'execute.return_value': body}) for body in response_bodies]
        versions_mock = mock.Mock(
            **{'list.return_value': pages_requests[0], 'list_next.side_effect': pages_requests[1:] + [None]}
        )
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value
        ) = versions_mock
        # fmt: on
        list_versions_response = self.hook.list_versions(
            model_name=model_name, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )

        self.assertEqual(list_versions_response, version_names)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().versions().list(pageSize=100, parent=model_path),
                mock.call().projects().models().versions().list().execute(num_retries=5),
            ]
            + [
                mock.call()
                .projects()
                .models()
                .versions()
                .list_next(previous_request=pages_requests[i], previous_response=response_bodies[i])
                for i in range(3)
            ],
            any_order=True,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_delete_version(self, mock_get_conn, mock_project_id):
        model_name = 'test-model'
        version_name = 'test-version'
        # fmt: off
        operation_path = 'projects/{}/operations/test-operation'.format(
            GCP_PROJECT_ID_HOOK_UNIT_TEST
        )
        version_path = 'projects/{}/models/{}/versions/{}'.format(
            GCP_PROJECT_ID_HOOK_UNIT_TEST, model_name, version_name
        )
        version = {'name': operation_path}
        operation_not_done = {'name': operation_path, 'done': False}
        operation_done = {'name': operation_path, 'done': True}

        (
            mock_get_conn.return_value.
            projects.return_value.
            operations.return_value.
            get.return_value.
            execute.side_effect
        ) = [operation_not_done, operation_done]

        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            versions.return_value.
            delete.return_value.
            execute.return_value
        ) = version
        # fmt: on
        delete_version_response = self.hook.delete_version(
            model_name=model_name, version_name=version_name, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )

        self.assertEqual(delete_version_response, operation_done)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().versions().delete(name=version_path),
                mock.call().projects().models().versions().delete().execute(num_retries=5),
                mock.call().projects().operations().get(name=operation_path),
                mock.call().projects().operations().get().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_model(self, mock_get_conn, mock_project_id):
        model_name = 'test-model'
        model = {
            'name': model_name,
        }
        project_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            create.return_value.
            execute.return_value
        ) = model
        # fmt: on
        create_model_response = self.hook.create_model(model=model, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST)

        self.assertEqual(create_model_response, model)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().create(body=model, parent=project_path),
                mock.call().projects().models().create().execute(num_retries=5),
            ]
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_get_model(self, mock_get_conn, mock_project_id):
        model_name = 'test-model'
        model = {'model': model_name}
        model_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/models/{model_name}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            get.return_value.
            execute.return_value
        ) = model
        # fmt: on
        get_model_response = self.hook.get_model(
            model_name=model_name, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )

        self.assertEqual(get_model_response, model)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().get(name=model_path),
                mock.call().projects().models().get().execute(num_retries=5),
            ]
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_delete_model(self, mock_get_conn, mock_project_id):
        model_name = 'test-model'
        model = {'model': model_name}
        model_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/models/{model_name}'
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            models.return_value.
            delete.return_value.
            execute.return_value
        ) = model
        # fmt: on
        self.hook.delete_model(model_name=model_name, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST)

        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().models().delete(name=model_path),
                mock.call().projects().models().delete().execute(num_retries=5),
            ]
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.time.sleep")
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_create_mlengine_job(self, mock_get_conn, mock_sleep, mock_project_id):
        job_id = 'test-job-id'
        project_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}'
        job_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/jobs/{job_id}'
        new_job = {
            'jobId': job_id,
            'foo': 4815162342,
        }
        job_succeeded = {
            'jobId': job_id,
            'state': 'SUCCEEDED',
        }
        job_queued = {
            'jobId': job_id,
            'state': 'QUEUED',
        }
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            create.return_value.
            execute.return_value
        ) = job_queued
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            get.return_value.
            execute.side_effect
        ) = [job_queued, job_succeeded]
        # fmt: on
        create_job_response = self.hook.create_job(job=new_job, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST)

        self.assertEqual(create_job_response, job_succeeded)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().jobs().create(body=new_job, parent=project_path),
                mock.call().projects().jobs().get(name=job_path),
                mock.call().projects().jobs().get().execute(num_retries=5),
            ],
            any_order=True,
        )

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.mlengine.MLEngineHook.get_conn")
    def test_cancel_mlengine_job(self, mock_get_conn, mock_project_id):
        job_id = 'test-job-id'
        job_path = f'projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/jobs/{job_id}'

        job_cancelled = {}
        # fmt: off
        (
            mock_get_conn.return_value.
            projects.return_value.
            jobs.return_value.
            cancel.return_value.
            execute.return_value
        ) = job_cancelled
        # fmt: on
        cancel_job_response = self.hook.cancel_job(job_id=job_id, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST)

        self.assertEqual(cancel_job_response, job_cancelled)
        mock_get_conn.assert_has_calls(
            [
                mock.call().projects().jobs().cancel(name=job_path),
                mock.call().projects().jobs().cancel().execute(num_retries=5),
            ],
            any_order=True,
        )

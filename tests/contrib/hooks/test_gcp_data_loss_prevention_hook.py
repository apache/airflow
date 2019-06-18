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

import unittest
from typing import Dict, Any

from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.contrib.hooks.gcp_data_loss_prevention_hook import CloudDataLossPreventionHook
from tests.compat import mock
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id

GCP_DLP_STRING = 'airflow.contrib.hooks.gcp_data_loss_prevention_hook.{}'
API_RESPONSE = {}  # type: Dict[Any, Any]


class TestCloudDataLossPreventionHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks." "gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudDataLossPreventionHook(gcp_conn_id="test")

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_inspect_content(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .content.return_value.inspect
        method.return_value.execute.return_value = API_RESPONSE
        response = self.hook.inspect_content(parent=parent, body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_inspect_content_http_error(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .content.return_value.inspect
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.inspect_content(parent=parent, body=body)
        self.assertEqual(
            str(e.exception),
            'Inspecting content failed: {}'.format(b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_deidentify_content(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .content.return_value.deidentify
        method.return_value.execute.return_value = API_RESPONSE
        response = self.hook.deidentify_content(parent=parent, body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_deidentify_content_http_error(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .content.return_value.deidentify
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.deidentify_content(parent=parent, body=body)
        self.assertEqual(
            str(e.exception),
            'De-identifying content failed: {}'.format(b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_reidentify_content(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .content.return_value.reidentify
        method.return_value.execute.return_value = API_RESPONSE
        response = self.hook.reidentify_content(parent=parent, body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_reidentify_content_http_error(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .content.return_value.reidentify
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.reidentify_content(parent=parent, body=body)
        self.assertEqual(
            str(e.exception),
            'Re-identifying content failed: {}'.format(b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_resource_projects(self, mock_service):
        name = 'projects/test_proj'

        method = mock_service.return_value.projects

        self.hook._get_resource(name=name)
        method.assert_called_once_with()

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_resource_organizations(self, mock_service):
        name = 'organizations/test_project'

        method = mock_service.return_value.organizations

        self.hook._get_resource(name=name)
        method.assert_called_once_with()

    def test_get_resource_invalid_name(self):
        name = 'test_project'

        with self.assertRaises(ValueError) as e:
            self.hook._get_resource(name=name)
        self.assertEqual(
            str(e.exception),
            "parent or name should begin with 'organizations' or 'projects'."
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_create_dlp_job(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.create
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.create_dlp_job(parent=parent, body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_create_dlp_job_http_error(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.create
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.create_dlp_job(parent=parent, body=body)
        self.assertEqual(
            str(e.exception),
            'Creating dlp job failed: {}'.format(b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_list_dlp_job(self, mock_service):
        parent = 'projects/test_project'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.list
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.list_dlp_jobs(parent=parent,
                                           list_filter=None,
                                           page_token=None,
                                           page_size=None,
                                           order_by=None,
                                           list_type=None)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            filter=None,
            pageToken=None,
            pageSize=None,
            orderBy=None,
            type=None
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_list_dlp_job_http_error(self, mock_service):
        parent = 'projects/test_project'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.list
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.list_dlp_jobs(parent=parent,
                                    list_filter=None,
                                    page_token=None,
                                    page_size=None,
                                    order_by=None,
                                    list_type=None)
        self.assertEqual(
            str(e.exception),
            'Listing dlp jobs for {} failed: {}'.format(parent, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_dlp_job(self, mock_service):
        name = 'projects/test_project/dlpJobs/test_job'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.get
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.get_dlp_job(name)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(name=name)

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_dlp_job_http_error(self, mock_service):
        name = 'projects/test_project/dlpJobs/test_job'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.get
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.get_dlp_job(name=name)
        self.assertEqual(
            str(e.exception),
            'Getting dlp job {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_delete_dlp_job(self, mock_service):
        name = 'projects/test_project/dlpJobs/test_job'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.delete
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.delete_dlp_job(name)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(name=name)

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_delete_dlp_job_http_error(self, mock_service):
        name = 'projects/test_project/dlpJobs/test_job'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.delete
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.delete_dlp_job(name=name)
        self.assertEqual(
            str(e.exception),
            'Deleting dlp job {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_cancel_dlp_job(self, mock_service):
        name = 'projects/test_project/dlpJobs/test_job'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.cancel
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.cancel_dlp_job(name)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(name=name)

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_cancel_dlp_job_http_error(self, mock_service):
        name = 'projects/test_project/dlpJobs/test_job'

        method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.cancel
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.cancel_dlp_job(name=name)
        self.assertEqual(
            str(e.exception),
            'Canceling dlp job {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_run_dlp_job(self, mock_service):
        parent = 'projects/test_project'
        body = {}
        name = 'projects/test_project/dlpJobs/test_job'

        create_job_method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.create
        create_job_method.return_value.execute.return_value = {
            'name': name
        }

        get_job_method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.get
        get_job_method.return_value.execute.return_value = {
            'state': 'DONE'
        }

        self.hook.run_dlp_job(parent=parent, body=body)
        create_job_method.assert_called_once_with(
            parent=parent,
            body=body
        )
        get_job_method.assert_called_once_with(
            name=name
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_run_dlp_job_failed(self, mock_service):
        parent = 'projects/test_project'
        body = {}
        name = 'projects/test_project/dlpJobs/test_job'

        create_job_method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.create
        create_job_method.return_value.execute.return_value = {
            'name': name
        }

        get_job_method = mock_service.return_value.projects.return_value\
            .dlpJobs.return_value.get
        get_job_method.return_value.execute.return_value = {
            'state': 'FAILED'
        }

        with self.assertRaises(AirflowException) as e:
            self.hook.run_dlp_job(parent=parent, body=body)
        self.assertEqual(
            str(e.exception),
            'DlpJob {} failed or canceled: {}'.format(name, 'FAILED')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_create_inspect_template(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.create
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.create_inspect_template(parent=parent, body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_create_inspect_template_http_error(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.create
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.create_inspect_template(parent=parent, body=body)
        self.assertEqual(
            str(e.exception),
            'Creating inspect template {} failed: {}'.format(parent, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_delete_inspect_template(self, mock_service):
        name = 'projects/test_project/inspectTemplates/test_template'

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.delete
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.delete_inspect_template(name=name)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(name=name)

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_delete_inspect_template_http_error(self, mock_service):
        name = 'projects/test_project/inspectTemplates/test_template'

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.delete
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.delete_inspect_template(name=name)
        self.assertEqual(
            str(e.exception),
            'Deleting inspect template {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_inspect_template(self, mock_service):
        name = 'projects/test_project/inspectTemplates/test_template'

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.get
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.get_inspect_template(name=name)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(name=name)

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_inspect_template_http_error(self, mock_service):
        name = 'projects/test_project/inspectTemplates/test_template'

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.get
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.get_inspect_template(name=name)
        self.assertEqual(
            str(e.exception),
            'Getting inspect template {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_list_inspect_templates(self, mock_service):
        parent = 'projects/test_project'

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.list
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.list_inspect_templates(parent=parent,
                                                    page_token=None,
                                                    page_size=None,
                                                    order_by=None)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            pageToken=None,
            pageSize=None,
            orderBy=None
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_list_inspect_templates_http_error(self, mock_service):
        parent = 'projects/test_project'

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.list
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.list_inspect_templates(parent=parent,
                                             page_token=None,
                                             page_size=None,
                                             order_by=None)
        self.assertEqual(
            str(e.exception),
            'Listing inspect templates for {} failed: {}'.format(parent, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_update_inspect_template(self, mock_service):
        name = 'projects/test_project/inspectTemplates/test_template'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.patch
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.update_inspect_template(name=name,
                                                     body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            name=name,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_update_inspect_template_http_error(self, mock_service):
        name = 'projects/test_project/inspectTemplates/test_template'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .inspectTemplates.return_value.patch
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.update_inspect_template(name=name,
                                              body=body)
        self.assertEqual(
            str(e.exception),
            'Updating inspect template {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_create_stored_info_type(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.create
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.create_stored_info_type(parent=parent, body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_create_stored_info_type_http_error(self, mock_service):
        parent = 'projects/test_project'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.create
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.create_stored_info_type(parent=parent, body=body)
        self.assertEqual(
            str(e.exception),
            'Creating storedInfoType {} failed: {}'.format(parent, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_delete_stored_info_type(self, mock_service):
        name = 'projects/test_project/storedInfoTypes/test_info_type'

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.delete
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.delete_stored_info_type(name=name)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(name=name)

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_delete_stored_info_type_http_error(self, mock_service):
        name = 'projects/test_project/storedInfoTypes/test_info_type'

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.delete
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.delete_stored_info_type(name=name)
        self.assertEqual(
            str(e.exception),
            'Deleting storedInfoType {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_stored_info_type(self, mock_service):
        name = 'projects/test_project/storedInfoTypes/test_info_type'

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.get
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.get_stored_info_type(name=name)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(name=name)

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_get_stored_info_type_http_error(self, mock_service):
        name = 'projects/test_project/storedInfoTypes/test_info_type'

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.get
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.get_stored_info_type(name=name)
        self.assertEqual(
            str(e.exception),
            'Getting storedInfoType {} failed: {}'.format(name, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_list_stored_info_types(self, mock_service):
        parent = 'projects/test_project'

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.list
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.list_stored_info_types(parent=parent,
                                                    page_token=None,
                                                    page_size=None,
                                                    order_by=None)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            parent=parent,
            pageToken=None,
            pageSize=None,
            orderBy=None
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_list_stored_info_types_http_error(self, mock_service):
        parent = 'projects/test_project'

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.list
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.list_stored_info_types(parent=parent,
                                             page_token=None,
                                             page_size=None,
                                             order_by=None)
        self.assertEqual(
            str(e.exception),
            'Listing storedInfoTypes {} failed: {}'.format(parent, b'Http error')
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_update_stored_info_type(self, mock_service):
        name = 'projects/test_project/storedInfoTypes/test_info_type'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.patch
        method.return_value.execute.return_value = API_RESPONSE

        response = self.hook.update_stored_info_type(name=name,
                                                     body=body)
        self.assertIs(response, API_RESPONSE)
        method.assert_called_once_with(
            name=name,
            body=body
        )

    @mock.patch(GCP_DLP_STRING.format('CloudDataLossPreventionHook.get_conn'))
    def test_update_stored_info_type_http_error(self, mock_service):
        name = 'projects/test_project/storedInfoTypes/test_info_type'
        body = {}

        method = mock_service.return_value.projects.return_value\
            .storedInfoTypes.return_value.patch
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Http error')

        with self.assertRaises(AirflowException) as e:
            self.hook.update_stored_info_type(name=name,
                                              body=body)
        self.assertEqual(
            str(e.exception),
            'Updating storedInfoType {} failed: {}'.format(name, b'Http error')
        )

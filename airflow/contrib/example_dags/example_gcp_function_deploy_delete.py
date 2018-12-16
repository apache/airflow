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

"""
Example Airflow DAG that creates a Google Cloud Function and then deletes it.

This DAG relies on the following OS environment variables
https://airflow.apache.org/concepts.html#variables
* GCP_PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* GCP_LOCATION - Google Cloud Functions region where the function should be
  created.
* GCF_SOURCE_ARCHIVE_URL - Path to the zipped source in Google Cloud Storage
or
    * GCF_SOURCE_UPLOAD_URL - Generated upload URL for the zipped source
    or
    * GCF_ZIP_PATH - Local path to the zipped source archive
or
* SOURCE_REPOSITORY - The URL pointing to the hosted repository where the function is
defined in a supported Cloud Source Repository URL format
https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#SourceRepository
* GCF_ENTRYPOINT - Name of the executable function in the source code.
"""

import os
import datetime

from airflow import models
from airflow.contrib.operators.gcp_function_operator \
    import GcfFunctionDeployOperator, GcfFunctionDeleteOperator
from airflow.utils import dates

# [START howto_operator_gcf_deploy_variables]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'europe-west1')
GCF_SOURCE_ARCHIVE_URL = os.environ.get('GCF_SOURCE_ARCHIVE_URL', '')
GCF_SOURCE_UPLOAD_URL = os.environ.get('GCF_SOURCE_UPLOAD_URL', '')
SOURCE_REPOSITORY = os.environ.get(
    'SOURCE_REPOSITORY',
    'https://source.developers.google.com/'
    'projects/{}/repos/hello-world/moveable-aliases/master'.format(GCP_PROJECT_ID))
GCF_ZIP_PATH = os.environ.get('GCF_ZIP_PATH', '')
GCF_ENTRYPOINT = os.environ.get('GCF_ENTRYPOINT', 'helloWorld')
GCF_SHORT_FUNCTION_NAME = os.environ.get('GCF_SHORT_FUNCTION_NAME', 'hello')
FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(GCP_PROJECT_ID,
                                                               GCP_LOCATION,
                                                               GCF_SHORT_FUNCTION_NAME)
GCF_RUNTIME = 'nodejs6'
GCP_VALIDATE_BODY = os.environ.get('GCP_VALIDATE_BODY', True)

# [END howto_operator_gcf_deploy_variables]

# [START howto_operator_gcf_deploy_body]
body = {
    "name": FUNCTION_NAME,
    "entryPoint": GCF_ENTRYPOINT,
    "runtime": GCF_RUNTIME,
    "httpsTrigger": {}
}
# [END howto_operator_gcf_deploy_body]

# [START howto_operator_gcf_default_args]
default_args = {
    'start_date': dates.days_ago(1)
}
# [END howto_operator_gcf_default_args]

# [START howto_operator_gcf_deploy_variants]
if GCF_SOURCE_ARCHIVE_URL:
    body['sourceArchiveUrl'] = GCF_SOURCE_ARCHIVE_URL
elif SOURCE_REPOSITORY:
    body['sourceRepository'] = {
        'url': SOURCE_REPOSITORY
    }
elif GCF_ZIP_PATH:
    body['sourceUploadUrl'] = ''
    default_args['zip_path'] = GCF_ZIP_PATH
elif GCF_SOURCE_UPLOAD_URL:
    body['sourceUploadUrl'] = GCF_SOURCE_UPLOAD_URL
else:
    raise Exception("Please provide one of the source_code parameters")
# [END howto_operator_gcf_deploy_variants]


with models.DAG(
    'example_gcp_function_deploy_delete',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    catchup=False
) as dag:
    # [START howto_operator_gcf_deploy]
    deploy_task = GcfFunctionDeployOperator(
        task_id="gcf_deploy_task",
        name=FUNCTION_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=body,
        validate_body=GCP_VALIDATE_BODY
    )
    # [END howto_operator_gcf_deploy]
    delete_task = GcfFunctionDeleteOperator(
        task_id="gcf_delete_task",
        name=FUNCTION_NAME
    )
    deploy_task >> delete_task

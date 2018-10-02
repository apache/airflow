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

This DAG relies on the following Airflow variables
https://airflow.apache.org/concepts.html#variables
* PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* LOCATION - Google Cloud Functions region where the function should be
  created.
* SOURCE_ARCHIVE_URL - Path to the zipped source in Google Cloud Storage
or
    * SOURCE_UPLOAD_URL - Generated upload URL for the zipped source
    or
    * ZIP_PATH - Local path to the zipped source archive
or
* SOURCE_REPOSITORY - The URL pointing to the hosted repository where the function is
defined in a supported Cloud Source Repository URL format
https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#SourceRepository
* ENTRYPOINT - Name of the executable function in the source code.
"""

import datetime

from airflow import models
from airflow.contrib.operators.gcp_function_operator \
    import GcfFunctionDeployOperator, GcfFunctionDeleteOperator
from airflow.utils import dates

# [START howto_operator_gcf_deploy_variables]
PROJECT_ID = models.Variable.get('PROJECT_ID', '')
LOCATION = models.Variable.get('LOCATION', '')
SOURCE_ARCHIVE_URL = models.Variable.get('SOURCE_ARCHIVE_URL', '')
SOURCE_UPLOAD_URL = models.Variable.get('SOURCE_UPLOAD_URL', '')
SOURCE_REPOSITORY = models.Variable.get('SOURCE_REPOSITORY', '')
ZIP_PATH = models.Variable.get('ZIP_PATH', '')
ENTRYPOINT = models.Variable.get('ENTRYPOINT', '')
FUNCTION_NAME = 'projects/{}/locations/{}/functions/{}'.format(PROJECT_ID, LOCATION,
                                                               ENTRYPOINT)
RUNTIME = 'nodejs6'
VALIDATE_BODY = models.Variable.get('VALIDATE_BODY', True)

# [END howto_operator_gcf_deploy_variables]

# [START howto_operator_gcf_deploy_body]
body = {
    "name": FUNCTION_NAME,
    "entryPoint": ENTRYPOINT,
    "runtime": RUNTIME,
    "httpsTrigger": {}
}
# [END howto_operator_gcf_deploy_body]

# [START howto_operator_gcf_deploy_args]
default_args = {
    'start_date': dates.days_ago(1),
    'project_id': PROJECT_ID,
    'location': LOCATION,
    'body': body,
    'validate_body': VALIDATE_BODY
}
# [END howto_operator_gcf_deploy_args]

# [START howto_operator_gcf_deploy_variants]
if SOURCE_ARCHIVE_URL:
    body['sourceArchiveUrl'] = SOURCE_ARCHIVE_URL
elif SOURCE_REPOSITORY:
    body['sourceRepository'] = {
        'url': SOURCE_REPOSITORY
    }
elif ZIP_PATH:
    body['sourceUploadUrl'] = ''
    default_args['zip_path'] = ZIP_PATH
elif SOURCE_UPLOAD_URL:
    body['sourceUploadUrl'] = SOURCE_UPLOAD_URL
else:
    raise Exception("Please provide one of the source_code parameters")
# [END howto_operator_gcf_deploy_variants]


with models.DAG(
    'example_gcp_function_deploy_delete',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    # [START howto_operator_gcf_deploy]
    deploy_task = GcfFunctionDeployOperator(
        task_id="gcf_deploy_task",
        name=FUNCTION_NAME
    )
    # [END howto_operator_gcf_deploy]
    delete_task = GcfFunctionDeleteOperator(
        task_id="gcf_delete_task",
        name=FUNCTION_NAME
    )
    deploy_task >> delete_task

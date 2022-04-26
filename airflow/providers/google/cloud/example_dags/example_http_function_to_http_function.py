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
Example Airflow DAG that displays interactions with Google Cloud Functions (HTTP).
It does the following tasks:
    * Create 2 functions
    * Invoke the 1st function
    * Invoke the 2nd function using response from 1st function
    * Delete both functions

This DAG relies on the following OS environment variables
https://airflow.apache.org/concepts.html#variables

* GCP_PROJECT_ID - Google Cloud Project to use for the Cloud Function.
* GCP_LOCATION - Google Cloud Functions region where the function should be
  created.
* GCF_ENTRYPOINT_1 - Name of the executable method in the source code for 1st function.
* GCF_ENTRYPOINT_2 - Name of the executable method in the source code for 2nd function.
* and one of the below (for both functions):

    * GCF_SOURCE_ARCHIVE_URL_1 - Path to the zipped source of 1st function in Google Cloud Storage
    * GCF_SOURCE_ARCHIVE_URL_2 - Path to the zipped source of 2nd function in Google Cloud Storage

    * GCF_SOURCE_UPLOAD_URL_1 - Generated upload URL for the zipped source (1st function) and GCF_ZIP_PATH_1 - Local path to
      the zipped source archive
    * GCF_SOURCE_UPLOAD_URL_2 - Generated upload URL for the zipped source (2nd function) and GCF_ZIP_PATH_2 - Local path to
      the zipped source archive

    * GCF_SOURCE_REPOSITORY_1 - The URL pointing to the hosted repository where the 1st function
      is defined in a supported Cloud Source Repository URL format
    * GCF_SOURCE_REPOSITORY_2 - The URL pointing to the hosted repository where the 2nd function
      is defined in a supported Cloud Source Repository URL format
      https://cloud.google.com/functions/docs/reference/rest/v1/projects.locations.functions#SourceRepository

"""

import os
from datetime import datetime
import json
from typing import Any, Dict, TYPE_CHECKING, Any, Dict, Sequence

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.providers.google.cloud.hooks.functions import CloudFunctionsHook

from airflow import models
from airflow.providers.google.cloud.operators.functions import (
    CloudFunctionDeleteFunctionOperator,
    CloudFunctionDeployFunctionOperator,
    CloudFunctionInvokeFunctionOperator,
)

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'europe-west1')
# make sure there are no dashes in function name (!)
GCF_SHORT_FUNCTION_NAME_1 = os.environ.get('GCF_SHORT_FUNCTION_NAME_1', 'hello').replace("-", "_")
GCF_SHORT_FUNCTION_NAME_2 = os.environ.get('GCF_SHORT_FUNCTION_NAME_2', 'helloFromOtherSide').replace("-", "_")

FUNCTION_NAME_1 = f'projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/functions/{GCF_SHORT_FUNCTION_NAME_1}'
FUNCTION_NAME_2 = f'projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/functions/{GCF_SHORT_FUNCTION_NAME_2}'

GCF_SOURCE_REPOSITORY_1 = os.environ.get(
    'GCF_SOURCE_REPOSITORY_1',
    f'https://source.developers.google.com/projects/{GCP_PROJECT_ID}/'
    f'repos/hello-world-cf/moveable-aliases/master',
)
GCF_SOURCE_REPOSITORY_2 = os.environ.get(
    'GCF_SOURCE_REPOSITORY_2',
    f'https://source.developers.google.com/projects/{GCP_PROJECT_ID}/'
    f'repos/hello-world-cf/moveable-aliases/function2',
)

GCF_ENTRYPOINT = os.environ.get('GCF_ENTRYPOINT', 'helloWorld')
GCF_RUNTIME = 'python39'
GCP_VALIDATE_BODY = os.environ.get('GCP_VALIDATE_BODY', "True") == "True"

# [START howto_operator_gcf_deploy_body]
body_1 = {"name": FUNCTION_NAME_1, "entryPoint": GCF_ENTRYPOINT, "runtime": GCF_RUNTIME, "httpsTrigger": {}}
body_2 = {"name": FUNCTION_NAME_2, "entryPoint": GCF_ENTRYPOINT, "runtime": GCF_RUNTIME, "httpsTrigger": {}}
# [END howto_operator_gcf_deploy_body]

# [START howto_operator_gcf_default_args]
default_args: Dict[str, Any] = {'retries': 3}
# [END howto_operator_gcf_default_args]

# [START howto_operator_gcf_deploy_variants]
body_1['sourceRepository'] = {'url': GCF_SOURCE_REPOSITORY_1}
body_2['sourceRepository'] = {'url': GCF_SOURCE_REPOSITORY_2}
# [END howto_operator_gcf_deploy_variants]

class CustomCloudFunctionInvokeOperator(CloudFunctionInvokeFunctionOperator):

    template_fields: Sequence[str] = (
        'function_id',
        'input_data',
        'location',
        'project_id',
        'impersonation_chain',
        'cf1_result'
    )

    def __init__(
        self,
        *args,
        cf1_result: Dict,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cf1_result = cf1_result

    def execute(self, context: 'Context'):
        hook = CloudFunctionsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info('Calling function %s.', self.function_id)

        self.input_data['cf1_result'] = self.cf1_result
        req_data = {"data": json.dumps(self.input_data)}

        result = hook.call_function(
            function_id=self.function_id,
            input_data=req_data,
            location=self.location,
            project_id=self.project_id,
        )
        self.log.info('Function called successfully. Execution id %s', result.get('executionId'))
        self.xcom_push(context=context, key='execution_id', value=result.get('executionId'))
        if 'result' in result:
            self.xcom_push(context=context, key='function_response', value=result.get('result'))
        # self.xcom_push(context=context, key='cf_response', value=result)
        return result

with models.DAG(
    'example_gcp_function',
    default_args=default_args,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_gcf_deploy 1st function]
    deploy_task_1 = CloudFunctionDeployFunctionOperator(
        task_id="gcf_deploy_task_1",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=body_1,
        validate_body=GCP_VALIDATE_BODY,
    )
    # [END howto_operator_gcf_deploy 1st function]

    # [START howto_operator_gcf_deploy 2nd function]
    deploy_task_2 = CloudFunctionDeployFunctionOperator(
        task_id="gcf_deploy_task_2",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=body_2,
        validate_body=GCP_VALIDATE_BODY,
    )
    # [END howto_operator_gcf_deploy 2nd function]

    # [START howto_operator_gcf_invoke_function 1st function]
    invoke_task_1 = CloudFunctionInvokeFunctionOperator(
        task_id="invoke_task_1",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        input_data={},
        function_id=GCF_SHORT_FUNCTION_NAME_1,
    )
    # [END howto_operator_gcf_invoke_function 1st function]

    # [START howto_operator_gcf_invoke_function 2nd function]
    invoke_task_2 = CustomCloudFunctionInvokeOperator(
        task_id="invoke_task_2",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cf1_result=invoke_task_1.output['function_response'],
        input_data={},
        function_id=GCF_SHORT_FUNCTION_NAME_2,
    )
    # [END howto_operator_gcf_invoke_function 2nd function]

    # [START howto_operator_gcf_delete 1st function]
    delete_task_1 = CloudFunctionDeleteFunctionOperator(
        task_id="gcf_delete_task_1",
        name=FUNCTION_NAME_1,
    )
    # [END howto_operator_gcf_delete 1st function]

    # [START howto_operator_gcf_delete 2nd function]
    delete_task_2 = CloudFunctionDeleteFunctionOperator(
        task_id="gcf_delete_task_2",
        name=FUNCTION_NAME_1,
    )
    # [END howto_operator_gcf_delete 2nd function]

    deploy_task_1 >> deploy_task_2 >> invoke_task_1 >> invoke_task_2 >> delete_task_1 >> delete_task_2

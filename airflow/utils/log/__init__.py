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
# fmt: off
from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    'cloudwatch_task_handler': {
        'CloudwatchTaskHandler': (
            'airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudwatchTaskHandler'
        ),
    },
    'es_task_handler': {
        'ElasticsearchTaskHandler': (
            'airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchTaskHandler'
        ),
    },
    'gcs_task_handler': {
        'GCSTaskHandler': 'airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler',
    },
    's3_task_handler': {
        'S3TaskHandler': 'airflow.providers.amazon.aws.log.s3_task_handler.S3TaskHandler',
    },
    'stackdriver_task_handler': {
        'StackdriverTaskHandler': (
            'airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverTaskHandler'
        ),
    },
    'wasb_task_handler': {
        'WasbTaskHandler': 'airflow.providers.microsoft.azure.log.wasb_task_handler.WasbTaskHandler',
    },
}

add_deprecated_classes(__deprecated_classes, __name__)

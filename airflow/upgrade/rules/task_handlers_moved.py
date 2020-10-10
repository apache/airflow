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

from airflow import conf
from airflow.upgrade.rules.base_rule import BaseRule
from airflow.utils.module_loading import import_string

LOGS = [
    (
        "airflow.providers.amazon.aws.log.s3_task_handler.S3TaskHandler",
        "airflow.utils.log.s3_task_handler.S3TaskHandler"
    ),
    (
        'airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudwatchTaskHandler',
        'airflow.utils.log.cloudwatch_task_handler.CloudwatchTaskHandler'
    ),
    (
        'airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchTaskHandler',
        'airflow.utils.log.es_task_handler.ElasticsearchTaskHandler'
    ),
    (
        "airflow.providers.google.cloud.log.stackdriver_task_handler.StackdriverTaskHandler",
        "airflow.utils.log.stackdriver_task_handler.StackdriverTaskHandler"
    ),
    (
        "airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler",
        "airflow.utils.log.gcs_task_handler.GCSTaskHandler"
    ),
    (
        "airflow.providers.microsoft.azure.log.wasb_task_handler.WasbTaskHandler",
        "airflow.utils.log.wasb_task_handler.WasbTaskHandler"
    )
]


class TaskHandlersMovedRule(BaseRule):
    title = "Changes in import path of remote task handlers"
    description = (
        "The remote log task handlers have been moved to the providers "
        "directory and into their respective providers packages."
    )

    def check(self):
        logging_class = conf.get("core", "logging_config_class", fallback=None)
        if logging_class:
            config = import_string(logging_class)
            configured_path = config['handlers']['task']['class']
            for new_path, old_path in LOGS:
                if configured_path == old_path:
                    return [
                        "This path : `{old}` should be updated to this path: `{new}`".format(old=old_path,
                                                                                             new=new_path)
                    ]

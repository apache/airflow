# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import configuration as conf

# TODO: Logging format and level should be configured
# in this file instead of from airflow.cfg. Currently
# there are other log format and level configurations in
# settings.py and cli.py.

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
LOG_FORMAT = conf.get('core', 'log_format')


BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
# TODO: This should be specified as s3_remote and/or gcs_remote
REMOTE_BASE_LOG_FOLDER = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': LOG_FORMAT,
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
        'file.task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': BASE_LOG_FOLDER,
        },
        's3.task': {
            'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
            'base_log_folder': BASE_LOG_FOLDER,
            'remote_base_log_folder': REMOTE_BASE_LOG_FOLDER,
            'formatter': 'airflow.task',
        },
    },
    'loggers': {
        'airflow.task': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task_runner': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'airflow.task.raw': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
    }
}

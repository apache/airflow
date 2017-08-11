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

import os

from airflow import configuration as conf

# TODO: Logging format and level should be configured
# in this file instead of from airflow.cfg. Currently
# there are other log format and level configurations in
# settings.py and cli.py. Please see AIRFLOW-1455.

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
LOG_FORMAT = conf.get('core', 'log_format')

BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')

# TODO: REMOTE_BASE_LOG_FOLDER should be deprecated and
# directly specify in the handler definitions. This is to
# provide compatibility to older remote log folder settings.
REMOTE_BASE_LOG_FOLDER = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')
S3_LOG_FOLDER = ''
GCS_LOG_FOLDER = ''
if REMOTE_BASE_LOG_FOLDER.startswith('s3:/'):
    S3_LOG_FOLDER = REMOTE_BASE_LOG_FOLDER
elif REMOTE_BASE_LOG_FOLDER.startswith('gs:/'):
    GCS_LOG_FOLDER = REMOTE_BASE_LOG_FOLDER

FILENAME_TEMPLATE = '{dag_id}/{task_id}/{execution_date}/{try_number}.log'

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': LOG_FORMAT,
        },
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
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'filename_template': FILENAME_TEMPLATE,
        },
        's3.task': {
            'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            's3_log_folder': S3_LOG_FOLDER,
            'filename_template': FILENAME_TEMPLATE,
        },
        'gcs.task': {
            'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'gcs_log_folder': GCS_LOG_FOLDER,
            'filename_template': FILENAME_TEMPLATE,
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

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

import os

from airflow import configuration as conf
from airflow.utils.file import mkdirs

# TODO: Logging format and level should be configured
# in this file instead of from airflow.cfg. Currently
# there are other log format and level configurations in
# settings.py and cli.py. Please see AIRFLOW-1455.
LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()


# Flask appbuilder's info level log is very verbose,
# so it's set to 'WARN' by default.
FAB_LOG_LEVEL = conf.get('core', 'FAB_LOGGING_LEVEL').upper()

LOG_FORMAT = conf.get('core', 'LOG_FORMAT')

BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')

PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'CHILD_PROCESS_LOG_DIRECTORY')

DAG_PROCESSOR_MANAGER_LOG_LOCATION = \
    conf.get('core', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')

FILENAME_TEMPLATE = conf.get('core', 'LOG_FILENAME_TEMPLATE')

PROCESSOR_FILENAME_TEMPLATE = conf.get('core', 'LOG_PROCESSOR_FILENAME_TEMPLATE')

# Storage bucket url for remote logging
# s3 buckets should start with "s3://"
# gcs buckets should start with "gs://"
# wasb buckets should start with "wasb"
# just to help Airflow select correct handler
REMOTE_BASE_LOG_FOLDER = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')

ELASTICSEARCH_HOST = conf.get('elasticsearch', 'ELASTICSEARCH_HOST')

LOG_ID_TEMPLATE = conf.get('elasticsearch', 'ELASTICSEARCH_LOG_ID_TEMPLATE')

END_OF_LOG_MARK = conf.get('elasticsearch', 'ELASTICSEARCH_END_OF_LOG_MARK')

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow': {
            'format': LOG_FORMAT,
        },
    },
    'handlers': {
        'console': {
            'class': 'airflow.utils.log.logging_mixin.RedirectStdHandler',
            'formatter': 'airflow',
            'stream': 'sys.stdout'
        },
        'task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'filename_template': FILENAME_TEMPLATE,
        },
        'processor': {
            'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        }
    },
    'loggers': {
        'airflow.processor': {
            'handlers': ['processor'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task': {
            'handlers': ['task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'flask_appbuilder': {
            'handler': ['console'],
            'level': FAB_LOG_LEVEL,
            'propagate': True,
        }
    },
    'root': {
        'handlers': ['console'],
        'level': LOG_LEVEL,
    }
}

DEFAULT_DAG_PARSING_LOGGING_CONFIG = {
    'handlers': {
        'processor_manager': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'airflow',
            'filename': DAG_PROCESSOR_MANAGER_LOG_LOCATION,
            'mode': 'a',
            'maxBytes': 104857600,  # 100MB
            'backupCount': 5
        }
    },
    'loggers': {
        'airflow.processor_manager': {
            'handlers': ['processor_manager'],
            'level': LOG_LEVEL,
            'propagate': False,
        }
    }
}

REMOTE_HANDLERS = {
    's3': {
        'task': {
            'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            's3_log_folder': REMOTE_BASE_LOG_FOLDER,
            'filename_template': FILENAME_TEMPLATE,
        },
        'processor': {
            'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            's3_log_folder': REMOTE_BASE_LOG_FOLDER,
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        },
    },
    'gcs': {
        'task': {
            'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'gcs_log_folder': REMOTE_BASE_LOG_FOLDER,
            'filename_template': FILENAME_TEMPLATE,
        },
        'processor': {
            'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'gcs_log_folder': REMOTE_BASE_LOG_FOLDER,
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
        },
    },
    'wasb': {
        'task': {
            'class': 'airflow.utils.log.wasb_task_handler.WasbTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'wasb_log_folder': REMOTE_BASE_LOG_FOLDER,
            'wasb_container': 'airflow-logs',
            'filename_template': FILENAME_TEMPLATE,
            'delete_local_copy': False,
        },
        'processor': {
            'class': 'airflow.utils.log.wasb_task_handler.WasbTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'wasb_log_folder': REMOTE_BASE_LOG_FOLDER,
            'wasb_container': 'airflow-logs',
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
            'delete_local_copy': False,
        },
    },
    'elasticsearch': {
        'task': {
            'class': 'airflow.utils.log.es_task_handler.ElasticsearchTaskHandler',
            'formatter': 'airflow',
            'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
            'log_id_template': LOG_ID_TEMPLATE,
            'filename_template': FILENAME_TEMPLATE,
            'end_of_log_mark': END_OF_LOG_MARK,
            'host': ELASTICSEARCH_HOST,
        },
    },
}

REMOTE_LOGGING = conf.get('core', 'remote_logging')

# Only update the handlers and loggers when CONFIG_PROCESSOR_MANAGER_LOGGER is set.
# This is to avoid exceptions when initializing RotatingFileHandler multiple times
# in multiple processes.
if os.environ.get('CONFIG_PROCESSOR_MANAGER_LOGGER') == 'True':
    DEFAULT_LOGGING_CONFIG['handlers'] \
        .update(DEFAULT_DAG_PARSING_LOGGING_CONFIG['handlers'])
    DEFAULT_LOGGING_CONFIG['loggers'] \
        .update(DEFAULT_DAG_PARSING_LOGGING_CONFIG['loggers'])

    # Manually create log directory for processor_manager handler as RotatingFileHandler
    # will only create file but not the directory.
    processor_manager_handler_config = DEFAULT_DAG_PARSING_LOGGING_CONFIG['handlers'][
        'processor_manager']
    directory = os.path.dirname(processor_manager_handler_config['filename'])
    mkdirs(directory, 0o755)

if REMOTE_LOGGING and REMOTE_BASE_LOG_FOLDER.startswith('s3://'):
    DEFAULT_LOGGING_CONFIG['handlers'].update(REMOTE_HANDLERS['s3'])
elif REMOTE_LOGGING and REMOTE_BASE_LOG_FOLDER.startswith('gs://'):
    DEFAULT_LOGGING_CONFIG['handlers'].update(REMOTE_HANDLERS['gcs'])
elif REMOTE_LOGGING and REMOTE_BASE_LOG_FOLDER.startswith('wasb'):
    DEFAULT_LOGGING_CONFIG['handlers'].update(REMOTE_HANDLERS['wasb'])
elif REMOTE_LOGGING and ELASTICSEARCH_HOST:
    DEFAULT_LOGGING_CONFIG['handlers'].update(REMOTE_HANDLERS['elasticsearch'])

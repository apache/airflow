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
#
import logging
import sys
from logging.config import dictConfig

from airflow import configuration as conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)


def configure_logging():
    logging_class_path = ''
    try:
        logging_class_path = conf.get('core', 'logging_config_class')
    except AirflowConfigException:
        log.debug('Could not find key logging_config_class in config')

    if logging_class_path:
        try:
            logging_config = import_string(logging_class_path)

            # Make sure that the variable is in scope
            assert (isinstance(logging_config, dict))

            log.info(
                'Successfully imported user-defined logging config from %s',
                logging_class_path
            )
        except Exception:
            # Import default logging configurations.
            raise ImportError(
                'Unable to load custom logging from {}'.format(logging_class_path)
            )
    else:
        from airflow.config_templates.airflow_local_settings import (
            DEFAULT_LOGGING_CONFIG as logging_config
        )
        log.debug('Unable to load custom logging, using default config instead')

    try:
        # Try to init logging
        dictConfig(logging_config)
    except ValueError as e:
        log.warning('Unable to load the config, contains a configuration error.')
        # When there is an error in the config, escalate the exception
        # otherwise Airflow would silently fall back on the default config
        raise e

    return logging_config

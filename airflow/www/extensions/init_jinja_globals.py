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

import logging
import socket

import pendulum

import airflow
from airflow.configuration import conf
from airflow.settings import IS_K8S_OR_K8SCELERY_EXECUTOR, STATE_COLORS
from airflow.utils.platform import get_airflow_git_version


def init_jinja_globals(app):
    """Add extra globals variable to Jinja context"""
    server_timezone = conf.get('core', 'default_timezone')
    if server_timezone == "system":
        server_timezone = pendulum.local_timezone().name
    elif server_timezone == "utc":
        server_timezone = "UTC"

    default_ui_timezone = conf.get('webserver', 'default_ui_timezone')
    if default_ui_timezone == "system":
        default_ui_timezone = pendulum.local_timezone().name
    elif default_ui_timezone == "utc":
        default_ui_timezone = "UTC"
    if not default_ui_timezone:
        default_ui_timezone = server_timezone

    expose_hostname = conf.getboolean('webserver', 'EXPOSE_HOSTNAME', fallback=True)
    hostname = socket.getfqdn() if expose_hostname else 'redact'

    try:
        airflow_version = airflow.__version__
    except Exception as e:  # pylint: disable=broad-except
        airflow_version = None
        logging.error(e)

    git_version = get_airflow_git_version()

    def prepare_jinja_globals():
        extra_globals = {
            'server_timezone': server_timezone,
            'default_ui_timezone': default_ui_timezone,
            'hostname': hostname,
            'navbar_color': conf.get('webserver', 'NAVBAR_COLOR'),
            'log_fetch_delay_sec': conf.getint('webserver', 'log_fetch_delay_sec', fallback=2),
            'log_auto_tailing_offset': conf.getint('webserver', 'log_auto_tailing_offset', fallback=30),
            'log_animation_speed': conf.getint('webserver', 'log_animation_speed', fallback=1000),
            'state_color_mapping': STATE_COLORS,
            'airflow_version': airflow_version,
            'git_version': git_version,
            'k8s_or_k8scelery_executor': IS_K8S_OR_K8SCELERY_EXECUTOR,
            'rest_api_enabled': conf.get('api', 'auth_backend') != 'airflow.api.auth.backend.deny_all',
        }

        if 'analytics_tool' in conf.getsection('webserver'):
            tool = conf.get('webserver', 'ANALYTICS_TOOL')
            extra_globals.update(
                {
                    'analytics_tool': tool
                }
            )
            if tool == 'apache_flagon_useralejs':
                extra_globals.update(
                    {
                        'apache_flagon_useralejs_data_auth': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_AUTH'),
                        'apache_flagon_useralejs_data_autostart': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_AUTOSTART'),
                        'apache_flagon_useralejs_data_interval': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_INTERVAL'),
                        'apache_flagon_useralejs_data_log_details': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_LOG_DETAILS'),
                        'apache_flagon_useralejs_data_resolution': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_RESOLUTION'),
                        'apache_flagon_useralejs_data_threshold': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_THRESHOLD'),
                        'apache_flagon_useralejs_data_tool': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_TOOL'),
                        'apache_flagon_useralejs_data_user': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_USER'),
                        'apache_flagon_useralejs_data_url': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_URL'),
                        'apache_flagon_useralejs_data_version': conf.get('webserver', 'APACHE_FLAGON_USERALEJS_DATA_VERSION')
                    }
                )
            else:
                extra_globals.update(
                    {
                        'analytics_id': conf.get('webserver', 'ANALYTICS_ID'),
                    }
                )

        return extra_globals

    app.context_processor(prepare_jinja_globals)

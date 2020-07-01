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

import socket

import pendulum

from airflow.configuration import conf
from airflow.settings import STATE_COLORS


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
    hosstname = socket.getfqdn() if expose_hostname else 'redact'

    def prepare_jinja_globals():
        extra_globals = {
            'server_timezone': server_timezone,
            'default_ui_timezone': default_ui_timezone,
            'hostname': hosstname,
            'navbar_color': conf.get('webserver', 'NAVBAR_COLOR'),
            'log_fetch_delay_sec': conf.getint('webserver', 'log_fetch_delay_sec', fallback=2),
            'log_auto_tailing_offset': conf.getint('webserver', 'log_auto_tailing_offset', fallback=30),
            'log_animation_speed': conf.getint('webserver', 'log_animation_speed', fallback=1000),
            'state_color_mapping': STATE_COLORS
        }

        if 'analytics_tool' in conf.getsection('webserver'):
            extra_globals.update(
                {
                    'analytics_tool': conf.get('webserver', 'ANALYTICS_TOOL'),
                    'analytics_id': conf.get('webserver', 'ANALYTICS_ID'),
                }
            )

        return extra_globals

    app.context_processor(prepare_jinja_globals)

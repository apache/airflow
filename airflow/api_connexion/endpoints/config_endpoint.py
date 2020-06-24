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

from flask import Response, request

from airflow.api_connexion.schemas.config_schema import Config, ConfigOption, ConfigSection, config_schema
from airflow.configuration import conf


def get_config() -> Response:
    """
    Get current configuration.
    """
    response_types = ['text/plain', 'application/json']
    content_type = request.accept_mimetypes.best_match(response_types)
    conf_dict = conf.as_dict(display_source=True, display_sensitive=True)
    config = Config(
        sections=[
            ConfigSection(
                name=section,
                options=[
                    ConfigOption(key=key, value=value, source=source)
                    for key, (value, source) in parameters.items()
                ]
            )
            for section, parameters in conf_dict.items()
        ]
    )
    if content_type == 'text/plain':
        config_text = '\n'.join(
            f'[{config_section.name}]\n' +
            ''.join(f'{config_option.key} = {config_option.value}  # source: {config_option.source}\n'
                    for config_option in config_section.options)
            for config_section in config.sections
        )
        return config_text
    else:
        return config_schema.dump(config)

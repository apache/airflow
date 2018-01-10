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
from __future__ import unicode_literals
from __future__ import absolute_import

import os
import json
from tempfile import mkstemp

from airflow import configuration as conf


COPY_SECTIONS = [
    'core', 'smtp', 'scheduler', 'celery', 'webserver', 'hive'
]


def tmp_configuration_copy():
    """
    Returns a path for a temporary file including a full copy of the configuration
    settings.
    :return: a path to a temporary file
    """
    cfg_dict = conf.as_dict(display_sensitive=True)
    temp_fd, cfg_path = mkstemp()

    cfg_subset = dict()
    for section in COPY_SECTIONS:
        cfg_subset[section] = cfg_dict.get(section, {})

    with os.fdopen(temp_fd, 'w') as temp_file:
        json.dump(cfg_subset, temp_file)

    return cfg_path

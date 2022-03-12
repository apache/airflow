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

import json
import os
from tempfile import mkstemp

from airflow.configuration import conf
from airflow.utils.platform import IS_WINDOWS


def tmp_configuration_copy(chmod=0o600, include_env=True, include_cmds=True):
    """
    Returns a path for a temporary file including a full copy of the configuration
    settings.

    :param include_env: Should the value of configuration from ``AIRFLOW__``
        environment variables be included or not
    :param include_cmds: Should the result of calling any *_cmd config be
        set (True, default), or should the _cmd options be left as the
        command to run (False)
    :return: a path to a temporary file
    """
    cfg_dict = conf.as_dict(
        display_sensitive=True, raw=True, include_cmds=include_cmds, include_env=include_env
    )
    temp_fd, cfg_path = mkstemp()

    with os.fdopen(temp_fd, 'w') as temp_file:
        # Set the permissions before we write anything to it.
        if chmod is not None and not IS_WINDOWS:
            os.fchmod(temp_fd, chmod)
        json.dump(cfg_dict, temp_file)

    return cfg_path

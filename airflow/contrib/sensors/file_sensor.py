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

import os
import stat

from airflow.contrib.hooks.fs_hook import FSHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class FileSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in a filesystem.

    If the path given is a directory then this sensor will only return true if
    any files exist inside it (either directly, or within a subdirectory)

    :param fs_conn_id: reference to the File (path)
        connection id
    :type fs_conn_id: string
    :param filepath: File or folder name (relative to
        the base path set within the connection)
    :type fs_conn_id: string
    """
    template_fields = ('filepath',)
    ui_color = '#91818a'

    @apply_defaults
    def __init__(self,
                 filepath,
                 fs_conn_id='fs_default2',
                 *args,
                 **kwargs):
        super(FileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.fs_conn_id = fs_conn_id

    def poke(self, context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = "/".join([basepath, self.filepath])
        self.log.info('Poking for file {full_path}'.format(**locals()))
        try:
            if stat.S_ISDIR(os.stat(full_path).st_mode):
                for root, dirs, files in os.walk(full_path):
                    if len(files):
                        return True
            else:
                # full_path was a file directly
                return True
        except OSError:
            return False
        return False

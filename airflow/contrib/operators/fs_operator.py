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

from os import walk
import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.utils.decorators import apply_defaults


class FileSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in a filesystem

    :param fs_conn_id: reference to the File (path)
        connection id
    :type fs_conn_id: string
    :param filepath: File or folder name (relative to
        the base path set within the connection)
    :type fs_conn_id: string
    """
    template_fields = ('filepath',)

    @apply_defaults
    def __init__(
            self,
            filepath,
            fs_conn_id='fs_default2',
            *args, **kwargs):
        super(FileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.fs_conn_id = fs_conn_id

    def poke(self, context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = "/".join([basepath, self.filepath])
        logging.info(
            'Poking for file {full_path} '.format(**locals()))
        try:
            files = [f for f in walk(full_path)]
        except:
            return False
        return True

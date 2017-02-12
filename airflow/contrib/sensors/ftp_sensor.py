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

import ftplib
import logging

from airflow.contrib.hooks.ftp_hook import FTPHook, FTPSHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class FTPSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on FTP.

    :param path: Remote file or directory path
    :type path: str
    :param ftp_conn_id: The connection to run the sensor against
    :type ftp_conn_id: str
    """
    template_fields = ('path',)

    @apply_defaults
    def __init__(self, path, ftp_conn_id='ftp_default', *args, **kwargs):
        super(FTPSensor, self).__init__(*args, **kwargs)

        self.path = path
        self.ftp_conn_id = ftp_conn_id

    def _create_hook(self):
        """Return connection hook."""
        return FTPHook(ftp_conn_id=self.ftp_conn_id)

    def poke(self, context):
        with self._create_hook() as hook:
            logging.info('Poking for %s', self.path)
            try:
                hook.get_mod_time(self.path)
            except ftplib.error_perm as e:
                error = str(e).split(None, 1)
                if error[1] != "Can't check for file existence":
                    raise e

                return False

            return True


class FTPSSensor(FTPSensor):
    """Waits for a file or directory to be present on FTP over SSL."""
    def _create_hook(self):
        """Return connection hook."""
        return FTPSHook(ftp_conn_id=self.ftp_conn_id)

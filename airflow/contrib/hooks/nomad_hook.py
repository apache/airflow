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

""" Hook to manage connection to nomad server
    NOTE: this operator also relies on the python-nomad
          package https://github.com/jrxFive/python-nomad """

import nomad

from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class NomadHook(BaseHook, LoggingMixin):
    def __init__(self,
                 nomad_conn_id='nomad_default',
                 *args,
                 **kwargs):
        self.connection = self.get_connection(nomad_conn_id)
        self.nomad_client = self.get_nomad_client(*args, **kwargs)

    def get_nomad_client(self, *args, **kwargs):
        return nomad.Nomad(host=self.connection.host,
                           port=self.connection.port,
                           *args,
                           **kwargs)

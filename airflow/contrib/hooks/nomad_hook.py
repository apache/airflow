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
    NOTE: this operator also relies on the python-nomad package https://github.com/jrxFive/python-nomad """

from airflow.hooks.base_hook import BaseHook

import nomad


class NomadHook(BaseHook):

    def __init__(self, nomad_conn_id='nomad_default', *args, **kwargs):
        self.get_connection = self.get_connection(nomad_conn_id)
        self.nomad_client = nomad.Nomad(host=self.get_connection.host,
                                        port=self.get_connection.port,
                                        *args,
                                        **kwargs)
        self.log.debug('Trying to connect to Nomad Server in {}:{}'.format(self.get_connection.host,
                                                                           self.get_connection.port,))

    def get_nomad_client(self):
        return self.nomad_client

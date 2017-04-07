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

import logging

from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class WasbBlobSensor(BaseSensorOperator):
    """
    Waits for a blob to arrive on Azure Blob Storage.
    
    :param container_name: Name of the container.
    :type container_name: str
    :param blob_name: Name of the blob.
    :type blob_name: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_blob()` takes.
    :type check_options: dict
    """

    template_fields = ('container_name', 'blob_name')

    @apply_defaults
    def __init__(self, container_name, blob_name,
                 wasb_conn_id='wasb_default', check_options=None, *args,
                 **kwargs):
        super(WasbBlobSensor, self).__init__(*args, **kwargs)
        if check_options is None:
            check_options = {}
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.check_options = check_options

    def poke(self, context):
        logging.info(
            'Poking for blob: {self.blob_name}\n'
            'in wasb://{self.container_name}'.format(**locals())
        )
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        return hook.check_for_blob(self.container_name, self.blob_name,
                                   **self.check_options)


class WasbPrefixSensor(BaseSensorOperator):
    """
    Waits for blobs matching a prefix to arrive on Azure Blob Storage.
    
    :param container_name: Name of the container.
    :type container_name: str
    :param prefix: Prefix of the blob.
    :type prefix: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_prefix()` takes.
    :type check_options: dict
    """

    template_fields = ('container_name', 'prefix')

    @apply_defaults
    def __init__(self, container_name, prefix, wasb_conn_id='wasb_default',
                 check_options=None, *args, **kwargs):
        super(WasbPrefixSensor, self).__init__(*args, **kwargs)
        if check_options is None:
            check_options = {}
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.prefix = prefix
        self.check_options = check_options

    def poke(self, context):
        logging.info(
            'Poking for prefix: {self.prefix}\n'
            'in wasb://{self.container_name}'.format(**locals())
        )
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        return hook.check_for_prefix(self.container_name, self.prefix,
                                     **self.check_options)

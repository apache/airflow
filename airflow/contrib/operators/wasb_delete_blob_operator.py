# -*- coding: utf-8 -*-
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
#
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class WasbDeleteBlobOperator(BaseOperator):
    """
    Deletes blob(s) on Azure Blob Storage.

    :param container_name: Name of the container. (templated)
    :type container_name: str
    :param blob_name: Name of the blob. (templated)
    :type blob_name: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_blob()` takes.
    :param is_prefix: If blob_name is a prefix, delete all files matching prefix.
    :type is_prefix: bool
    :param ignore_if_missing: if True, then return success even if the
    blob does not exist.
    :type ignore_if_missing: bool
    """

    template_fields = ('container_name', 'blob_name')

    @apply_defaults
    def __init__(self, container_name, blob_name,
                 wasb_conn_id='wasb_default', check_options=None,
                 is_prefix=False, ignore_if_missing=False,
                 *args,
                 **kwargs):
        super(WasbDeleteBlobOperator, self).__init__(*args, **kwargs)
        if check_options is None:
            check_options = {}
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.check_options = check_options
        self.is_prefix = is_prefix
        self.ignore_if_missing = ignore_if_missing

    def execute(self, context):
        self.log.info(
            'Deleting blob: {self.blob_name}\n'
            'in wasb://{self.container_name}'.format(**locals())
        )
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        hook.delete_file(self.container_name, self.blob_name,
                         self.is_prefix, self.ignore_if_missing,
                         **self.check_options)

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

import json

from airflow.hooks.druid_hook import DruidHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DruidOperator(BaseOperator):
    """
    Allows to submit a task directly to druid

    :param json_index_file: The filepath to the druid index specification
    :type json_index_file: str
    :param druid_ingest_conn_id: The connection id of the Druid overlord which
        accepts index jobs
    :type druid_ingest_conn_id: str
    """
    template_fields = ('index_spec_str',)
    template_ext = ('.json',)

    @apply_defaults
    def __init__(self, json_index_file,
                 druid_ingest_conn_id='druid_ingest_default',
                 max_ingestion_time=None,
                 *args, **kwargs):
        super(DruidOperator, self).__init__(*args, **kwargs)
        self.conn_id = druid_ingest_conn_id
        self.max_ingestion_time = max_ingestion_time

        with open(json_index_file) as data_file:
            index_spec = json.load(data_file)
        self.index_spec_str = json.dumps(
            index_spec,
            sort_keys=True,
            indent=4,
            separators=(',', ': ')
        )

    def execute(self, context):
        hook = DruidHook(
            druid_ingest_conn_id=self.conn_id,
            max_ingestion_time=self.max_ingestion_time
        )
        self.log.info("Submitting %s", self.index_spec_str)
        hook.submit_indexing_job(self.index_spec_str)

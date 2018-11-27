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


from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_athena_hook import AWSAthenaHook
from airflow.operators.sensors import BaseSensorOperator


# our own implementation of athena step sensor, including multiple failure states.

class AthenaSensor(BaseSensorOperator):
    INTERMEDIATE_STATES = ('QUEUED', 'RUNNING',)
    FAILURE_STATES = ('FAILED', 'CANCELLED',)
    SUCCESS_STATES = ('SUCCEEDED',)

    template_fields = ['query_execution_id']
    template_ext = ()
    ui_color = '#66c3ff'

    @apply_defaults
    def __init__(self,
                 query_execution_id,
                 max_retires=None,
                 aws_conn_id='aws_default',
                 sleep_time=10,
                 *args, **kwargs):
        super(BaseSensorOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.query_execution_id = query_execution_id
        self.hook = None
        self.sleep_time = sleep_time
        self.max_retires = max_retires

    def poke(self, context):
        self.hook = self.get_hook()
        self.hook.get_conn()
        state = self.hook.poll_query_status(self.query_execution_id, self.max_retires)

        if state in self.FAILURE_STATES:
            raise AirflowException('Athena sensor failed')

        if state in self.INTERMEDIATE_STATES:
            raise False
        return True

    def get_hook(self):
        return AWSAthenaHook(self.aws_conn_id, self.sleep_time)

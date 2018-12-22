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
from airflow.contrib.hooks.aws_hook import AwsHook


class EmrHook(AwsHook):
    """
    Interact with AWS EMR. emr_conn_id is only necessary for using the
    create_job_flow method.
    """

    def __init__(self, emr_conn_id=None, *args, **kwargs):
        self.emr_conn_id = emr_conn_id
        super(EmrHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_client_type('emr')
        return self.conn

    def create_job_flow(self, job_flow_overrides):
        """
        Creates a job flow using the config from the EMR connection.
        Keys of the json extra hash may have the arguments of the boto3
        run_job_flow method.
        Overrides for this config may be passed as the job_flow_overrides.
        """

        if not self.emr_conn_id:
            raise AirflowException('emr_conn_id must be present to use create_job_flow')

        emr_conn = self.get_connection(self.emr_conn_id)

        config = emr_conn.extra_dejson.copy()
        config.update(job_flow_overrides)

        response = self.get_conn().run_job_flow(**config)

        return response

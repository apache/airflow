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
"""
This module contains a Google Dataprep operator.
"""

from typing import Dict

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.dataprep import GoogleDataprepHook
from airflow.utils.decorators import apply_defaults


class DataprepGetJobsForJobGroupOperator(BaseOperator):
    """
    Get information about the batch jobs within a Cloud Dataprep job.
    API documentation https://clouddataprep.com/documentation/api#section/Overview

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprepGetJobsForJobGroupOperator`


    :param job_id The ID of the job that will be requests
    :type job_id: int
    """

    template_fields = ("job_id",)

    @apply_defaults
    def __init__(
        self, *, job_id: int, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id

    def execute(self, context: Dict):
        self.log.info("Fetching data for job with id: %d ...", self.job_id)
        hook = GoogleDataprepHook(dataprep_conn_id="dataprep_conn_id")
        response = hook.get_jobs_for_job_group(job_id=self.job_id)
        return response

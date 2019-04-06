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


from airflow.hooks.presto_hook import PrestoHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PrestoOperator(BaseOperator):
    """
    Runs HQL query on Presto.

    :param hql: HQL query to execute on Presto. (templated)
    :type hql: str
    :param conn_id: presto connection id
    :type conn_id: str
    """

    template_fields = ('hql',)
    template_ext = ('.hql',)

    @apply_defaults
    def __init__(self,
                 hql,  # type: str
                 conn_id='presto_default',  # type: str
                 *args,
                 **kwargs):
        super(PrestoOperator, self).__init__(*args, **kwargs)
        self.hql = hql
        self.conn_id = conn_id

    def execute(self, context):
        presto_hook = PrestoHook(presto_conn_id=self.conn_id)
        self.log.info("Executing hql: %s", self.hql)
        presto_hook.run(self.hql)
        self.log.info("Finished!")

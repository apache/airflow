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
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.plugin.rest_api import RestApiPlugin, path_cast


@path_cast.datetime('execution_date')
@path_cast.body_var('conf')
@RestApiPlugin.requires_authentication
def post(
    dag_id,
    run_id=None,
    conf=None,
    execution_date=None,
    replace_microseconds=True,
):
    response = trigger_dag(
        dag_id,
        run_id,
        conf,
        execution_date,
        replace_microseconds
    )
    return {
        'message': "Created {}".format(response)
    }

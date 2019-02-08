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
import logging
import string

import connexion
import pkg_resources
from connexion import Resolver
from flask import jsonify

from airflow import AirflowException
from airflow.api import load_auth
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


class RestApiPlugin(AirflowPlugin):
    """
    Add swagger entrypoint
    """
    name = 'restful-api'

    @classmethod
    def on_load(cls, flask_app, configuration, *args, **kwargs):
        load_auth()
        app = connexion.App(
            __name__,
            specification_dir=pkg_resources.resource_filename(
                __name__,
                ''
            )
        )
        log.debug("Loading airflow API.")
        experimental_api = app.add_api(
            'experimental_swagger.yaml',
            strict_validation=True,
            resolver=AirflowRestyResolver('experimental'),
        )
        cls.flask_blueprints.append(experimental_api.blueprint)

        # The proposed v1 API would be included the same way
        # as the experimental API.
        #
        # v1_api = app.add_api(
        #     'v1_swagger.yaml',
        #     strict_validation=True,
        #     resolver=AirflowRestyResolver('v1'),
        # )
        # cls.flask_blueprints.append(v1_api.blueprint)


class AirflowRestyResolver(Resolver):
    handler_mapping = {
        'dags.delete': 'airflow.api.common.experimental.delete_dag.delete_dag',
        'dags.dag_runs.list': 'airflow.api.common.experimental.get_dag_runs.get_dag_runs',
        'dags.dag_runs.get': 'airflow.api.common.experimental.get_dag_run_state.get_dag_run_state',
        'dags.dag_runs.post': 'airflow.api.common.experimental.trigger_dag.trigger_dag',
        'dags.dag_runs.tasks.get': 'airflow.api.common.experimental.get_task_instance.get_task_instance',
        'dags.paused.get': 'airflow.www.api.experimental.endpoints.dag_paused',
        'dags.tasks.get': 'airflow.api.common.experimental.get_task.get_task',
        'latest_runs.list': 'airflow.www.api.experimental.endpoints.latest_dag_runs',
        'test.list': 'airflow.www.api.experimental.endpoints.test',
        'pools.list': 'airflow.api.common.experimental.pool.get_pools',
        'pools.get': 'airflow.api.common.experimental.pool.get_pool',
        'pools.post': 'airflow.api.common.experimental.pool.create_pool',
        'pools.delete': 'airflow.api.common.experimental.pool.delete_pool',
    }

    def __init__(self, version):
        super(AirflowRestyResolver, self).__init__()
        self.version = version

    def resolve_function_from_operation_id(self, operation_id):
        method = super(AirflowRestyResolver, self).resolve_function_from_operation_id(operation_id)

        def ensure_serialisable(func):
            def handler(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, list):
                        return [i.to_json() for i in result]
                    elif hasattr(result, 'to_json'):
                        return result.to_json()
                    else:
                        return result
                except AirflowException as err:
                    response = jsonify(error="{}".format(err))
                    response.status_code = err.status_code
                    return response

            return handler

        return ensure_serialisable(method)

    def resolve_operation_id(self, operation):
        literals = []
        is_collection_endpoint = False
        for literal, var_text, _, _ in string.Formatter().parse(operation.path):
            literals.append(literal.strip('/').replace('/', '.'))
            is_collection_endpoint = (var_text is None)
        controller = '.'.join(literals)

        def get_function_name():
            method = operation.method

            is_list = is_collection_endpoint and (method.lower() == 'get')

            return 'list' if is_list else method.lower()

        handler = '{}.{}'.format(controller, get_function_name())
        if (self.version == 'experimental') \
           and (handler in self.handler_mapping):
            return self.handler_mapping[handler]

        return '.'.join(['airflow.plugin.rest_api', self.version, handler])

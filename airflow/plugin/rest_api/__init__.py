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

import airflow
from airflow import AirflowException
from airflow.api import load_auth
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


class RestApiPlugin(AirflowPlugin):
    """
    Add swagger entrypoint
    """
    name = 'restful-api'
    requires_authentication = None

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

        cls.requires_authentication = airflow.api.api_auth.requires_authentication


class AirflowRestyResolver(Resolver):
    """
    A class to allow mapping between the API Path/Method operations
    and python modules in the airflow package.
    In future, this mapping should not be needed and handlers should
    be picked up from airflow.plugin.rest_api. This migration would be
    an ongoing
    """

    handler_mapping = {
        'dags.delete': 'airflow.api.common.experimental.delete_dag.delete_dag',
        'dags.dag_runs.list': 'airflow.api.common.experimental.get_dag_runs.get_dag_runs',
        'dags.dag_runs.get': 'airflow.api.common.experimental.get_dag_run_state.get_dag_run_state',
        'dags.dag_runs.tasks.get': 'airflow.api.common.experimental.get_task_instance.get_task_instance',
        'dags.tasks.get': 'airflow.api.common.experimental.get_task.get_task',
        'pools.list': 'airflow.api.common.experimental.pool.get_pools',
        'pools.get': 'airflow.api.common.experimental.pool.get_pool',
        'pools.post': 'airflow.api.common.experimental.pool.create_pool',
        'pools.delete': 'airflow.api.common.experimental.pool.delete_pool',
    }

    def __init__(self, version):
        super(AirflowRestyResolver, self).__init__()
        self.version = version

    def resolve_function_from_operation_id(self, operation_id):
        """
        Make sure any method used as an endpoint will return an object
        which can be returned in an HTTP json response
        """
        method = super(AirflowRestyResolver, self).resolve_function_from_operation_id(operation_id)

        def var_properties(obj):
            return {
                k: str(v)
                for k, v in vars(obj).items()
                if not k.startswith('_')
            }

        def handle_errors(func):
            def handler(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except AirflowException as err:
                    response = jsonify(error="{}".format(err))
                    response.status_code = err.status_code
                    return response, response.status_code
            return handler

        def ensure_serialisable(func):
            def handler(*args, **kwargs):
                result = func(*args, **kwargs)
                if isinstance(result, list):
                    if len(result):
                        if hasattr(result[0], 'to_json'):
                            return [i.to_json() for i in result]
                        else:
                            return [dict(i) for i in result]
                    else:
                        return []
                elif hasattr(result, 'to_json'):
                    return result.to_json()
                elif isinstance(result, dict):
                    return result
                else:
                    return var_properties(result)

            return handler

        if self.version == 'experimental':
            return handle_errors(
                ensure_serialisable(method)
            )
        else:
            return handle_errors(method)

    def resolve_operation_id(self, operation):
        """
        Given an operation, create a module path to a string.
        """
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
            # If we're using the experimental API, use the explicit mapping
            # from `self.handler_mapping`.
            return self.handler_mapping[handler]

        return '.'.join(['airflow.plugin.rest_api', self.version, handler])

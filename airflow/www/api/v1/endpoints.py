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

import airflow.api.common.variables as variables
import airflow.api.common.proto.variable_pb2 as variable_pb2

from flask import (
    url_for, Markup, Blueprint, redirect, jsonify, abort, request, app, send_file
)

apiv1 = Blueprint('apiv1', __name__)


@apiv1.route('/variables/get/<string:key>')
def get_var(key):
    try:
        my_var = variables.get_var(key)
    except ValueError as e:
        logging.log(logging.WARN, e)
        abort(404)

    var = variable_pb2.Variable()
    var.key = key
    var.value = my_var
    return var.SerializeToString()


@apiv1.route('/variables/set', methods=['POST'])
def set_var():
    var = variable_pb2.Variable(request.content)
    serialize_json = False
    if var.serialize_json:
        serialize_json = var.serialize_json

    variables.set_var(var.key, var.value, serialize_json)


@apiv1.route('/variables/list', methods=['GET'])
def list_var():
    vars_list = variables.list_var()
    pb2_vars = variable_pb2.VariableList()
    for var in vars_list:
        pb2_var = pb2_vars.variables.add()
        pb2_var.key = var.key
        pb2_var.value = var.get_val()

    return pb2_vars.SerializeToString()


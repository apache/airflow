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
import airflow.common.variables as variables

from flask import (
    url_for, Markup, Blueprint, redirect, jsonify, abort, request, app, send_file
)

apiv1 = Blueprint('apiv1', __name__)


@apiv1.route('/variables/get/<string:key>/<string:default_var>/<string:deserialize_json>')
def get_var(key, default_var=None, deserialize_json=None):
    try:
        my_var = variables.get_var(key, default_var, deserialize_json)
    except ValueError as e:
        abort(404)

    return jsonify({'variable':
                        {'key' : key,
                         'value': my_var,
                         }
                    })


@apiv1.route('/variables/set', methods=['POST'])
def set_var():
    variables.set_var(request.json['key'], request.json['value'], request.json['serialize_json'])


@apiv1.route('/variables/list', methods=['GET'])
def list_var():
    my_vars = variables.list_var()
    return jsonify({'variables': my_vars})


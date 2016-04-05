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
from airflow import settings
from airflow.models import Connection

import logging
import random

from flask import (
    url_for, Markup, Blueprint, redirect, jsonify, abort
)

apiv1 = Blueprint('apiv1', __name__)


# todo: use DAG id and signature to verify access
# todo: use roles
@apiv1.route('/get_connections/<string:conn_id>')
def get_connections(conn_id):
    logging.info("Looking up conn_id: " + conn_id)

    session = settings.Session()
    dbs = (
        session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .all()
    )
    if not dbs:
        logging.warning("Cannot find connection " + conn_id)
        abort(404)

    session.expunge_all()
    session.close()

    conns = []
    for db in dbs:
        conn = {'host': db.host,
                'port': db.port,
                'schema': db.schema,
                'login': db.login,
                'password': db.password,
                'extra': db.extra,
                'conn_type': db.conn_type,
                'is_encrypted': db.is_encrypted,
                'is_extra_encrypted': db.is_extra_encrypted,
                }
        conns.append(conn)

    return jsonify({'data': conns})

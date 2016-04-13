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
from airflow.security.flask_kerberos import requires_authentication
from airflow.www.app import dagbag, csrf
from airflow import configuration

import logging
import random
import os
import uuid

from flask import (
    url_for, Markup, Blueprint, redirect, jsonify, abort, request, app, send_file
)

apiv1 = Blueprint('apiv1', __name__)


# todo: use DAG id and signature to verify access
# todo: use roles
@requires_authentication
@apiv1.route('/get_connections/<string:dag_id>/<string:conn_id>')
def get_connections(dag_id, conn_id):
    logging.info("Looking up conn_id {0} for dag id {1}".format(conn_id, dag_id))

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


@requires_authentication
@apiv1.route('/submit', methods=['POST'])
@csrf.exempt
def submit():
    data = request.files['file']
    ext = os.path.splitext(data.filename)[1]
    filename = str(uuid.uuid4()) + ext
    path = os.path.join(configuration.get('core', 'DAGS_FOLDER'), filename)
    logging.info("Saving {} to {}".format(data.filename, path))
    data.save(path)

    logging.info("Processing {}".format(path))
    dags = dagbag.process_file(filepath=path, only_if_updated=False, safe_mode=True)
    if not dags:
        logging.warning("No DAGS found in {}".format(path))
        os.unlink(path)
        abort(400)

    # todo: how to send to scheduler, webservers, workers to update themselves?

    return jsonify({'result': 'OK'})


@apiv1.route('/get_dags')
def get_dags():
    dags = []
    for d in dagbag.dags:
        dag = dagbag.get_dag(d)
        if dag.is_subdag:
            continue

        jsondag = {
            'dag_id': dag.dag_id,
            'last_changed': dagbag.file_last_changed[dag.full_filepath],
        }
        dags.append(jsondag)

    return jsonify({'data': dags})


@apiv1.route('/get_dag_file/<string:dag_id>')
def get_dag_file(dag_id):
    dag = dagbag.get_dag(dag_id)
    if not dag:
        logging.warning("Dag id {} not found.".format(dag_id))
        abort(404)

    logging.info("Sending {}".format(dag.full_filepath))

    # todo: maybe set mimetype?
    return send_file(dag.full_filepath, attachment_filename=dag.filepath)
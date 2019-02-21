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

from airflow.exceptions import AirflowException, DagNotFound
from airflow import models, settings
from airflow.www import utils as wwwutils


def get_code(dag_id):
    """Return python code of a given dag_id."""
    session = settings.Session()
    DM = models.DagModel
    dag = session.query(DM).filter(DM.dag_id == dag_id).first()
    session.close()
    # Check DAG exists.
    if dag is None:
        error_message = "Dag id {} not found".format(dag_id)
        raise DagNotFound(error_message)

    try:
        with wwwutils.open_maybe_zipped(dag.fileloc, 'r') as f:
            code = f.read()
            return code
    except IOError as e:
        error_message = "Error {} while reading Dag id {} Code".format(str(e), dag_id)
        raise AirflowException(error_message)

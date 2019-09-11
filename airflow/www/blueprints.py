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
from flask import (
    url_for, Blueprint, redirect,
)

from airflow import jobs
from airflow.www import utils as wwwutils

routes = Blueprint('routes', __name__)


@routes.route('/')
def index():
    return redirect(url_for('admin.index'))


@routes.route('/health')
def health():
    """
    An endpoint helping check the health status of the Airflow instance,
    including metadatabase and scheduler.
    """

    payload = {
        'metadatabase': {'status': 'unhealthy'}
    }
    latest_scheduler_heartbeat = None
    scheduler_status = 'unhealthy'
    payload['metadatabase'] = {'status': 'healthy'}
    try:
        scheduler_job = jobs.SchedulerJob.most_recent_job()

        if scheduler_job:
            latest_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
            if scheduler_job.is_alive():
                scheduler_status = 'healthy'
    except Exception:
        payload['metadatabase']['status'] = 'unhealthy'

    payload['scheduler'] = {'status': scheduler_status,
                            'latest_scheduler_heartbeat': latest_scheduler_heartbeat}

    return wwwutils.json_response(payload)

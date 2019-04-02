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
from datetime import timedelta
from flask import (
    url_for, Blueprint, redirect,
)
from sqlalchemy import func

from airflow import configuration as conf
from airflow import jobs, settings
from airflow.utils import timezone
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
    session = settings.Session()
    BJ = jobs.BaseJob
    payload = {}
    scheduler_health_check_threshold = timedelta(seconds=conf.getint('scheduler',
                                                                     'scheduler_health_check_threshold'
                                                                     ))

    latest_scheduler_heartbeat = None
    payload['metadatabase'] = {'status': 'healthy'}
    try:
        latest_scheduler_heartbeat = session.query(func.max(BJ.latest_heartbeat)). \
            filter(BJ.state == 'running', BJ.job_type == 'SchedulerJob'). \
            scalar()
    except Exception:
        payload['metadatabase']['status'] = 'unhealthy'

    if not latest_scheduler_heartbeat:
        scheduler_status = 'unhealthy'
    else:
        if timezone.utcnow() - latest_scheduler_heartbeat <= scheduler_health_check_threshold:
            scheduler_status = 'healthy'
        else:
            scheduler_status = 'unhealthy'

    payload['scheduler'] = {'status': scheduler_status,
                            'latest_scheduler_heartbeat': str(latest_scheduler_heartbeat)}

    return wwwutils.json_response(payload)

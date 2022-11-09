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
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer

from airflow.configuration import conf
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session


class HealthServer(BaseHTTPRequestHandler):
    """Small webserver to serve scheduler health check"""

    def do_GET(self):
        if self.path == '/health':
            try:
                with create_session() as session:
                    scheduler_job = (
                        session.query(SchedulerJob)
                        .filter_by(hostname=get_hostname())
                        .order_by(SchedulerJob.latest_heartbeat.desc())
                        .limit(1)
                        .first()
                    )
                if scheduler_job and scheduler_job.is_alive():
                    self.send_response(200)
                    self.end_headers()
                else:
                    self.send_error(503)
            except Exception:
                self.send_error(503)
        else:
            self.send_error(404)


def serve_health_check():
    health_check_port = conf.getint('scheduler', 'SCHEDULER_HEALTH_CHECK_SERVER_PORT')
    httpd = HTTPServer(("0.0.0.0", health_check_port), HealthServer)
    httpd.serve_forever()


if __name__ == "__main__":
    serve_health_check()

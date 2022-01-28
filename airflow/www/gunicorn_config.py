#!/usr/bin/env python
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

import os
import setproctitle
from opentelemetry import trace
from opentelemetry.exporter.jaeger.proto.grpc import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from airflow import settings


def post_worker_init(_):
    """
    Set process title.

    This is used by airflow.cli.commands.webserver_command to track the status of the worker.
    """
    old_title = setproctitle.getproctitle()
    setproctitle.setproctitle(settings.GUNICORN_WORKER_READY_PREFIX + old_title)


def on_starting(server):
    from airflow.providers_manager import ProvidersManager

    # Load providers before forking workers
    ProvidersManager().connection_form_widgets


def post_fork(server, worker):
    """
    Needed to workoround open-telemetry not working well with gunicorn fork model.

    See more: https://opentelemetry-python.readthedocs.io/en/latest/examples/fork-process-model/README.html
    """
    if os.environ["AIRFLOW_ENABLE_GUNICORN_OPENTELEMETRY"] == "true":

        server.log.info("Worker spawned (pid: %s)", worker.pid)

        resource = Resource.create(attributes={"service.name": "airflow-service"})

        trace.set_tracer_provider(TracerProvider(resource=resource))
        span_processor = BatchSpanProcessor(JaegerExporter())
        trace.get_tracer_provider().add_span_processor(span_processor)

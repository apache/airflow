 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



Checking Airflow Health Status
==============================

Airflow has two methods to check the health of components - HTTP checks and CLI checks. All available checks are
accessible through the CLI, but only some are accessible through HTTP due to the role of the component being checked
and the tools being used to monitor the deployment.

For example, when running on Kubernetes, use `a Liveness probe <https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/>`__ (``livenessProbe`` property)
with :ref:`CLI checks <check-health/cli-checks-for-scheduler>` on the scheduler deployment to restart it when it fails.
For the webserver, you can configure the readiness probe (``readinessProbe`` property) using :ref:`check-health/http-endpoint`.

For an example for a Docker Compose environment, see the ``docker-compose.yaml`` file available in the :doc:`/howto/docker-compose/index`.

.. _check-health/http-endpoint:

Webserver Health Check Endpoint
-------------------------------

To check the health status of your Airflow instance, you can simply access the endpoint
``/api/v2/monitor/health``. It will return a JSON object that provides a high-level glance at the health status across multiple Airflow components,
including per-instance details when multiple schedulers, triggerers, or Dag processors are running.

.. code-block:: JSON

  {
    "metadatabase": {
      "status": "healthy"
    },
    "scheduler": {
      "status": "healthy",
      "latest_scheduler_heartbeat": "2018-12-26T17:15:11+00:00",
      "detailed_status": "healthy",
      "instances": [
        {
          "hostname": "scheduler-1.example.com",
          "latest_scheduler_heartbeat": "2018-12-26T17:15:11+00:00"
        }
      ]
    },
    "triggerer": {
      "status": "healthy",
      "latest_triggerer_heartbeat": "2018-12-26T17:16:12+00:00",
      "detailed_status": "degraded",
      "instances": [
        {
          "hostname": "triggerer-1.example.com",
          "latest_triggerer_heartbeat": "2018-12-26T17:16:12+00:00",
          "team_name": "team-a"
        }
      ]
    },
    "dag_processor": {
      "status": "healthy",
      "latest_dag_processor_heartbeat": "2018-12-26T17:16:12+00:00",
      "detailed_status": "healthy",
      "instances": [
        {
          "hostname": "dag-processor-1.example.com",
          "latest_dag_processor_heartbeat": "2018-12-26T17:16:12+00:00",
          "bundle_names": ["dags-team-a"]
        }
      ]
    }
  }

* ``metadatabase``

  * ``status`` is ``"healthy"`` when a valid connection can be initiated with the database, otherwise ``"unhealthy"``.

* Component-level fields for ``scheduler``, ``triggerer``, and ``dag_processor``

  * ``status`` (legacy aggregate): ``"healthy"`` if **any** running instance is alive, otherwise ``"unhealthy"``
    (including when no running jobs exist for that component).

  * ``detailed_status``: whether every part of that component's work is being covered by a live instance.
    What counts as "every part" differs per component, because only some of them divide their work up:

    * **Dag processor** — the parts are the Dag bundles in ``[dag_processor] dag_bundle_config_list``.
      A processor started without ``--bundle-name`` covers every configured bundle;
      one started with it covers only the bundles it was given.
      ``"healthy"`` when every configured bundle has a live processor, ``"degraded"`` when only some do,
      ``"down"`` when none do.
    * **Triggerer** — with ``[core] multi_team`` enabled, the parts are the teams those bundles are scoped to
      (plus the unscoped bundles), because a triggerer only picks up triggers for its own team.
      ``"healthy"`` when every team scope has a live triggerer, ``"degraded"`` when only some do,
      ``"down"`` when none do. With multi-team disabled, no team filtering applies, so any live triggerer
      covers everything: ``"healthy"`` if one is alive, ``"down"`` if none is.
    * **Scheduler** — schedulers are symmetric and share no partitioned work, so there is nothing partial
      to report: ``"healthy"`` if at least one is alive, ``"down"`` if none is. ``"degraded"`` is never
      returned for the scheduler. Use ``instances`` to see how many replicas are up, and your orchestrator
      or the ``scheduler_heartbeat`` metric to alert on reduced scheduling throughput.

    Because the expected set of work comes from configuration rather than from the job table,
    ``detailed_status`` is unaffected by how instances come and go. Restarting an instance — including after
    a ``SIGKILL``, an out-of-memory kill, or a node eviction, none of which let Airflow mark the old job row
    as finished — does not report the component as ``"degraded"``.

  * ``latest_*_heartbeat``: the most recent heartbeat among running jobs of that type (ordered by heartbeat descending),
    or ``null`` when there are none.
    An instance is considered alive when its latest heartbeat is within the component health-check threshold
    (defaults and config options: ``[scheduler] scheduler_health_check_threshold``,
    ``[triggerer] triggerer_health_check_threshold``,
    ``[dag_processor] health_check_threshold``).

  * ``instances``: one entry per **live** instance of that type — ``null`` when none is live. Airflow cannot
    mark a job as finished when its process is killed abruptly (``SIGKILL``, an out-of-memory kill, a node
    eviction), so the job row of such an instance is never closed; listing it would show a host that is gone
    and, under an orchestrator that assigns a fresh hostname on each restart, will never come back.
    ``status`` and ``latest_*_heartbeat`` are still derived from every unfinished job, so how long a dead
    component has been silent stays visible after it drops out of ``instances``. Each entry includes:

    * ``hostname``: host where the component is running
    * the corresponding ``latest_*_heartbeat`` for that instance
    * ``team_name`` (triggerer only): team the triggerer is scoped to, or ``null`` when unscoped
    * ``bundle_names`` (Dag processor only): Dag bundles that processor is configured to parse, or ``null`` when unset

  * For HA deployments, prefer ``detailed_status`` and ``instances`` when you need to see every scheduler,
    triggerer, or Dag processor. The top-level ``status`` remains useful for simple probes that only care
    whether at least one instance is healthy.

Please keep in mind that the HTTP response code of ``/api/v2/monitor/health`` endpoint **should not** be used to determine the health
status of the application. The return code is only indicative of the state of the rest call (200 for success).

Served by the web server, this health check endpoint is independent of the newer :ref:`Scheduler Health Check Server <check-health/scheduler-health-check-server>`, which optionally runs on each scheduler.

.. note::

  * For this check to work, at least one working web server is required. Suppose you use this check for scheduler
    monitoring, then in case of failure of the web server, you will lose the ability to monitor scheduler, which means
    that it can be restarted even if it is in good condition. For greater confidence, consider using :ref:`CLI Check for Scheduler <check-health/cli-checks-for-scheduler>` or  :ref:`Scheduler Health Check Server <check-health/scheduler-health-check-server>`.

  * Using this endpoint as webserver probes (liveness/readiness) makes it contingent on Airflow core components' availability (database, scheduler, etc).
    Webservers will be frequently restarted if any of these core components are down. To make Webservers less prone to other components' failures, consider using endpoints like ``api/v2/version``.

.. _check-health/scheduler-health-check-server:

Scheduler Health Check Server
-----------------------------

In order to check scheduler health independent of the web server, Airflow optionally starts a small HTTP server
in each scheduler to serve a scheduler ``/health`` endpoint. It returns status code ``200`` when the scheduler
is healthy and status code ``503`` when the scheduler is unhealthy. To run this server in each scheduler, set
``[scheduler]enable_health_check`` to ``True``. By default, it is ``False``. The server is running on the port
specified by the ``[scheduler]scheduler_health_check_server_port`` option. By default, it is ``8974``. We are
using `http.server.BaseHTTPRequestHandler <https://docs.python.org/3/library/http.server.html#http.server.BaseHTTPRequestHandler>`__ as a small server.

.. _check-health/cli-checks-for-scheduler:

CLI Check for Scheduler
-----------------------

Scheduler creates an entry in the table :class:`airflow.jobs.job.Job` with information about the host and
timestamp (heartbeat) at startup, and then updates it regularly. You can use this to check if the scheduler is
working correctly. To do this, you can use the ``airflow jobs check`` command. On failure, the command will exit
with a non-zero error code.

To check if the local scheduler is still working properly, run:

.. code-block:: bash

    airflow jobs check --job-type SchedulerJob --local

To check if any scheduler is running when you are using high availability, run:

.. code-block:: bash

    airflow jobs check --job-type SchedulerJob --allow-multiple --limit 100

``--limit`` caps how many jobs are inspected, starting with the ones that reported a heartbeat most
recently. Set it to ``0`` to inspect all of them, so that no scheduler is missed regardless of how
many are running.

CLI Check for Database
----------------------

To verify that the database is working correctly, you can use the ``airflow db check`` command. On failure, the command will exit
with a non-zero error code.

HTTP monitoring for Celery Cluster
----------------------------------

You can optionally use Flower to monitor the health of the Celery cluster. It also provides an HTTP API that you can use to build a health check for your environment.

For details about installation, see: :doc:`apache-airflow-providers-celery:celery_executor`. For details about usage, see: `The Flower project documentation <https://flower.readthedocs.io/>`__.

CLI Check for Celery Workers
----------------------------

To verify that the Celery workers are working correctly, you can use the ``celery inspect ping`` command. On failure, the command will exit
with a non-zero error code.

.. note::

  For this check to work, ``[celery]worker_enable_remote_control`` must be ``True``.
  If the parameter is set to ``False``, the command will exit with a non-zero error code.

To check if the worker running on the local host is working correctly, run:

.. code-block:: bash

    celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d celery@${HOSTNAME}

To check if the all workers in the cluster running is working correctly, run:

.. code-block:: bash

    celery --app airflow.providers.celery.executors.celery_executor.app inspect ping

For more information, see: `Management Command-line Utilities (inspect/control) <https://docs.celeryproject.org/en/stable/userguide/monitoring.html#monitoring-control>`__ and `Workers Guide <https://docs.celeryproject.org/en/stable/userguide/workers.html>`__ in the Celery documentation.

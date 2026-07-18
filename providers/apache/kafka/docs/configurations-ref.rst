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

.. _configuration:kafka:

.. include:: /../../../../devel-common/src/sphinx_exts/includes/providers-configurations-ref.rst
.. include:: /../../../../devel-common/src/sphinx_exts/includes/sections-and-options.rst


Highlighted configurations
===========================

The ``[kafka_event_producer]`` section configures the ``KafkaEventProducerPlugin``,
which publishes Airflow DagRun and TaskInstance state-change events to a
Kafka topic. DagRun and TaskInstance events are separated and enabled by
distinct flags. Both event-type flags default to ``False``.

.. _configuration_kafka_event_producer_use_cases:kafka:

Common use-cases
----------------

  * Consume the Kafka events by an external observability or analytics tool and gather info about the state
    of multiple Airflow instances without polling their metadata DBs.
  * Based on the state of a DagRun, trigger a downstream external system/pipeline (notifications, alerting,
    cross-team handoffs) without direct interaction with Airflow.
  * Coordinate Dags across multiple Airflow instances over a shared Kafka service.

    * For example, team_A with Airflow instance_A has a deferred task which is triggered
      when a task from team_B with Airflow instance_B finishes.

.. _configuration_kafka_event_producer_activation:kafka:

Activating the plugin
---------------------

To enable event publishing you need to

  * enable at least one event-type flag
  * point the plugin at an Airflow Kafka connection via ``kafka_config_id``
    (defaults to ``kafka_default``) that carries the broker address and any
    other confluent-kafka client options on its extras
  * have a pre-existing kafka topic

.. code-block:: ini

    [kafka_event_producer]
    dag_run_events_enabled = True
    task_instance_events_enabled = True
    kafka_config_id = kafka_events
    topic = airflow.events

The connection's ``extra`` JSON accepts the full confluent-kafka client
configuration — including SASL/TLS options and callbacks (e.g. ``error_cb``,
``oauth_cb``) given as dotted-path strings, which are resolved to callables
before the producer is built.

.. code-block:: json

    {
        "bootstrap.servers": "broker:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": "my_company.auth.oauth_cb"
    }

Environment-variable equivalents:

.. code-block:: ini

    AIRFLOW__KAFKA_EVENT_PRODUCER__DAG_RUN_EVENTS_ENABLED=True
    AIRFLOW__KAFKA_EVENT_PRODUCER__TASK_INSTANCE_EVENTS_ENABLED=True
    AIRFLOW__KAFKA_EVENT_PRODUCER__KAFKA_CONFIG_ID=kafka_events
    AIRFLOW__KAFKA_EVENT_PRODUCER__TOPIC=airflow.events

The two event flags are independent, users can opt-in to get only DagRun
event messages or only TaskInstance event messages or both.

The topic must already exist on the broker, it's not auto-created. On a missing
topic, broker connection failure, or any other producer init error, the plugin
doesn't fail, instead it logs a warning and retries the init after ``topic_check_retry_interval``
seconds (default ``60``). Once the topic is created on the broker the plugin will pick it up.

.. _configuration_kafka_event_producer_filtering:kafka:

Filtering events
----------------

DagRun and TaskInstance events are filtered separately. Each filter is a
comma-separated list of ``fnmatch`` glob patterns; an empty list means
"allow all", and **deny takes precedence over allow**.

.. code-block:: ini

    [kafka_event_producer]
    dag_run_dag_id_allowlist = sales_*,marketing_*
    dag_run_dag_id_denylist = sales_internal_*

    task_instance_dag_id_allowlist = sales_*
    task_instance_dag_id_denylist =
    task_instance_task_id_allowlist = load_*,extract_*
    task_instance_task_id_denylist = *_cleanup

TaskInstance events must pass *both* the dag-id and task-id filters.
Mapped task instances share their parent's ``task_id``, so a single
``task_id`` pattern covers every map index.

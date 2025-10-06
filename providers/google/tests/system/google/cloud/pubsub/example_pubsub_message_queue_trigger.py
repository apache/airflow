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

"""
Example Airflow DAG that demonstrates using Google Cloud Pub/Sub with MessageQueueTrigger
and Asset Watchers for event-driven workflows.

This example shows how to create a DAG that triggers when messages arrive in a
Google Cloud Pub/Sub subscription using Asset Watchers.

Prerequisites
-------------

Before running this example, ensure you have:

1. A GCP project with Pub/Sub API enabled
2. The following Pub/Sub resources created in your project:

   - Topic: ``test-topic``
   - Subscription: ``test-subscription``

You can create these resources using:

.. code-block:: bash

    # Create topic
    gcloud pubsub topics create test-topic --project={PROJECT_ID}

    # Create subscription
    gcloud pubsub subscriptions create test-subscription \\
        --topic=test-topic --project={PROJECT_ID}

How to test
-----------

1. Ensure the Pub/Sub resources exist (see Prerequisites above)
2. Publish a message to trigger the DAG:

   .. code-block:: bash

       gcloud pubsub topics publish test-topic \\
           --message="Test message" --project={PROJECT_ID}

3. The DAG will be triggered automatically when the message arrives
"""

from __future__ import annotations

# [START howto_trigger_pubsub_message_queue]
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, AssetWatcher

# Define a trigger that listens to a Google Cloud Pub/Sub subscription
trigger = MessageQueueTrigger(
    scheme="google+pubsub",
    project_id="my-project",
    subscription="test-subscription",
    ack_messages=True,
    max_messages=1,
    gcp_conn_id="google_cloud_default",
    poke_interval=60.0,
)

# Define an asset that watches for messages on the Pub/Sub subscription
asset = Asset("pubsub_queue_asset_1", watchers=[AssetWatcher(name="pubsub_watcher_1", trigger=trigger)])

with DAG(
    dag_id="example_pubsub_message_queue_trigger",
    schedule=[asset],
) as dag:
    process_message_task = EmptyOperator(task_id="process_pubsub_message")
# [END howto_trigger_pubsub_message_queue]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)

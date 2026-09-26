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

.. _write-logs-s3-compatible:

Use an S3-compatible object store for Airflow remote task logs
==============================================================

The Amazon provider talks to any S3-compatible object store, not just Amazon S3. Because the
:ref:`S3 remote task handler <write-logs-amazon-s3>` issues standard S3 API calls, pointing the
``aws`` connection at a custom ``endpoint_url`` makes it write Airflow task logs to that
endpoint with no new provider and no core change. You use the ``s3://`` scheme in
``[logging]`` exactly as you would for Amazon S3, and the same connection also backs
``ObjectStoragePath("s3://...")`` for Dag data.

Amazon S3 is the baseline. The same steps work against other services that expose an
S3-compatible API, for example Backblaze B2, Cloudflare R2, and MinIO. The only per-provider
differences are the endpoint URL, the region, and whether path-style addressing is required.

This recipe targets Airflow 3.x with ``apache-airflow-providers-amazon``.

Prerequisites
-------------

- A bucket for logs (private). The examples use ``$S3_BUCKET_NAME``.
- An access key and secret scoped to that bucket. Prefer a bucket-scoped key over an
  account-wide one.
- ``apache-airflow-providers-amazon`` installed. For ``ObjectStoragePath`` you also need the
  ``s3fs`` extra: ``pip install 'apache-airflow-providers-amazon[s3fs]'``.

Every S3-compatible service issues an access key id and a secret access key. Map them onto the
AWS connection fields as follows.

============================  =================================  =============================================
S3-compatible value           Standardized env var               AWS connection field
============================  =================================  =============================================
Access key id                 ``S3_ACCESS_KEY_ID``               ``login`` (AWS access key id)
Secret access key             ``S3_SECRET_ACCESS_KEY``           ``password`` (AWS secret access key)
Bucket name                   ``S3_BUCKET_NAME``                 used in ``remote_base_log_folder``
Region                        ``S3_REGION``                      ``extra.region_name``
S3 endpoint                   ``S3_ENDPOINT``                    ``extra.endpoint_url``
============================  =================================  =============================================

Find the endpoint and region for your bucket in your provider's console or CLI. For Amazon S3
the endpoint is the default AWS endpoint and you can omit ``endpoint_url`` entirely; for other
S3-compatible services set ``endpoint_url`` to the provider's S3 endpoint, such as
``https://your-s3-endpoint.example.com``. The connection ``extra.endpoint_url`` must include a
scheme, for example ``https://`` or ``http://``.

Step 1: Create the connection pointing at your endpoint
-------------------------------------------------------

Create an ``aws`` connection whose ``endpoint_url`` extra is your S3 endpoint. The Amazon
provider sends every S3 call to that endpoint instead of the AWS default, which is what makes
the S3 handler talk to your store. For Amazon S3 you can leave ``endpoint_url`` unset and the
provider uses the default AWS endpoint.

Using an environment-variable connection (no secrets on the command line):

.. code-block:: bash

    export AIRFLOW_CONN_AWS_S3='{
      "conn_type": "aws",
      "login": "'"$S3_ACCESS_KEY_ID"'",
      "password": "'"$S3_SECRET_ACCESS_KEY"'",
      "extra": {
        "endpoint_url": "'"$S3_ENDPOINT"'",
        "region_name": "'"$S3_REGION"'",
        "config_kwargs": {"s3": {"addressing_style": "path"}}
      }
    }'

The ``config_kwargs`` ``addressing_style: path`` selects path-style addressing
(``endpoint/bucket/key``). Amazon S3 accepts both styles; several S3-compatible services expect
path-style addressing, so set it when your provider requires it.

The equivalent JSON when you create the connection in the UI (``Admin -> Connections``,
connection type ``Amazon Web Services``) or store it in a secrets backend:

.. code-block:: json

    {
      "conn_type": "aws",
      "login": "<S3_ACCESS_KEY_ID>",
      "password": "<S3_SECRET_ACCESS_KEY>",
      "extra": {
        "endpoint_url": "https://your-s3-endpoint.example.com",
        "region_name": "us-east-1",
        "config_kwargs": {"s3": {"addressing_style": "path"}}
      }
    }

Never hardcode the secret access key in a Dag, ``airflow.cfg``, or version control. Read it
from the environment or a secrets backend.

Step 2: Enable remote logging to the bucket
-------------------------------------------

Configure the ``[logging]`` section of ``airflow.cfg`` (or the equivalent
``AIRFLOW__LOGGING__*`` environment variables) so Airflow uploads task logs to the bucket
through the connection from Step 1:

.. code-block:: ini

    [logging]
    remote_logging = True
    remote_base_log_folder = s3://<S3_BUCKET_NAME>/logs
    remote_log_conn_id = aws_s3
    # Some S3-compatible stores do not support server-side encryption headers; leave this off
    # unless your store supports them.
    encrypt_s3_logs = False

The ``s3://`` scheme routes through the same S3 task handler used for Amazon S3
(``S3TaskHandler``), which resolves ``S3Hook(aws_conn_id="aws_s3")`` and therefore inherits the
endpoint from the connection extra. Restart the scheduler, the API server, the triggerer, and
the workers so they pick up the new configuration.

As environment variables:

.. code-block:: bash

    export AIRFLOW__LOGGING__REMOTE_LOGGING=True
    export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="s3://${S3_BUCKET_NAME}/logs"
    export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_s3

The access key needs the equivalent of ``s3:ListBucket`` on the bucket and ``s3:GetObject`` /
``s3:PutObject`` on the log prefix. A key scoped to the bucket with read and write capabilities
covers this.

Step 3: Verify
--------------

- Trigger any example Dag and let a task finish.
- Confirm the objects appear under ``s3://<S3_BUCKET_NAME>/logs/`` in your provider's console
  or with any S3 client (for example ``aws s3 ls s3://<S3_BUCKET_NAME>/logs/ --endpoint-url
  "$S3_ENDPOINT"``).
- Open the task log in the Airflow UI. The log is served from the remote store; a banner notes
  that the log was read from the remote location.

Reusing the connection for Dag data
-----------------------------------

The same ``aws_s3`` connection backs ``ObjectStoragePath`` for reading and writing Dag data,
because ``ObjectStoragePath("s3://...")`` builds an ``s3fs`` filesystem from the connection and
inherits the same ``endpoint_url``:

.. code-block:: python

    from airflow.sdk import ObjectStoragePath

    base = ObjectStoragePath("s3://aws_s3@my-bucket/airflow-demo/")

See the example Dag ``example_s3_compatible_object_storage`` for an input to transform to
output pipeline against an S3-compatible store.

Troubleshooting
---------------

- ``SignatureDoesNotMatch`` or ``403``: re-check that ``endpoint_url`` includes a URL scheme,
  that ``region_name`` matches the bucket's region, and that ``login`` / ``password`` hold the
  access key id and secret access key for a key scoped to the bucket.
- Logs stay local only: confirm ``remote_logging = True`` and that the components were
  restarted. ``remote_base_log_folder`` must start with ``s3://``.
- ``ImportError`` for ``s3fs`` when using ``ObjectStoragePath``: install the extra with
  ``pip install 'apache-airflow-providers-amazon[s3fs]'``.

Trigger Dags from bucket event notifications
--------------------------------------------

Beyond logs and data, the same bucket can drive upload-triggered pipelines. Many S3-compatible
services can send a webhook when an object is created or deleted in a bucket, and Airflow
exposes a REST endpoint that creates a Dag run. Wiring the two together turns an upload into an
Airflow Dag run, with no polling sensor.

The flow is: a client uploads an object to the bucket; the object store posts an
event-notification payload to an HTTPS webhook you control (for example a small function or API
gateway); that webhook authenticates to the Airflow REST API and triggers the Dag, passing the
object key in the run configuration. The endpoint is ``POST /api/v2/dags/{dag_id}/dagRuns``
with a JSON body that accepts ``logical_date``, an optional ``dag_run_id``, and a ``conf``
object. Put the object key from the notification into ``conf`` so the Dag knows which object to
process:

.. code-block:: bash

    curl -X POST "$AIRFLOW_API/api/v2/dags/s3_event_ingestion/dagRuns" \
      -H "Authorization: Bearer $AIRFLOW_JWT" \
      -H "Content-Type: application/json" \
      -d '{"logical_date": "2026-01-01T00:00:00Z", "conf": {"object_key": "incoming/file.txt"}}'

A Dag built for this pattern can read ``conf["object_key"]`` and process that object straight
from the store with ``ObjectStoragePath(f"s3://aws_s3@{bucket}/{object_key}")``, reusing the
connection from Step 1. Keep the webhook thin: validate the notification, mint or hold a
short-lived Airflow API token, and forward only the object key. This pattern fits ingestion
pipelines such as transcode on upload or index on upload, where work should start the moment
data lands in the bucket.

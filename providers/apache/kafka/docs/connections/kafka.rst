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

.. _howto/connection:kafka:

Apache Kafka Connection
========================

The Apache Kafka connection type configures a connection to Apache Kafka via the ``confluent-kafka`` Python package.

.. |Kafka Connection| image:: kafka_connection.png
    :width: 400
    :alt: Kafka Connection Screenshot


Default Connection IDs
----------------------

Kafka hooks and operators use ``kafka_default`` by default, this connection is very minimal and should not be assumed useful for more than the most trivial of testing.

Configuring the Connection
--------------------------

Connections are configured as a json serializable string provided to the ``extra`` field. The ``error_cb`` parameter can be
used to specify a callback function by providing a path to the function. e.g ``"module.callback_func"``. A full list
of parameters are described in the
`Confluent Kafka python library <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>`_.

If you are defining the Airflow connection from the Airflow UI, the ``extra`` field will be renamed to ``Config Dict``.

Most operators and hooks will check that at the minimum the ``bootstrap.servers`` key exists and has a value set to be valid.

Amazon MSK with IAM authentication
----------------------------------

`Amazon MSK <https://aws.amazon.com/msk/>`_ clusters (both provisioned and serverless) can be
authenticated with `IAM <https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html>`_.
This requires the ``aws-msk-iam-sasl-signer-python`` package, which is installed with the ``msk`` extra:

.. code-block:: bash

    pip install apache-airflow-providers-apache-kafka[msk]

When the ``bootstrap.servers`` point at an Amazon MSK endpoint (for example
``*.kafka.<region>.amazonaws.com`` or ``*.kafka-serverless.<region>.amazonaws.com``) and
``sasl.mechanism`` is set to ``OAUTHBEARER``, the hook automatically generates and refreshes the
IAM authentication token, deriving the AWS region from the bootstrap servers. The credentials are
resolved by the signer using the standard AWS credential provider chain (environment variables,
shared config/credentials files, instance/task IAM roles, etc.).

An example ``extra`` (``Config Dict``) for an MSK connection:

.. code-block:: json

    {
        "bootstrap.servers": "boot-abcde1.c2.kafka-serverless.us-east-1.amazonaws.com:9098",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "group.id": "my-group"
    }

An explicit ``oauth_cb`` provided in the connection configuration is always respected and is never
overwritten by the automatic MSK IAM callback.

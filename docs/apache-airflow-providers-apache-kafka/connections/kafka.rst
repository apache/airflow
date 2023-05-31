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

.. _howto/connection: kafka

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

Connections are configured as a json serializable string provided to the ``extra`` field. A full list of parameters
are described in the `Confluent Kafka python library <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>`_.

If you are defining the Airflow connection from the Airflow UI, the ``extra`` field will be renamed to ``Config Dict``.

Most operators and hooks will check that at the minimum the ``bootstrap.servers`` key exists and has a value set to be valid.

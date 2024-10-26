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

==============
Amazon Neptune
==============

`Amazon Neptune Database <https://aws.amazon.com/neptune/>`__ is a serverless graph database designed
for superior scalability and availability. Neptune Database provides built-in security,
continuous backups, and integrations with other AWS services.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:NeptuneStartDbClusterOperator:

Start a Neptune database cluster
================================

To start a existing Neptune database cluster, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune.StartNeptuneDbClusterOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. note::
    This operator only starts an existing Neptune database cluster, it does not create a cluster.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_neptune.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_neptune_cluster]
    :end-before: [END howto_operator_start_neptune_cluster]

.. _howto/operator:StopNeptuneDbClusterOperator:

Stop a Neptune database cluster
===============================

To stop a running Neptune database cluster, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune.StartNeptuneDbClusterOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_neptune.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_stop_neptune_cluster]
    :end-before: [END howto_operator_stop_neptune_cluster]

Reference
---------

* `AWS boto3 library documentation for Neptune <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune.html>`__

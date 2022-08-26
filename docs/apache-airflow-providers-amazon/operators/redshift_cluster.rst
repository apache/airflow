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

===============
Amazon Redshift
===============

`Amazon Redshift <https://aws.amazon.com/redshift/>`__ manages all the work of setting up, operating, and scaling a data warehouse:
provisioning capacity, monitoring and backing up the cluster, and applying patches and upgrades to
the Amazon Redshift engine. You can focus on using your data to acquire new insights for your
business and customers.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:RedshiftCreateClusterOperator:

Create an Amazon Redshift cluster
=================================

To create an Amazon Redshift Cluster with the specified parameters you can use
:class:`~airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftCreateClusterOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_redshift_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_cluster]
    :end-before: [END howto_operator_redshift_cluster]

.. _howto/operator:RedshiftResumeClusterOperator:

Resume an Amazon Redshift cluster
=================================

To resume a 'paused' Amazon Redshift cluster you can use
:class:`RedshiftResumeClusterOperator <airflow.providers.amazon.aws.operators.redshift_cluster>`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_redshift_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_resume_cluster]
    :end-before: [END howto_operator_redshift_resume_cluster]

.. _howto/operator:RedshiftPauseClusterOperator:

Pause an Amazon Redshift cluster
================================

To pause an 'available' Amazon Redshift cluster you can use
:class:`RedshiftPauseClusterOperator <airflow.providers.amazon.aws.operators.redshift_cluster>`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_redshift_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_pause_cluster]
    :end-before: [END howto_operator_redshift_pause_cluster]

.. _howto/operator:RedshiftCreateClusterSnapshotOperator:

Create an Amazon Redshift cluster snapshot
==========================================

To create Amazon Redshift cluster snapshot you can use
:class:`RedshiftCreateClusterSnapshotOperator <airflow.providers.amazon.aws.operators.redshift_cluster>`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_redshift_cluster.py
  :language: python
  :dedent: 4
  :start-after: [START howto_operator_redshift_create_cluster_snapshot]
  :end-before: [END howto_operator_redshift_create_cluster_snapshot]

.. _howto/operator:RedshiftDeleteClusterOperator:

Delete an Amazon Redshift cluster
=================================

To delete an Amazon Redshift cluster you can use
:class:`RedshiftDeleteClusterOperator <airflow.providers.amazon.aws.operators.redshift_cluster>`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_redshift_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_delete_cluster]
    :end-before: [END howto_operator_redshift_delete_cluster]

Sensors
-------

.. _howto/sensor:RedshiftClusterSensor:

Wait on an Amazon Redshift cluster state
========================================

To check the state of an Amazon Redshift Cluster until it reaches the target state or another terminal
state you can use :class:`~airflow.providers.amazon.aws.sensors.redshift_cluster.RedshiftClusterSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_redshift_cluster.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_redshift_cluster]
    :end-before: [END howto_sensor_redshift_cluster]

Reference
---------

* `AWS boto3 library documentation for Amazon Redshift <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html>`__

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

========================
Amazon Neptune Analytics
========================

`Amazon Neptune Analytics <https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html>`__ is a memory-optimized graph database engine for analytics. With Neptune Analytics, you can get insights and find trends by processing large amounts of graph data in seconds.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:NeptuneCreateGraphOperator:

Create a new Neptune Graph
==========================

To create a new Neptune Analytics Graph, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneCreateGraphOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_neptune_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_analytics_create_graph]
    :end-before: [END howto_operator_neptune_analytics_create_graph]


.. _howto/operator:NeptuneDeleteGraphOperator:

Delete a Neptune Graph
======================

To delete an existing Neptune Analytics Graph, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneDeleteGraphOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_neptune_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_analytics_delete_graph]
    :end-before: [END howto_operator_neptune_analytics_delete_graph]

.. _howto/operator:NeptuneCreatePrivateGraphEndpointOperator:

Create a Neptune Graph private endpoint
=======================================

To create a VPC Endpoint for connecting to an existing Neptune Graph, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneCreatePrivateGraphEndpointOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_neptune_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_analytics_create_private_endpoint]
    :end-before: [END howto_operator_neptune_analytics_create_private_endpoint]

.. _howto/operator:NeptuneDeletePrivateGraphEndpointOperator:

Delete a Neptune Graph private endpoint
=======================================

To delete a VPC Endpoint attached to an existing Neptune Graph, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneDeletePrivateGraphEndpointOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_neptune_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_analytics_delete_private_endpoint]
    :end-before: [END howto_operator_neptune_analytics_delete_private_endpoint]

.. _howto/operator:NeptuneCreateGraphWithImportOperator:

Create a Neptune Graph with a data import task
==============================================

To create a Neptune Analytics Graph and immediately import data, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneCreateGraphWithImportOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_neptune_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_analytics_create_graph_with_import]
    :end-before: [END howto_operator_neptune_analytics_create_graph_with_import]

.. _howto/operator:NeptuneStartImportTaskOperator:

Import data into an existing Neptune Graph
==========================================

To import data into an existing Neptune Analytics Graph, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneStartImportTaskOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_neptune_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_analytics_start_import_task]
    :end-before: [END howto_operator_neptune_analytics_start_import_task]

.. _howto/operator:NeptuneCancelImportTaskOperator:

Cancel a running import task
============================

To cancel an existing import task, you can use
:class:`~airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneCancelImportTaskOperator`.
This operator can be run in deferrable mode by passing ``deferrable=True`` as a parameter. This requires
the aiobotocore module to be installed.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_neptune_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_neptune_analytics_cancel_import_task]
    :end-before: [END howto_operator_neptune_analytics_cancel_import_task]

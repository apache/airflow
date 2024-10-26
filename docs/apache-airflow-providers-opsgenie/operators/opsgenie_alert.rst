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

.. _howto/operator:OpsgenieCreateAlertOperator:

OpsgenieCreateAlertOperator
===========================

Use the :class:`~airflow.providers.opsgenie.operators.opsgenie.OpsgenieCreateAlertOperator` to send an alert to opsgenie.


Using the Operator
^^^^^^^^^^^^^^^^^^
Send an alert to Opsgenie with a specific message.

.. exampleinclude:: /../../providers/tests/system/opsgenie/example_opsgenie_alert.py
    :language: python
    :start-after: [START howto_opsgenie_create_alert_operator]
    :end-before: [END howto_opsgenie_create_alert_operator]

.. _howto/operator:OpsgenieCloseAlertOperator:

OpsgenieCloseAlertOperator
==========================

Use the :class:`~airflow.providers.opsgenie.operators.opsgenie.OpsgenieCloseAlertOperator` to close alert in opsgenie.


Using the Operator
^^^^^^^^^^^^^^^^^^
Close alert in Opsgenie.

.. exampleinclude:: /../../providers/tests/system/opsgenie/example_opsgenie_alert.py
    :language: python
    :start-after: [START howto_opsgenie_close_alert_operator]
    :end-before: [END howto_opsgenie_close_alert_operator]

.. _howto/operator:OpsgenieDeleteAlertOperator:

OpsgenieDeleteAlertOperator
===========================

Use the :class:`~airflow.providers.opsgenie.operators.opsgenie.OpsgenieDeleteAlertOperator` to delete alert in opsgenie.


Using the Operator
^^^^^^^^^^^^^^^^^^
Delete alert in Opsgenie.

.. exampleinclude:: /../../providers/tests/system/opsgenie/example_opsgenie_alert.py
    :language: python
    :dedent: 4
    :start-after: [START howto_opsgenie_delete_alert_operator]
    :end-before: [END howto_opsgenie_delete_alert_operator]

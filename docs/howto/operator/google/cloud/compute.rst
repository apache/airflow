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



Google Compute Engine Operators
===============================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:ComputeEngineStartInstanceOperator:

ComputeEngineStartInstanceOperator
----------------------------------

Use the
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineStartInstanceOperator`
to start an existing Google Compute Engine instance.

Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start]
    :end-before: [END howto_operator_gce_start]

You can also create the operator without project id - project id will be retrieved
from the GCP connection id used:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start_no_project_id]
    :end-before: [END howto_operator_gce_start_no_project_id]


Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_start_template_fields]
    :end-before: [END gce_instance_start_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `start an instance
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/start>`_.

.. _howto/operator:ComputeEngineStopInstanceOperator:

ComputeEngineStopInstanceOperator
---------------------------------

Use the operator to stop Google Compute Engine instance.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineStopInstanceOperator`

Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop]
    :end-before: [END howto_operator_gce_stop]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop_no_project_id]
    :end-before: [END howto_operator_gce_stop_no_project_id]

Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_stop_template_fields]
    :end-before: [END gce_instance_stop_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `stop an instance
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/stop>`_.

.. _howto/operator:ComputeEngineSetMachineTypeOperator:

ComputeEngineSetMachineTypeOperator
-----------------------------------

Use the operator to change machine type of a Google Compute Engine instance.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineSetMachineTypeOperator`.

Arguments
"""""""""



Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type]
    :end-before: [END howto_operator_gce_set_machine_type]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type_no_project_id]
    :end-before: [END howto_operator_gce_set_machine_type_no_project_id]

Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_set_machine_type_template_fields]
    :end-before: [END gce_instance_set_machine_type_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `set the machine type
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType>`_.

.. _howto/operator:ComputeEngineCopyInstanceTemplateOperator:

ComputeEngineCopyInstanceTemplateOperator
-----------------------------------------

Use the operator to copy an existing Google Compute Engine instance template
applying a patch to it.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineCopyInstanceTemplateOperator`.

Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute_igm.py
    :language: python
    :start-after: [START howto_operator_compute_template_copy_args]
    :end-before: [END howto_operator_compute_template_copy_args]

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_copy_template]
    :end-before: [END howto_operator_gce_igm_copy_template]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_copy_template_no_project_id]
    :end-before: [END howto_operator_gce_igm_copy_template_no_project_id]

Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_template_copy_operator_template_fields]
    :end-before: [END gce_instance_template_copy_operator_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `create a new instance with an existing template
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates>`_.

.. _howto/operator:ComputeEngineInstanceGroupUpdateManagerTemplateOperator:

ComputeEngineInstanceGroupUpdateManagerTemplateOperator
-------------------------------------------------------

Use the operator to update a template in Google Compute Engine Instance Group Manager.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineInstanceGroupUpdateManagerTemplateOperator`.

Arguments
"""""""""


Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute_igm.py
    :language: python
    :start-after: [START howto_operator_compute_igm_update_template_args]
    :end-before: [END howto_operator_compute_igm_update_template_args]

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_update_template]
    :end-before: [END howto_operator_gce_igm_update_template]

You can also create the operator without project id - project id will be retrieved
from the GCP connection used:

.. exampleinclude:: ../../../../../airflow/providers/google/cloud/example_dags/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_update_template_no_project_id]
    :end-before: [END howto_operator_gce_igm_update_template_no_project_id]


Templating
""""""""""

.. literalinclude:: ../../../../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_igm_update_template_operator_template_fields]
    :end-before: [END gce_igm_update_template_operator_template_fields]

Troubleshooting
"""""""""""""""

You might find that your ComputeEngineInstanceGroupUpdateManagerTemplateOperator fails with
missing permissions. To execute the operation, the service account requires
the permissions that theService Account User role provides
(assigned via Google Cloud IAM).

More information
""""""""""""""""

See Google Compute Engine API documentation to `manage a group instance
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers>`_.

Reference
---------

For further information, look at:

* `Google Cloud API Documentation <https://cloud.google.com/compute/docs/reference/rest/v1/>`__
* `Product Documentation <https://cloud.google.com/bigtable/docs/>`__

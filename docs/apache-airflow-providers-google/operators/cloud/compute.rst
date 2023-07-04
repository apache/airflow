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

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:ComputeEngineInsertInstanceOperator:

ComputeEngineInsertInstanceOperator
-----------------------------------

Use the
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineInsertInstanceOperator`
to create new Google Compute Engine instance.

Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_insert]
    :end-before: [END howto_operator_gce_insert]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection id used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_insert_no_project_id]
    :end-before: [END howto_operator_gce_insert_no_project_id]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_insert_fields]
    :end-before: [END gce_instance_insert_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `insert an instance
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert>`_.

.. _howto/operator:ComputeEngineInsertInstanceFromTemplateOperator:

ComputeEngineInsertInstanceFromTemplateOperator
-----------------------------------------------

Use the
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineInsertInstanceFromTemplateOperator`
to create new Google Compute Engine instance based on specified instance template.

Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_insert_from_template]
    :end-before: [END howto_operator_gce_insert_from_template]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection id used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_insert_from_template_no_project_id]
    :end-before: [END howto_operator_gce_insert_from_template_no_project_id]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_insert_from_template_fields]
    :end-before: [END gce_instance_insert_from_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `insert an instance from template
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert>`_.

.. _howto/operator:ComputeEngineDeleteInstanceOperator:

ComputeEngineDeleteInstanceOperator
-----------------------------------

Use the
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineDeleteInstanceOperator`
to delete an existing Google Compute Engine instance.

Using the operator
""""""""""""""""""

You can create the operator without project id - project id will be retrieved
from the Google Cloud connection id used. The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_delete_no_project_id]
    :end-before: [END howto_operator_gce_delete_no_project_id]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_delete_template_fields]
    :end-before: [END gce_instance_delete_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `delete an instance
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/delete>`_.

.. _howto/operator:ComputeEngineStartInstanceOperator:

ComputeEngineStartInstanceOperator
----------------------------------

Use the
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineStartInstanceOperator`
to start an existing Google Compute Engine instance.

Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start]
    :end-before: [END howto_operator_gce_start]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection id used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_start_no_project_id]
    :end-before: [END howto_operator_gce_start_no_project_id]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
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

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop]
    :end-before: [END howto_operator_gce_stop]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_stop_no_project_id]
    :end-before: [END howto_operator_gce_stop_no_project_id]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
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

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type]
    :end-before: [END howto_operator_gce_set_machine_type]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_set_machine_type_no_project_id]
    :end-before: [END howto_operator_gce_set_machine_type_no_project_id]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_set_machine_type_template_fields]
    :end-before: [END gce_instance_set_machine_type_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `set the machine type
<https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType>`_.

.. _howto/operator:ComputeEngineDeleteInstanceTemplateOperator:

ComputeEngineDeleteInstanceTemplateOperator
-------------------------------------------

Use the operator to delete Google Compute Engine instance template.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineDeleteInstanceTemplateOperator`.

Using the operator
""""""""""""""""""

The code to create the operator:

You can create the operator without project id - project id will be retrieved
from the Google Cloud connection used. The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_delete_old_template_no_project_id]
    :end-before: [END howto_operator_gce_delete_old_template_no_project_id]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_template_delete_fields]
    :end-before: [END gce_instance_template_delete_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `delete a template
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates/delete>`_.

.. _howto/operator:ComputeEngineInsertInstanceTemplateOperator:

ComputeEngineInsertInstanceTemplateOperator
-------------------------------------------

Use the operator to create Google Compute Engine instance template.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineInsertInstanceTemplateOperator`.

Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_insert_template]
    :end-before: [END howto_operator_gce_igm_insert_template]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_insert_template_no_project_id]
    :end-before: [END howto_operator_gce_igm_insert_template_no_project_id]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_template_insert_fields]
    :end-before: [END gce_instance_template_insert_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `create a new template
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates/insert>`_.

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

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_copy_template]
    :end-before: [END howto_operator_gce_igm_copy_template]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_copy_template_no_project_id]
    :end-before: [END howto_operator_gce_igm_copy_template_no_project_id]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_instance_template_copy_operator_template_fields]
    :end-before: [END gce_instance_template_copy_operator_template_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `create a new instance with an existing template
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates>`_.

.. _howto/operator:ComputeEngineInsertInstanceGroupManagerOperator:

ComputeEngineInsertInstanceGroupManagerOperator
-----------------------------------------------

Use the operator to create a Compute Engine Instance Group Manager.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineInsertInstanceGroupManagerOperator`.

Arguments
"""""""""


Using the operator
""""""""""""""""""

The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :start-after: [START howto_operator_gce_insert_igm]
    :end-before: [END howto_operator_gce_insert_igm]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_insert_igm_no_project_id]
    :end-before: [END howto_operator_gce_insert_igm_no_project_id]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_igm_insert_fields]
    :end-before: [END gce_igm_insert_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `create a group instance
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers/insert>`_.

.. _howto/operator:ComputeEngineDeleteInstanceGroupManagerOperator:

ComputeEngineDeleteInstanceGroupManagerOperator
-----------------------------------------------

Use the operator to delete a Compute Engine Instance Group Manager.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.compute.ComputeEngineDeleteInstanceGroupManagerOperator`.

Arguments
"""""""""


Using the operator
""""""""""""""""""

You can create the operator without project id - project id will be retrieved
from the Google Cloud connection used. The code to create the operator:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_delete_igm_no_project_id]
    :end-before: [END howto_operator_gce_delete_igm_no_project_id]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
    :language: python
    :dedent: 4
    :start-after: [START gce_igm_delete_fields]
    :end-before: [END gce_igm_delete_fields]

More information
""""""""""""""""

See Google Compute Engine API documentation to `delete a group instance
<https://cloud.google.com/compute/docs/reference/rest/v1/instanceGroupManagers/delete>`_.

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

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_update_template]
    :end-before: [END howto_operator_gce_igm_update_template]

You can also create the operator without project id - project id will be retrieved
from the Google Cloud connection used:

.. exampleinclude:: /../../tests/system/providers/google/cloud/compute/example_compute_igm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gce_igm_update_template_no_project_id]
    :end-before: [END howto_operator_gce_igm_update_template_no_project_id]


Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/compute.py
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

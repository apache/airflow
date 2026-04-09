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


Azure Virtual Machine Operators
================================

Waiting Strategy
----------------
The VM action operators support two patterns:

* ``wait_for_completion=True`` (default): operator blocks until Azure operation finishes.
* ``wait_for_completion=False``: operator submits the operation and returns quickly.

When you want to reduce worker slot usage for long VM state transitions, use
``wait_for_completion=False`` together with
:class:`~airflow.providers.microsoft.azure.sensors.compute.AzureVirtualMachineStateSensor`
in ``deferrable=True`` mode to move the waiting to the triggerer.

.. _howto/operator:AzureVirtualMachineStartOperator:

AzureVirtualMachineStartOperator
---------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.compute.AzureVirtualMachineStartOperator`
to start an Azure Virtual Machine.

Below is an example of using this operator to start a VM:

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_vm_start]
    :end-before: [END howto_operator_azure_vm_start]


.. _howto/operator:AzureVirtualMachineStopOperator:

AzureVirtualMachineStopOperator
--------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.compute.AzureVirtualMachineStopOperator`
to stop (deallocate) an Azure Virtual Machine. This releases compute resources and stops billing.

Below is an example of using this operator to stop a VM:

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_vm_stop]
    :end-before: [END howto_operator_azure_vm_stop]


.. _howto/operator:AzureVirtualMachineRestartOperator:

AzureVirtualMachineRestartOperator
-----------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.compute.AzureVirtualMachineRestartOperator`
to restart an Azure Virtual Machine.

Below is an example of using this operator to restart a VM:

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_vm_restart]
    :end-before: [END howto_operator_azure_vm_restart]


.. _howto/sensor:AzureVirtualMachineStateSensor:

AzureVirtualMachineStateSensor
-------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.sensors.compute.AzureVirtualMachineStateSensor`
to poll a VM until it reaches a target power state (e.g., ``running``, ``deallocated``).
This sensor supports deferrable mode.

Below is an example of using this sensor:

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_compute.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_azure_vm_state]
    :end-before: [END howto_sensor_azure_vm_state]


Reference
---------

For further information, look at:

* `Azure Virtual Machines Documentation <https://azure.microsoft.com/en-us/products/virtual-machines/>`__

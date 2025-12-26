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


Microsoft PSRP Operators
=======================================

The
`PowerShell Remoting Protocol (PSRP)
<https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-psrp/>`__
protocol is required whenever a user wants to execute commands on a Windows
server from a client using a native PowerShell runspace.

The :class:`~airflow.providers.microsoft.psrp.operators.psrp.PsrpOperator`
operator implements such client capabilities, enabling the
scheduling of Windows jobs from Airflow. Internally, it makes use of
the `pypsrp <https://pypi.org/project/pypsrp/>`__ client library.

Compared to
:class:`~airflow.providers.microsoft.winrm.operators.winrm.WinRMOperator`,
using PSRP extends the remoting capabilities in Windows, providing
better session control and close integration with the PowerShell
ecosystem (i.e., .NET Runspace interface):

* Run multiple commands in a single session
* Reuse the runspace to create multiple sessions
* Work with PowerShell objects instead of just text
* Use constrained endpoints using JEA (Just-Enough-Administration)
* Ability to use the .NET Runspace interface


Using the Operator
^^^^^^^^^^^^^^^^^^

When instantiating the
:class:`~airflow.providers.microsoft.psrp.operators.psrp.PsrpOperator`
operator, you must provide a cmdlet, command or script using one of the
following named arguments:

.. list-table:: Provide one of the following arguments to the operator
   :widths: 10 15 20
   :header-rows: 1

   * - Argument name
     - Description
     - Examples
   * - cmdlet
     - Invoke a PowerShell cmdlet.
     - ``Copy-Item``, ``Restart-Computer``
   * - command
     - Carries out the specified command using the
       `cmd <https://docs.microsoft.com/en-us/windows-server/
       administration/windows-commands/cmd>`__ command interpreter.
     - ``robocopy C:\Logfiles\* C:\Drawings /S /E``
   * - powershell
     - Run a PowerShell script.
     - ``Copy-Item -Path "C:\Logfiles\*" -Destination "C:\Drawings" -Recurse``


Output
######

PowerShell provides multiple output streams.

In general, the operator logs a record using the built-in logging
mechanism for records that arrive on these streams using a job status
polling mechanism. The success stream (i.e., stdout or shell output)
is handled differently, as explained in the following:

When :doc:`XComs <apache-airflow:core-concepts/xcoms>` are enabled and when
the operator is used with a native PowerShell cmdlet or script, the
shell output is converted to JSON using the ``ConvertTo-Json`` cmdlet
and then decoded on the client-side by the operator such that the
operator's return value is compatible with the serialization required
by XComs.

When XComs are not enabled (that is, ``do_xcom_push`` is set to
false), the shell output is instead logged like the other output
streams and will appear in the task instance log.


Secure strings
##############

The operator adds a template filter ``securestring`` which will encrypt
the value and make it available in the remote session as a
`SecureString
<https://docs.microsoft.com/en-us/dotnet/api/system.security.securestring>`__
type. This ensures for example that the value is not accidentally
logged.

Using the template filter requires the Dag to be configured to
:ref:`render fields as native objects
<concepts:templating-native-objects>` (the default is to coerce all
values into strings which won't work here because we need a value
which has been tagged to be serialized as a secure string). Use
``render_template_as_native_obj=True`` to enable this.

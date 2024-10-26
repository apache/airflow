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

.. _howto/operator:SalesforceApexRestOperator:

SalesforceApexRestOperator
==========================

Use the :class:`~airflow.providers.salesforce.operators.salesforce_apex_rest.SalesforceApexRestOperator` to execute Apex Rest.


Using the Operator
^^^^^^^^^^^^^^^^^^
You can also use this library to call custom Apex methods:

This would call the endpoint ``https://<instance>.salesforce.com/services/apexrest/User/Activity`` with ``payload`` as
the body content encoded with ``json.dumps``

.. exampleinclude:: /../../providers/tests/system/salesforce/example_salesforce_apex_rest.py
    :language: python
    :start-after: [START howto_salesforce_apex_rest_operator]
    :end-before: [END howto_salesforce_apex_rest_operator]

You can read more about Apex on the
`Force.com Apex Code Developer's Guide <https://developer.salesforce.com/docs/atlas.en-us.apexcode.meta/apexcode/apex_dev_guide.htm>`__.

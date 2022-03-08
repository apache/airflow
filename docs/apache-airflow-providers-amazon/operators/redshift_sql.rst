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

.. _howto/operator:RedshiftSQLOperator:

Amazon Redshift Operators
=========================

`Amazon Redshift <https://aws.amazon.com/redshift/>`__ manages all the work of setting up, operating, and scaling a data warehouse:
provisioning capacity, monitoring and backing up the cluster, and applying patches and upgrades to
the Amazon Redshift engine. You can focus on using your data to acquire new insights for your
business and customers.

Airflow provides an operator to execute queries against an Amazon Redshift cluster.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

Redshift SQL
^^^^^^^^^^^^

This operator executes a SQL query against an Amazon Redshift cluster.

Execute a SQL query
"""""""""""""""""""

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_sql]
    :end-before: [END howto_operator_redshift_sql]

Execute a SQL query with parameters
"""""""""""""""""""""""""""""""""""

RedshiftSQLOperator supports the ``parameters`` attribute which allows us to dynamically pass
parameters into SQL statements.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_sql_with_params]
    :end-before: [END howto_operator_redshift_sql_with_params]

Reference
^^^^^^^^^

For further information, look at:

* `Amazon Redshift Python connector <https://docs.aws.amazon.com/redshift/latest/mgmt/python-connect-examples.html>`__

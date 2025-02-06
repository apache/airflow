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

:py:mod:`tests.system.providers.amazon.aws.example_sql_to_s3`
=============================================================

.. py:module:: tests.system.providers.amazon.aws.example_sql_to_s3


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sql_to_s3.create_connection
   tests.system.providers.amazon.aws.example_sql_to_s3.setup_security_group
   tests.system.providers.amazon.aws.example_sql_to_s3.delete_security_group



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sql_to_s3.DAG_ID
   tests.system.providers.amazon.aws.example_sql_to_s3.DB_LOGIN
   tests.system.providers.amazon.aws.example_sql_to_s3.DB_PASS
   tests.system.providers.amazon.aws.example_sql_to_s3.DB_NAME
   tests.system.providers.amazon.aws.example_sql_to_s3.IP_PERMISSION
   tests.system.providers.amazon.aws.example_sql_to_s3.REDSHIFT_TABLE
   tests.system.providers.amazon.aws.example_sql_to_s3.SQL_QUERY
   tests.system.providers.amazon.aws.example_sql_to_s3.SQL_CREATE_TABLE
   tests.system.providers.amazon.aws.example_sql_to_s3.SQL_INSERT_DATA
   tests.system.providers.amazon.aws.example_sql_to_s3.sys_test_context_task
   tests.system.providers.amazon.aws.example_sql_to_s3.test_context
   tests.system.providers.amazon.aws.example_sql_to_s3.test_run


.. py:data:: DAG_ID
   :value: 'example_sql_to_s3'



.. py:data:: DB_LOGIN
   :value: 'adminuser'



.. py:data:: DB_PASS
   :value: 'MyAmazonPassword1'



.. py:data:: DB_NAME
   :value: 'dev'



.. py:data:: IP_PERMISSION



.. py:data:: REDSHIFT_TABLE
   :value: 'test_table'



.. py:data:: SQL_QUERY



.. py:data:: SQL_CREATE_TABLE



.. py:data:: SQL_INSERT_DATA



.. py:data:: sys_test_context_task



.. py:function:: create_connection(conn_id_name, cluster_id)


.. py:function:: setup_security_group(sec_group_name, ip_permissions, vpc_id)


.. py:function:: delete_security_group(sec_group_id, sec_group_name)


.. py:data:: test_context



.. py:data:: test_run

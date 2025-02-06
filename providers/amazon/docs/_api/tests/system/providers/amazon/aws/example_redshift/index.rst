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

:py:mod:`tests.system.providers.amazon.aws.example_redshift`
============================================================

.. py:module:: tests.system.providers.amazon.aws.example_redshift


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_redshift.create_connection
   tests.system.providers.amazon.aws.example_redshift.setup_security_group
   tests.system.providers.amazon.aws.example_redshift.delete_security_group



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_redshift.DAG_ID
   tests.system.providers.amazon.aws.example_redshift.DB_LOGIN
   tests.system.providers.amazon.aws.example_redshift.DB_PASS
   tests.system.providers.amazon.aws.example_redshift.DB_NAME
   tests.system.providers.amazon.aws.example_redshift.POLL_INTERVAL
   tests.system.providers.amazon.aws.example_redshift.IP_PERMISSION
   tests.system.providers.amazon.aws.example_redshift.sys_test_context_task
   tests.system.providers.amazon.aws.example_redshift.test_context
   tests.system.providers.amazon.aws.example_redshift.test_run


.. py:data:: DAG_ID
   :value: 'example_redshift'



.. py:data:: DB_LOGIN
   :value: 'adminuser'



.. py:data:: DB_PASS
   :value: 'MyAmazonPassword1'



.. py:data:: DB_NAME
   :value: 'dev'



.. py:data:: POLL_INTERVAL
   :value: 10



.. py:data:: IP_PERMISSION



.. py:data:: sys_test_context_task



.. py:function:: create_connection(conn_id_name, cluster_id)


.. py:function:: setup_security_group(sec_group_name, ip_permissions, vpc_id)


.. py:function:: delete_security_group(sec_group_id, sec_group_name)


.. py:data:: test_context



.. py:data:: test_run

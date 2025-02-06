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

:py:mod:`tests.system.providers.amazon.aws.example_dms`
=======================================================

.. py:module:: tests.system.providers.amazon.aws.example_dms

.. autoapi-nested-parse::

   Note:  DMS requires you to configure specific IAM roles/permissions.  For more information, see
   https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Security.html#CHAP_Security.APIRole



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_dms.create_security_group
   tests.system.providers.amazon.aws.example_dms.create_sample_table
   tests.system.providers.amazon.aws.example_dms.create_dms_assets
   tests.system.providers.amazon.aws.example_dms.delete_dms_assets
   tests.system.providers.amazon.aws.example_dms.delete_security_group



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_dms.DAG_ID
   tests.system.providers.amazon.aws.example_dms.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_dms.sys_test_context_task
   tests.system.providers.amazon.aws.example_dms.RDS_ENGINE
   tests.system.providers.amazon.aws.example_dms.RDS_PROTOCOL
   tests.system.providers.amazon.aws.example_dms.RDS_USERNAME
   tests.system.providers.amazon.aws.example_dms.RDS_PASSWORD
   tests.system.providers.amazon.aws.example_dms.TABLE_HEADERS
   tests.system.providers.amazon.aws.example_dms.SAMPLE_DATA
   tests.system.providers.amazon.aws.example_dms.SG_IP_PERMISSION
   tests.system.providers.amazon.aws.example_dms.test_context
   tests.system.providers.amazon.aws.example_dms.test_run


.. py:data:: DAG_ID
   :value: 'example_dms'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:data:: RDS_ENGINE
   :value: 'postgres'



.. py:data:: RDS_PROTOCOL
   :value: 'postgresql'



.. py:data:: RDS_USERNAME
   :value: 'username'



.. py:data:: RDS_PASSWORD
   :value: 'rds_password'



.. py:data:: TABLE_HEADERS
   :value: ['apache_project', 'release_year']



.. py:data:: SAMPLE_DATA
   :value: [('Airflow', '2015'), ('OpenOffice', '2012'), ('Subversion', '2000'), ('NiFi', '2006')]



.. py:data:: SG_IP_PERMISSION



.. py:function:: create_security_group(security_group_name, vpc_id)


.. py:function:: create_sample_table(instance_name, db_name, table_name)


.. py:function:: create_dms_assets(db_name, instance_name, replication_instance_name, bucket_name, role_arn, source_endpoint_identifier, target_endpoint_identifier, table_definition)


.. py:function:: delete_dms_assets(replication_instance_arn, source_endpoint_arn, target_endpoint_arn, source_endpoint_identifier, target_endpoint_identifier, replication_instance_name)


.. py:function:: delete_security_group(security_group_id, security_group_name)


.. py:data:: test_context



.. py:data:: test_run

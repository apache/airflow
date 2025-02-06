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

:py:mod:`tests.system.providers.amazon.aws.example_ec2`
=======================================================

.. py:module:: tests.system.providers.amazon.aws.example_ec2


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_ec2.get_latest_ami_id
   tests.system.providers.amazon.aws.example_ec2.create_key_pair
   tests.system.providers.amazon.aws.example_ec2.delete_key_pair
   tests.system.providers.amazon.aws.example_ec2.parse_response



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_ec2.DAG_ID
   tests.system.providers.amazon.aws.example_ec2.sys_test_context_task
   tests.system.providers.amazon.aws.example_ec2.test_context
   tests.system.providers.amazon.aws.example_ec2.test_run


.. py:data:: DAG_ID
   :value: 'example_ec2'



.. py:data:: sys_test_context_task



.. py:function:: get_latest_ami_id()

   Returns the AMI ID of the most recently-created Amazon Linux image


.. py:function:: create_key_pair(key_name)


.. py:function:: delete_key_pair(key_pair_id)


.. py:function:: parse_response(instance_ids)


.. py:data:: test_context



.. py:data:: test_run

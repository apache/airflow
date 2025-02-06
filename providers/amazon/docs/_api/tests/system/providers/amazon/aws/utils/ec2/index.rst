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

:py:mod:`tests.system.providers.amazon.aws.utils.ec2`
=====================================================

.. py:module:: tests.system.providers.amazon.aws.utils.ec2


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.utils.ec2.get_default_vpc_id
   tests.system.providers.amazon.aws.utils.ec2.create_address_allocation
   tests.system.providers.amazon.aws.utils.ec2.create_nat_gateway
   tests.system.providers.amazon.aws.utils.ec2.create_route_table
   tests.system.providers.amazon.aws.utils.ec2.create_private_subnets
   tests.system.providers.amazon.aws.utils.ec2.delete_subnets
   tests.system.providers.amazon.aws.utils.ec2.delete_route_table
   tests.system.providers.amazon.aws.utils.ec2.delete_nat_gateway
   tests.system.providers.amazon.aws.utils.ec2.remove_address_allocation



.. py:function:: get_default_vpc_id()

   Returns the VPC ID of the account's default VPC.


.. py:function:: create_address_allocation()

   Allocate a new IP address


.. py:function:: create_nat_gateway(allocation_id, subnet_id)

   Create a NAT gateway


.. py:function:: create_route_table(vpc_id, nat_gateway_id, test_name)

   Create a route table for private subnets.


.. py:function:: create_private_subnets(vpc_id, route_table_id, test_name, number_to_make = 1, cidr_block = None)

   Fargate Profiles require two private subnets in two different availability zones.
   These subnets require as well an egress route to the internet, using a NAT gateway to achieve this.


.. py:function:: delete_subnets(subnets)


.. py:function:: delete_route_table(route_table_id)


.. py:function:: delete_nat_gateway(nat_gateway_id)


.. py:function:: remove_address_allocation(allocation_id)

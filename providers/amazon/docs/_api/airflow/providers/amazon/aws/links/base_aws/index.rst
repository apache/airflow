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

:py:mod:`airflow.providers.amazon.aws.links.base_aws`
=====================================================

.. py:module:: airflow.providers.amazon.aws.links.base_aws


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.links.base_aws.BaseAwsLink




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.links.base_aws.BASE_AWS_CONSOLE_LINK


.. py:data:: BASE_AWS_CONSOLE_LINK
   :value: 'https://console.{aws_domain}'



.. py:class:: BaseAwsLink


   Bases: :py:obj:`airflow.models.BaseOperatorLink`

   Base Helper class for constructing AWS Console Link.

   .. py:attribute:: name
      :type: ClassVar[str]



   .. py:attribute:: key
      :type: ClassVar[str]



   .. py:attribute:: format_str
      :type: ClassVar[str]



   .. py:method:: get_aws_domain(aws_partition)
      :staticmethod:


   .. py:method:: format_link(**kwargs)

      Format AWS Service Link.

      Some AWS Service Link should require additional escaping
      in this case this method should be overridden.


   .. py:method:: get_link(operator, *, ti_key)

      Link to Amazon Web Services Console.

      :param operator: airflow operator
      :param ti_key: TaskInstance ID to return link for
      :return: link to external system


   .. py:method:: persist(context, operator, region_name, aws_partition, **kwargs)
      :classmethod:

      Store link information into XCom.

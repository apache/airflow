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

airflow.providers.cncf.kubernetes.utils.delete_from
===================================================

.. py:module:: airflow.providers.cncf.kubernetes.utils.delete_from


Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.delete_from.DEFAULT_DELETION_BODY


Exceptions
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.delete_from.FailToDeleteError


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict
   airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_yaml


Module Contents
---------------

.. py:data:: DEFAULT_DELETION_BODY

.. py:function:: delete_from_dict(k8s_client, data, body, namespace, verbose=False, **kwargs)

.. py:function:: delete_from_yaml(*, k8s_client, yaml_objects=None, verbose = False, namespace = 'default', body = None, **kwargs)

.. py:exception:: FailToDeleteError(api_exceptions)

   Bases: :py:obj:`Exception`


   For handling error if an error occurred when handling a yaml file during deletion of the resource.


   .. py:attribute:: api_exceptions


   .. py:method:: __str__()

      Return str(self).

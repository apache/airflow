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

airflow.providers.cncf.kubernetes.resource_convert.configmap
============================================================

.. py:module:: airflow.providers.cncf.kubernetes.resource_convert.configmap


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.resource_convert.configmap.convert_configmap
   airflow.providers.cncf.kubernetes.resource_convert.configmap.convert_configmap_to_volume


Module Contents
---------------

.. py:function:: convert_configmap(configmap_name)

   Convert a str into an k8s object.

   :param configmap_name: config map name
   :return:


.. py:function:: convert_configmap_to_volume(configmap_info)

   Convert a dictionary of config_map_name and mount_path into k8s volume mount object and k8s volume.

   :param configmap_info: a dictionary of {config_map_name: mount_path}
   :return:

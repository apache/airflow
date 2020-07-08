# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import tempfile
from typing import Optional, Union

import yaml
from kubernetes import client, config

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


def _load_body_to_dict(body):
    try:
        body_dict = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise AirflowException("Exception when loading resource definition: %s\n" % e)
    return body_dict


class KubernetesHook(BaseHook):
    """
    Creates Kubernetes API connection.

    :param conn_id: the connection to Kubernetes cluster
    :type conn_id: str
    """

    def __init__(
        self,
        conn_id: str = "kubernetes_default"
    ):
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self):
        """
        Returns kubernetes api session for use with requests
        """
        connection = self.get_connection(self.conn_id)
        extras = connection.extra_dejson
        if extras.get("extra__kubernetes__in_cluster"):
            self.log.debug("loading kube_config from: in_cluster configuration")
            config.load_incluster_config()
        elif extras.get("extra__kubernetes__kube_config") is None:
            self.log.debug("loading kube_config from: default file")
            config.load_kube_config()
        else:
            with tempfile.NamedTemporaryFile() as temp_config:
                self.log.debug("loading kube_config from: connection kube_config")
                temp_config.write(extras.get("extra__kubernetes__kube_config").encode())
                temp_config.flush()
                config.load_kube_config(temp_config.name)
        return client.ApiClient()

    def create_custom_resource_definition(self,
                                          group: str,
                                          version: str,
                                          plural: str,
                                          body: Union[str, dict],
                                          namespace: Optional[str] = None
                                          ):
        """
        Creates custom resource definition object in Kubernetes

        :param group: api group
        :type group: str
        :param version: api version
        :type version: str
        :param plural: api plural
        :type plural: str
        :param body: crd object definition
        :type body: Union[str, dict]
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        api = client.CustomObjectsApi(self.get_conn())
        if namespace is None:
            namespace = self.get_namespace()
        if isinstance(body, str):
            body = _load_body_to_dict(body)
        try:
            response = api.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                body=body
            )
            self.log.debug("Response: %s", response)
            return response
        except client.rest.ApiException as e:
            raise AirflowException("Exception when calling -> create_custom_resource_definition: %s\n" % e)

    def get_custom_resource_definition(self,
                                       group: str,
                                       version: str,
                                       plural: str,
                                       name: str,
                                       namespace: Optional[str] = None):
        """
        Get custom resource definition object from Kubernetes

        :param group: api group
        :type group: str
        :param version: api version
        :type version: str
        :param plural: api plural
        :type plural: str
        :param name: crd object name
        :type name: str
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        custom_resource_definition_api = client.CustomObjectsApi(self.get_conn())
        if namespace is None:
            namespace = self.get_namespace()
        try:
            response = custom_resource_definition_api.get_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name
            )
            return response
        except client.rest.ApiException as e:
            raise AirflowException("Exception when calling -> get_custom_resource_definition: %s\n" % e)

    def get_namespace(self):
        """
        Returns the namespace that defined in the connection
        """
        connection = self.get_connection(self.conn_id)
        extras = connection.extra_dejson
        namespace = extras.get("extra__kubernetes__namespace", "default")
        return namespace

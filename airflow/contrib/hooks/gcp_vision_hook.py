# -*- coding: utf-8 -*-
#
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
from copy import deepcopy

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError, RetryError
from google.cloud.vision_v1 import ProductSearchClient
from google.protobuf.json_format import MessageToDict

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class NameDeterminer:
    """
    Class used for checking if the entity has the 'name' attribute set.

    * If so, no action is taken.

    * If not, and the name can be constructed from other parameters provided, it is created and filled in
      the entity.

    * If both the entity's 'name' attribute is set and the name can be constructed from other parameters
      provided:

        * If they are the same - no action is taken

        * if they are different - an exception is thrown.

    """

    def __init__(self, label, id_label, get_path):
        self.label = label
        self.id_label = id_label
        self.get_path = get_path

    def get_entity_with_name(self, entity, entity_id, location, project_id):
        entity = deepcopy(entity)
        explicit_name = getattr(entity, 'name')
        if location and entity_id:
            # Necessary parameters to construct the name are present. Checking for conflict with explicit name
            constructed_name = self.get_path(project_id, location, entity_id)
            if not explicit_name:
                entity.name = constructed_name
                return entity
            elif explicit_name != constructed_name:
                self._raise_ex_different_names(constructed_name, explicit_name)
        else:
            # Not enough parameters to construct the name. Trying to use the name from Product / ProductSet.
            if explicit_name:
                return entity
            else:
                self._raise_ex_unable_to_determine_name()

    def _raise_ex_unable_to_determine_name(self):
        raise AirflowException(
            "Unable to determine the {label} name. Please either set the name directly in the {label} "
            "object or provide the `location` and `{id_label}` parameters.".format(
                label=self.label, id_label=self.id_label
            )
        )

    def _raise_ex_different_names(self, constructed_name, explicit_name):
        raise AirflowException(
            "The {label} name provided in the object ({explicit_name}) is different than the name created "
            "from the input parameters ({constructed_name}). Please either: 1) Remove the {label} name, 2) "
            "Remove the location and {id_label} parameters, 3) Unify the {label} name and input "
            "parameters.".format(
                label=self.label,
                explicit_name=explicit_name,
                constructed_name=constructed_name,
                id_label=self.id_label,
            )
        )


class CloudVisionHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Vision APIs.
    """

    _client = None
    product_name_determiner = NameDeterminer('Product', 'product_id', ProductSearchClient.product_path)
    product_set_name_determiner = NameDeterminer(
        'ProductSet', 'productset_id', ProductSearchClient.product_set_path
    )

    def __init__(self, gcp_conn_id='google_cloud_default', delegate_to=None):
        super(CloudVisionHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Retrieves connection to Cloud Vision.

        :return: Google Cloud Vision client object.
        :rtype: google.cloud.vision_v1.ProductSearchClient
        """
        if not self._client:
            self._client = ProductSearchClient(credentials=self._get_credentials())
        return self._client

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_product_set(
        self,
        location,
        product_set,
        project_id=None,
        product_set_id=None,
        retry=None,
        timeout=None,
        metadata=None,
    ):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetCreateOperator`
        """
        client = self.get_conn()
        parent = ProductSearchClient.location_path(project_id, location)
        self.log.info('Creating a new ProductSet under the parent: %s', parent)
        response = self._handle_request(
            lambda **kwargs: client.create_product_set(**kwargs),
            parent=parent,
            product_set=product_set,
            product_set_id=product_set_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('ProductSet created: %s', response.name if response else '')
        self.log.debug('ProductSet created:\n%s', response)

        if not product_set_id:
            # Product set id was generated by the API
            product_set_id = self._get_autogenerated_id(response)
            self.log.info('Extracted autogenerated ProductSet ID from the response: %s', product_set_id)

        return product_set_id

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_product_set(
        self, location, product_set_id, project_id=None, retry=None, timeout=None, metadata=None
    ):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetGetOperator`
        """
        client = self.get_conn()
        name = ProductSearchClient.product_set_path(project_id, location, product_set_id)
        self.log.info('Retrieving ProductSet: %s', name)
        response = self._handle_request(
            lambda **kwargs: client.get_product_set(**kwargs),
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('ProductSet retrieved.')
        self.log.debug('ProductSet retrieved:\n%s', response)
        return MessageToDict(response)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def update_product_set(
        self,
        product_set,
        location=None,
        product_set_id=None,
        update_mask=None,
        project_id=None,
        retry=None,
        timeout=None,
        metadata=None,
    ):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetUpdateOperator`
        """
        client = self.get_conn()
        product_set = self.product_set_name_determiner.get_entity_with_name(
            product_set, product_set_id, location, project_id
        )
        self.log.info('Updating ProductSet: %s', product_set.name)
        response = self._handle_request(
            lambda **kwargs: client.update_product_set(**kwargs),
            product_set=product_set,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('ProductSet updated: %s', response.name if response else '')
        self.log.debug('ProductSet updated:\n%s', response)
        return MessageToDict(response)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_product_set(
        self, location, product_set_id, project_id=None, retry=None, timeout=None, metadata=None
    ):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductSetDeleteOperator`
        """
        client = self.get_conn()
        name = ProductSearchClient.product_set_path(project_id, location, product_set_id)
        self.log.info('Deleting ProductSet: %s', name)
        response = self._handle_request(
            lambda **kwargs: client.delete_product_set(**kwargs),
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('ProductSet with the name [%s] deleted.', name)
        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_product(
        self, location, product, project_id=None, product_id=None, retry=None, timeout=None, metadata=None
    ):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductCreateOperator`
        """
        client = self.get_conn()
        parent = ProductSearchClient.location_path(project_id, location)
        self.log.info('Creating a new Product under the parent: %s', parent)
        response = self._handle_request(
            lambda **kwargs: client.create_product(**kwargs),
            parent=parent,
            product=product,
            product_id=product_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('Product created: %s', response.name if response else '')
        self.log.debug('Product created:\n%s', response)

        if not product_id:
            # Product id was generated by the API
            product_id = self._get_autogenerated_id(response)
            self.log.info('Extracted autogenerated Product ID from the response: %s', product_id)

        return product_id

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_product(self, location, product_id, project_id=None, retry=None, timeout=None, metadata=None):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductGetOperator`
        """
        client = self.get_conn()
        name = ProductSearchClient.product_path(project_id, location, product_id)
        self.log.info('Retrieving Product: %s', name)
        response = self._handle_request(
            lambda **kwargs: client.get_product(**kwargs),
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('Product retrieved.')
        self.log.debug('Product retrieved:\n%s', response)
        return MessageToDict(response)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def update_product(
        self,
        product,
        location=None,
        product_id=None,
        update_mask=None,
        project_id=None,
        retry=None,
        timeout=None,
        metadata=None,
    ):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductUpdateOperator`
        """
        client = self.get_conn()
        product = self.product_name_determiner.get_entity_with_name(product, product_id, location, project_id)
        self.log.info('Updating ProductSet: %s', product.name)
        response = self._handle_request(
            lambda **kwargs: client.update_product(**kwargs),
            product=product,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('Product updated: %s', response.name if response else '')
        self.log.debug('Product updated:\n%s', response)
        return MessageToDict(response)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_product(self, location, product_id, project_id=None, retry=None, timeout=None, metadata=None):
        """
        For the documentation see:
        :class:`~airflow.contrib.operators.gcp_vision_operator.CloudVisionProductDeleteOperator`
        """
        client = self.get_conn()
        name = ProductSearchClient.product_path(project_id, location, product_id)
        self.log.info('Deleting ProductSet: %s', name)
        response = self._handle_request(
            lambda **kwargs: client.delete_product(**kwargs),
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info('Product with the name [%s] deleted:', name)
        return response

    def _handle_request(self, fun, **kwargs):
        try:
            return fun(**kwargs)
        except GoogleAPICallError as e:
            if isinstance(e, AlreadyExists):
                raise e
            else:
                self.log.error('The request failed:\n%s', str(e))
                raise AirflowException(e)
        except RetryError as e:
            self.log.error('The request failed due to a retryable error and retry attempts failed.')
            raise AirflowException(e)
        except ValueError as e:
            self.log.error('The request failed, the parameters are invalid.')
            raise AirflowException(e)

    @staticmethod
    def _get_entity_name(is_product, project_id, location, entity_id):
        if is_product:
            return ProductSearchClient.product_path(project_id, location, entity_id)
        else:
            return ProductSearchClient.product_set_path(project_id, location, entity_id)

    @staticmethod
    def _get_autogenerated_id(response):
        try:
            name = response.name
        except AttributeError as e:
            raise AirflowException('Unable to get name from response... [{}]\n{}'.format(response, e))
        if '/' not in name:
            raise AirflowException('Unable to get id from name... [{}]'.format(name))
        return name.rsplit('/', 1)[1]

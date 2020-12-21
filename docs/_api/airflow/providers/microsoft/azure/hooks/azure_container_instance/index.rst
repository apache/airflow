:mod:`airflow.providers.microsoft.azure.hooks.azure_container_instance`
=======================================================================

.. py:module:: airflow.providers.microsoft.azure.hooks.azure_container_instance


Module Contents
---------------

.. py:class:: AzureContainerInstanceHook(conn_id: str = 'azure_default')

   Bases: :class:`airflow.providers.microsoft.azure.hooks.base_azure.AzureBaseHook`

   A hook to communicate with Azure Container Instances.

   This hook requires a service principal in order to work.
   After creating this service principal
   (Azure Active Directory/App Registrations), you need to fill in the
   client_id (Application ID) as login, the generated password as password,
   and tenantId and subscriptionId in the extra's field as a json.

   :param conn_id: connection id of a service principal which will be used
       to start the container instance
   :type conn_id: str

   
   .. method:: create_or_update(self, resource_group: str, name: str, container_group: ContainerGroup)

      Create a new container group

      :param resource_group: the name of the resource group
      :type resource_group: str
      :param name: the name of the container group
      :type name: str
      :param container_group: the properties of the container group
      :type container_group: azure.mgmt.containerinstance.models.ContainerGroup



   
   .. method:: get_state_exitcode_details(self, resource_group: str, name: str)

      Get the state and exitcode of a container group

      :param resource_group: the name of the resource group
      :type resource_group: str
      :param name: the name of the container group
      :type name: str
      :return: A tuple with the state, exitcode, and details.
          If the exitcode is unknown 0 is returned.
      :rtype: tuple(state,exitcode,details)



   
   .. method:: get_messages(self, resource_group: str, name: str)

      Get the messages of a container group

      :param resource_group: the name of the resource group
      :type resource_group: str
      :param name: the name of the container group
      :type name: str
      :return: A list of the event messages
      :rtype: list[str]



   
   .. method:: get_state(self, resource_group: str, name: str)

      Get the state of a container group

      :param resource_group: the name of the resource group
      :type resource_group: str
      :param name: the name of the container group
      :type name: str
      :return: ContainerGroup
      :rtype: ~azure.mgmt.containerinstance.models.ContainerGroup



   
   .. method:: get_logs(self, resource_group: str, name: str, tail: int = 1000)

      Get the tail from logs of a container group

      :param resource_group: the name of the resource group
      :type resource_group: str
      :param name: the name of the container group
      :type name: str
      :param tail: the size of the tail
      :type tail: int
      :return: A list of log messages
      :rtype: list[str]



   
   .. method:: delete(self, resource_group: str, name: str)

      Delete a container group

      :param resource_group: the name of the resource group
      :type resource_group: str
      :param name: the name of the container group
      :type name: str



   
   .. method:: exists(self, resource_group: str, name: str)

      Test if a container group exists

      :param resource_group: the name of the resource group
      :type resource_group: str
      :param name: the name of the container group
      :type name: str





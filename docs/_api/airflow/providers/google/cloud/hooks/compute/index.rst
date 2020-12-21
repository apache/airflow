:mod:`airflow.providers.google.cloud.hooks.compute`
===================================================

.. py:module:: airflow.providers.google.cloud.hooks.compute

.. autoapi-nested-parse::

   This module contains a Google Compute Engine Hook.



Module Contents
---------------

.. data:: TIME_TO_SLEEP_IN_SECONDS
   :annotation: = 1

   

.. py:class:: GceOperationStatus

   Class with GCE operations statuses.

   .. attribute:: PENDING
      :annotation: = PENDING

      

   .. attribute:: RUNNING
      :annotation: = RUNNING

      

   .. attribute:: DONE
      :annotation: = DONE

      


.. py:class:: ComputeEngineHook(api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Compute Engine APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   .. attribute:: _conn
      :annotation: :Optional[Any]

      

   
   .. method:: get_conn(self)

      Retrieves connection to Google Compute Engine.

      :return: Google Compute Engine services object
      :rtype: dict



   
   .. method:: start_instance(self, zone: str, resource_id: str, project_id: str)

      Starts an existing instance defined by project_id, zone and resource_id.
      Must be called with keyword arguments rather than positional.

      :param zone: Google Cloud zone where the instance exists
      :type zone: str
      :param resource_id: Name of the Compute Engine instance resource
      :type resource_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: stop_instance(self, zone: str, resource_id: str, project_id: str)

      Stops an instance defined by project_id, zone and resource_id
      Must be called with keyword arguments rather than positional.

      :param zone: Google Cloud zone where the instance exists
      :type zone: str
      :param resource_id: Name of the Compute Engine instance resource
      :type resource_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: set_machine_type(self, zone: str, resource_id: str, body: dict, project_id: str)

      Sets machine type of an instance defined by project_id, zone and resource_id.
      Must be called with keyword arguments rather than positional.

      :param zone: Google Cloud zone where the instance exists.
      :type zone: str
      :param resource_id: Name of the Compute Engine instance resource
      :type resource_id: str
      :param body: Body required by the Compute Engine setMachineType API,
          as described in
          https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType
      :type body: dict
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: _execute_set_machine_type(self, zone: str, resource_id: str, body: dict, project_id: str)



   
   .. method:: get_instance_template(self, resource_id: str, project_id: str)

      Retrieves instance template by project_id and resource_id.
      Must be called with keyword arguments rather than positional.

      :param resource_id: Name of the instance template
      :type resource_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: Instance template representation as object according to
          https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
      :rtype: dict



   
   .. method:: insert_instance_template(self, body: dict, project_id: str, request_id: Optional[str] = None)

      Inserts instance template using body specified
      Must be called with keyword arguments rather than positional.

      :param body: Instance template representation as object according to
          https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
      :type body: dict
      :param request_id: Optional, unique request_id that you might add to achieve
          full idempotence (for example when client call times out repeating the request
          with the same request id will not create a new instance template again)
          It should be in UUID format as defined in RFC 4122
      :type request_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: get_instance_group_manager(self, zone: str, resource_id: str, project_id: str)

      Retrieves Instance Group Manager by project_id, zone and resource_id.
      Must be called with keyword arguments rather than positional.

      :param zone: Google Cloud zone where the Instance Group Manager exists
      :type zone: str
      :param resource_id: Name of the Instance Group Manager
      :type resource_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: Instance group manager representation as object according to
          https://cloud.google.com/compute/docs/reference/rest/beta/instanceGroupManagers
      :rtype: dict



   
   .. method:: patch_instance_group_manager(self, zone: str, resource_id: str, body: dict, project_id: str, request_id: Optional[str] = None)

      Patches Instance Group Manager with the specified body.
      Must be called with keyword arguments rather than positional.

      :param zone: Google Cloud zone where the Instance Group Manager exists
      :type zone: str
      :param resource_id: Name of the Instance Group Manager
      :type resource_id: str
      :param body: Instance Group Manager representation as json-merge-patch object
          according to
          https://cloud.google.com/compute/docs/reference/rest/beta/instanceTemplates/patch
      :type body: dict
      :param request_id: Optional, unique request_id that you might add to achieve
          full idempotence (for example when client call times out repeating the request
          with the same request id will not create a new instance template again).
          It should be in UUID format as defined in RFC 4122
      :type request_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :return: None



   
   .. method:: _wait_for_operation_to_complete(self, project_id: str, operation_name: str, zone: Optional[str] = None)

      Waits for the named operation to complete - checks status of the async call.

      :param operation_name: name of the operation
      :type operation_name: str
      :param zone: optional region of the request (might be None for global operations)
      :type zone: str
      :return: None



   
   .. staticmethod:: _check_zone_operation_status(service: Any, operation_name: str, project_id: str, zone: str, num_retries: int)



   
   .. staticmethod:: _check_global_operation_status(service: Any, operation_name: str, project_id: str, num_retries: int)



   
   .. method:: get_instance_info(self, zone: str, resource_id: str, project_id: str)

      Gets instance information.

      :param zone: Google Cloud zone where the Instance Group Manager exists
      :type zone: str
      :param resource_id: Name of the Instance Group Manager
      :type resource_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str



   
   .. method:: get_instance_address(self, zone: str, resource_id: str, project_id: str, use_internal_ip: bool = False)

      Return network address associated to instance.

      :param zone: Google Cloud zone where the Instance Group Manager exists
      :type zone: str
      :param resource_id: Name of the Instance Group Manager
      :type resource_id: str
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :param use_internal_ip: If true, return private IP address.
      :type use_internal_ip: bool



   
   .. method:: set_instance_metadata(self, zone: str, resource_id: str, metadata: Dict[str, str], project_id: str)

      Set instance metadata.

      :param zone: Google Cloud zone where the Instance Group Manager exists
      :type zone: str
      :param resource_id: Name of the Instance Group Manager
      :type resource_id: str
      :param metadata: The new instance metadata.
      :type metadata: Dict
      :param project_id: Optional, Google Cloud project ID where the
          Compute Engine Instance exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str





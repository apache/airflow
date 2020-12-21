:mod:`airflow.providers.google.cloud.sensors.gcs`
=================================================

.. py:module:: airflow.providers.google.cloud.sensors.gcs

.. autoapi-nested-parse::

   This module contains Google Cloud Storage sensors.



Module Contents
---------------

.. py:class:: GCSObjectExistenceSensor(*, bucket: str, object: str, google_cloud_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a file in Google Cloud Storage.

   :param bucket: The Google Cloud Storage bucket where the object is.
   :type bucket: str
   :param object: The name of the object to check in the Google cloud
       storage bucket.
   :type object: str
   :param google_cloud_conn_id: The connection ID to use when
       connecting to Google Cloud Storage.
   :type google_cloud_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'object', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: dict)




.. function:: ts_function(context)
   Default callback for the GoogleCloudStorageObjectUpdatedSensor. The default
   behaviour is check for the object being updated after execution_date +
   schedule_interval.


.. py:class:: GCSObjectUpdateSensor(bucket: str, object: str, ts_func: Callable = ts_function, google_cloud_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks if an object is updated in Google Cloud Storage.

   :param bucket: The Google Cloud Storage bucket where the object is.
   :type bucket: str
   :param object: The name of the object to download in the Google cloud
       storage bucket.
   :type object: str
   :param ts_func: Callback for defining the update condition. The default callback
       returns execution_date + schedule_interval. The callback takes the context
       as parameter.
   :type ts_func: function
   :param google_cloud_conn_id: The connection ID to use when
       connecting to Google Cloud Storage.
   :type google_cloud_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'object', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: dict)




.. py:class:: GCSObjectsWtihPrefixExistenceSensor(bucket: str, prefix: str, google_cloud_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of GCS objects at a given prefix, passing matches via XCom.

   When files matching the given prefix are found, the poke method's criteria will be
   fulfilled and the matching objects will be returned from the operator and passed
   through XCom for downstream tasks.

   :param bucket: The Google Cloud Storage bucket where the object is.
   :type bucket: str
   :param prefix: The name of the prefix to check in the Google cloud
       storage bucket.
   :type prefix: str
   :param google_cloud_conn_id: The connection ID to use when
       connecting to Google Cloud Storage.
   :type google_cloud_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'prefix', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: poke(self, context: dict)



   
   .. method:: execute(self, context: dict)

      Overridden to allow matches to be passed




.. function:: get_time()
   This is just a wrapper of datetime.datetime.now to simplify mocking in the
   unittests.


.. py:class:: GCSUploadSessionCompleteSensor(bucket: str, prefix: str, inactivity_period: float = 60 * 60, min_objects: int = 1, previous_objects: Optional[Set[str]] = None, allow_delete: bool = True, google_cloud_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for changes in the number of objects at prefix in Google Cloud Storage
   bucket and returns True if the inactivity period has passed with no
   increase in the number of objects. Note, this sensor will no behave correctly
   in reschedule mode, as the state of the listed objects in the GCS bucket will
   be lost between rescheduled invocations.

   :param bucket: The Google Cloud Storage bucket where the objects are.
       expected.
   :type bucket: str
   :param prefix: The name of the prefix to check in the Google cloud
       storage bucket.
   :param inactivity_period: The total seconds of inactivity to designate
       an upload session is over. Note, this mechanism is not real time and
       this operator may not return until a poke_interval after this period
       has passed with no additional objects sensed.
   :type inactivity_period: float
   :param min_objects: The minimum number of objects needed for upload session
       to be considered valid.
   :type min_objects: int
   :param previous_objects: The set of object ids found during the last poke.
   :type previous_objects: set[str]
   :param allow_delete: Should this sensor consider objects being deleted
       between pokes valid behavior. If true a warning message will be logged
       when this happens. If false an error will be raised.
   :type allow_delete: bool
   :param google_cloud_conn_id: The connection ID to use when connecting
       to Google Cloud Storage.
   :type google_cloud_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'prefix', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: _get_gcs_hook(self)



   
   .. method:: is_bucket_updated(self, current_objects: Set[str])

      Checks whether new objects have been uploaded and the inactivity_period
      has passed and updates the state of the sensor accordingly.

      :param current_objects: set of object ids in bucket during last poke.
      :type current_objects: set[str]



   
   .. method:: poke(self, context: dict)





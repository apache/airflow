:mod:`airflow.providers.amazon.aws.sensors.s3_keys_unchanged`
=============================================================

.. py:module:: airflow.providers.amazon.aws.sensors.s3_keys_unchanged


Module Contents
---------------

.. py:class:: S3KeysUnchangedSensor(*, bucket_name: str, prefix: str, aws_conn_id: str = 'aws_default', verify: Optional[Union[bool, str]] = None, inactivity_period: float = 60 * 60, min_objects: int = 1, previous_objects: Optional[Set[str]] = None, allow_delete: bool = True, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for changes in the number of objects at prefix in AWS S3
   bucket and returns True if the inactivity period has passed with no
   increase in the number of objects. Note, this sensor will not behave correctly
   in reschedule mode, as the state of the listed objects in the S3 bucket will
   be lost between rescheduled invocations.

   :param bucket_name: Name of the S3 bucket
   :type bucket_name: str
   :param prefix: The prefix being waited on. Relative path from bucket root level.
   :type prefix: str
   :param aws_conn_id: a reference to the s3 connection
   :type aws_conn_id: str
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :type verify: Optional[Union[bool, str]]
   :param inactivity_period: The total seconds of inactivity to designate
       keys unchanged. Note, this mechanism is not real time and
       this operator may not return until a poke_interval after this period
       has passed with no additional objects sensed.
   :type inactivity_period: float
   :param min_objects: The minimum number of objects needed for keys unchanged
       sensor to be considered valid.
   :type min_objects: int
   :param previous_objects: The set of object ids found during the last poke.
   :type previous_objects: Optional[Set[str]]
   :param allow_delete: Should this sensor consider objects being deleted
       between pokes valid behavior. If true a warning message will be logged
       when this happens. If false an error will be raised.
   :type allow_delete: bool

   .. attribute:: template_fields
      :annotation: = ['bucket_name', 'prefix']

      

   
   .. method:: hook(self)

      Returns S3Hook.



   
   .. method:: is_keys_unchanged(self, current_objects: Set[str])

      Checks whether new objects have been uploaded and the inactivity_period
      has passed and updates the state of the sensor accordingly.

      :param current_objects: set of object ids in bucket during last poke.
      :type current_objects: set[str]



   
   .. method:: poke(self, context)





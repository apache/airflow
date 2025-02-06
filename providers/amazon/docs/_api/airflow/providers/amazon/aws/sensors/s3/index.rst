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

:py:mod:`airflow.providers.amazon.aws.sensors.s3`
=================================================

.. py:module:: airflow.providers.amazon.aws.sensors.s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.s3.S3KeySensor
   airflow.providers.amazon.aws.sensors.s3.S3KeysUnchangedSensor




.. py:class:: S3KeySensor(*, bucket_key, bucket_name = None, wildcard_match = False, check_fn = None, aws_conn_id = 'aws_default', verify = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Waits for one or multiple keys (a file-like instance on S3) to be present in a S3 bucket.

   The path is just a key/value pointer to a resource for the given S3 path.
   Note: S3 does not support folders directly, and only provides key/value pairs.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:S3KeySensor`

   :param bucket_key: The key(s) being waited on. Supports full s3:// style url
       or relative path from root level. When it's specified as a full s3://
       url, please leave bucket_name as `None`
   :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
       is not provided as a full ``s3://`` url. When specified, all the keys passed to ``bucket_key``
       refers to this bucket
   :param wildcard_match: whether the bucket_key should be interpreted as a
       Unix wildcard pattern
   :param check_fn: Function that receives the list of the S3 objects,
       and returns a boolean:
       - ``True``: the criteria is met
       - ``False``: the criteria isn't met
       **Example**: Wait for any S3 object size more than 1 megabyte  ::

           def check_fn(files: List) -> bool:
               return any(f.get('Size', 0) > 1048576 for f in files)
   :param aws_conn_id: a reference to the s3 connection
   :param deferrable: Run operator in the deferrable mode
   :param verify: Whether to verify SSL certificates for S3 connection.
       By default, SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket_key', 'bucket_name')



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: execute(context)

      Airflow runs this method on the worker and defers using the trigger.


   .. py:method:: execute_complete(context, event)

      Execute when the trigger fires - returns immediately.

      Relies on trigger to throw an exception, otherwise it assumes execution was successful.


   .. py:method:: get_hook()

      Create and return an S3Hook.


   .. py:method:: hook()



.. py:class:: S3KeysUnchangedSensor(*, bucket_name, prefix, aws_conn_id = 'aws_default', verify = None, inactivity_period = 60 * 60, min_objects = 1, previous_objects = None, allow_delete = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Return True if inactivity_period has passed with no increase in the number of objects matching prefix.

   Note, this sensor will not behave correctly in reschedule mode, as the state of the listed
   objects in the S3 bucket will be lost between rescheduled invocations.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:S3KeysUnchangedSensor`

   :param bucket_name: Name of the S3 bucket
   :param prefix: The prefix being waited on. Relative path from bucket root level.
   :param aws_conn_id: a reference to the s3 connection
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :param inactivity_period: The total seconds of inactivity to designate
       keys unchanged. Note, this mechanism is not real time and
       this operator may not return until a poke_interval after this period
       has passed with no additional objects sensed.
   :param min_objects: The minimum number of objects needed for keys unchanged
       sensor to be considered valid.
   :param previous_objects: The set of object ids found during the last poke.
   :param allow_delete: Should this sensor consider objects being deleted
       between pokes valid behavior. If true a warning message will be logged
       when this happens. If false an error will be raised.
   :param deferrable: Run sensor in the deferrable mode

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('bucket_name', 'prefix')



   .. py:method:: hook()

      Returns S3Hook.


   .. py:method:: is_keys_unchanged(current_objects)

      Check for new objects after the inactivity_period and update the sensor state accordingly.

      :param current_objects: set of object ids in bucket during last poke.


   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: execute(context)

      Airflow runs this method on the worker and defers using the trigger if deferrable is True.


   .. py:method:: execute_complete(context, event = None)

      Execute when the trigger fires - returns immediately.

      Relies on trigger to throw an exception, otherwise it assumes execution was successful.

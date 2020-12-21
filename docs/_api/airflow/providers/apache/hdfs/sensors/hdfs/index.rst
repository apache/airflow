:mod:`airflow.providers.apache.hdfs.sensors.hdfs`
=================================================

.. py:module:: airflow.providers.apache.hdfs.sensors.hdfs


Module Contents
---------------

.. data:: log
   

   

.. py:class:: HdfsSensor(*, filepath: str, hdfs_conn_id: str = 'hdfs_default', ignored_ext: Optional[List[str]] = None, ignore_copying: bool = True, file_size: Optional[int] = None, hook: Type[HDFSHook] = HDFSHook, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a file or folder to land in HDFS

   .. attribute:: template_fields
      :annotation: = ['filepath']

      

   .. attribute:: ui_color
      

      

   
   .. staticmethod:: filter_for_filesize(result: List[Dict[Any, Any]], size: Optional[int] = None)

      Will test the filepath result and test if its size is at least self.filesize

      :param result: a list of dicts returned by Snakebite ls
      :param size: the file size in MB a file should be at least to trigger True
      :return: (bool) depending on the matching criteria



   
   .. staticmethod:: filter_for_ignored_ext(result: List[Dict[Any, Any]], ignored_ext: List[str], ignore_copying: bool)

      Will filter if instructed to do so the result to remove matching criteria

      :param result: list of dicts returned by Snakebite ls
      :type result: list[dict]
      :param ignored_ext: list of ignored extensions
      :type ignored_ext: list
      :param ignore_copying: shall we ignore ?
      :type ignore_copying: bool
      :return: list of dicts which were not removed
      :rtype: list[dict]



   
   .. method:: poke(self, context: Dict[Any, Any])

      Get a snakebite client connection and check for file.




.. py:class:: HdfsRegexSensor(regex: Pattern[str], *args, **kwargs)

   Bases: :class:`airflow.providers.apache.hdfs.sensors.hdfs.HdfsSensor`

   Waits for matching files by matching on regex

   
   .. method:: poke(self, context: Dict[Any, Any])

      Poke matching files in a directory with self.regex

      :return: Bool depending on the search criteria




.. py:class:: HdfsFolderSensor(be_empty: bool = False, *args, **kwargs)

   Bases: :class:`airflow.providers.apache.hdfs.sensors.hdfs.HdfsSensor`

   Waits for a non-empty directory

   
   .. method:: poke(self, context: Dict[str, Any])

      Poke for a non empty directory

      :return: Bool depending on the search criteria





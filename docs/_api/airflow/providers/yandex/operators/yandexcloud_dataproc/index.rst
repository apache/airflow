:mod:`airflow.providers.yandex.operators.yandexcloud_dataproc`
==============================================================

.. py:module:: airflow.providers.yandex.operators.yandexcloud_dataproc


Module Contents
---------------

.. py:class:: DataprocCreateClusterOperator(*, folder_id: Optional[str] = None, cluster_name: Optional[str] = None, cluster_description: str = '', cluster_image_version: str = '1.1', ssh_public_keys: Optional[Union[str, Iterable[str]]] = None, subnet_id: Optional[str] = None, services: Iterable[str] = ('HDFS', 'YARN', 'MAPREDUCE', 'HIVE', 'SPARK'), s3_bucket: Optional[str] = None, zone: str = 'ru-central1-b', service_account_id: Optional[str] = None, masternode_resource_preset: str = 's2.small', masternode_disk_size: int = 15, masternode_disk_type: str = 'network-ssd', datanode_resource_preset: str = 's2.small', datanode_disk_size: int = 15, datanode_disk_type: str = 'network-ssd', datanode_count: int = 2, computenode_resource_preset: str = 's2.small', computenode_disk_size: int = 15, computenode_disk_type: str = 'network-ssd', computenode_count: int = 0, connection_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates Yandex.Cloud Data Proc cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:DataprocCreateClusterOperator`

   :param folder_id: ID of the folder in which cluster should be created.
   :type folder_id: Optional[str]
   :param cluster_name: Cluster name. Must be unique inside the folder.
   :type cluster_name: Optional[str]
   :param cluster_description: Cluster description.
   :type cluster_description: str
   :param cluster_image_version: Cluster image version. Use default.
   :type cluster_image_version: str
   :param ssh_public_keys: List of SSH public keys that will be deployed to created compute instances.
   :type ssh_public_keys: Optional[Union[str, Iterable[str]]]
   :param subnet_id: ID of the subnetwork. All Data Proc cluster nodes will use one subnetwork.
   :type subnet_id: str
   :param services: List of services that will be installed to the cluster. Possible options:
       HDFS, YARN, MAPREDUCE, HIVE, TEZ, ZOOKEEPER, HBASE, SQOOP, FLUME, SPARK, SPARK, ZEPPELIN, OOZIE
   :type services: Iterable[str]
   :param s3_bucket: Yandex.Cloud S3 bucket to store cluster logs.
                     Jobs will not work if the bucket is not specified.
   :type s3_bucket: Optional[str]
   :param zone: Availability zone to create cluster in.
                Currently there are ru-central1-a, ru-central1-b and ru-central1-c.
   :type zone: str
   :param service_account_id: Service account id for the cluster.
                              Service account can be created inside the folder.
   :type service_account_id: Optional[str]
   :param masternode_resource_preset: Resources preset (CPU+RAM configuration)
                                      for the master node of the cluster.
   :type masternode_resource_preset: str
   :param masternode_disk_size: Masternode storage size in GiB.
   :type masternode_disk_size: int
   :param masternode_disk_type: Masternode storage type. Possible options: network-ssd, network-hdd.
   :type masternode_disk_type: str
   :param datanode_resource_preset: Resources preset (CPU+RAM configuration)
                                    for the data nodes of the cluster.
   :type datanode_resource_preset: str
   :param datanode_disk_size: Datanodes storage size in GiB.
   :type datanode_disk_size: int
   :param datanode_disk_type: Datanodes storage type. Possible options: network-ssd, network-hdd.
   :type datanode_disk_type: str
   :param computenode_resource_preset: Resources preset (CPU+RAM configuration)
                                       for the compute nodes of the cluster.
   :type computenode_resource_preset: str
   :param computenode_disk_size: Computenodes storage size in GiB.
   :type computenode_disk_size: int
   :param computenode_disk_type: Computenodes storage type. Possible options: network-ssd, network-hdd.
   :type computenode_disk_type: str
   :param connection_id: ID of the Yandex.Cloud Airflow connection.
   :type connection_id: Optional[str]

   
   .. method:: execute(self, context)




.. py:class:: DataprocDeleteClusterOperator(*, connection_id: Optional[str] = None, cluster_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes Yandex.Cloud Data Proc cluster.

   :param connection_id: ID of the Yandex.Cloud Airflow connection.
   :type cluster_id: Optional[str]
   :param cluster_id: ID of the cluster to remove. (templated)
   :type cluster_id: Optional[str]

   .. attribute:: template_fields
      :annotation: = ['cluster_id']

      

   
   .. method:: execute(self, context)




.. py:class:: DataprocCreateHiveJobOperator(*, query: Optional[str] = None, query_file_uri: Optional[str] = None, script_variables: Optional[Dict[str, str]] = None, continue_on_failure: bool = False, properties: Optional[Dict[str, str]] = None, name: str = 'Hive job', cluster_id: Optional[str] = None, connection_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs Hive job in Data Proc cluster.

   :param query: Hive query.
   :type query: Optional[str]
   :param query_file_uri: URI of the script that contains Hive queries. Can be placed in HDFS or S3.
   :type query_file_uri: Optional[str]
   :param properties: A mapping of property names to values, used to configure Hive.
   :type properties: Optional[Dist[str, str]]
   :param script_variables: Mapping of query variable names to values.
   :type script_variables: Optional[Dist[str, str]]
   :param continue_on_failure: Whether to continue executing queries if a query fails.
   :type continue_on_failure: bool
   :param name: Name of the job. Used for labeling.
   :type name: str
   :param cluster_id: ID of the cluster to run job in.
                      Will try to take the ID from Dataproc Hook object if ot specified. (templated)
   :type cluster_id: Optional[str]
   :param connection_id: ID of the Yandex.Cloud Airflow connection.
   :type connection_id: Optional[str]

   .. attribute:: template_fields
      :annotation: = ['cluster_id']

      

   
   .. method:: execute(self, context)




.. py:class:: DataprocCreateMapReduceJobOperator(*, main_class: Optional[str] = None, main_jar_file_uri: Optional[str] = None, jar_file_uris: Optional[Iterable[str]] = None, archive_uris: Optional[Iterable[str]] = None, file_uris: Optional[Iterable[str]] = None, args: Optional[Iterable[str]] = None, properties: Optional[Dict[str, str]] = None, name: str = 'Mapreduce job', cluster_id: Optional[str] = None, connection_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs Mapreduce job in Data Proc cluster.

   :param main_jar_file_uri: URI of jar file with job.
                             Can be placed in HDFS or S3. Can be specified instead of main_class.
   :type main_class: Optional[str]
   :param main_class: Name of the main class of the job. Can be specified instead of main_jar_file_uri.
   :type main_class: Optional[str]
   :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
   :type file_uris: Optional[Iterable[str]]
   :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
   :type archive_uris: Optional[Iterable[str]]
   :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
   :type archive_uris: Optional[Iterable[str]]
   :param properties: Properties for the job.
   :type properties: Optional[Dist[str, str]]
   :param args: Arguments to be passed to the job.
   :type args: Optional[Iterable[str]]
   :param name: Name of the job. Used for labeling.
   :type name: str
   :param cluster_id: ID of the cluster to run job in.
                      Will try to take the ID from Dataproc Hook object if ot specified. (templated)
   :type cluster_id: Optional[str]
   :param connection_id: ID of the Yandex.Cloud Airflow connection.
   :type connection_id: Optional[str]

   .. attribute:: template_fields
      :annotation: = ['cluster_id']

      

   
   .. method:: execute(self, context)




.. py:class:: DataprocCreateSparkJobOperator(*, main_class: Optional[str] = None, main_jar_file_uri: Optional[str] = None, jar_file_uris: Optional[Iterable[str]] = None, archive_uris: Optional[Iterable[str]] = None, file_uris: Optional[Iterable[str]] = None, args: Optional[Iterable[str]] = None, properties: Optional[Dict[str, str]] = None, name: str = 'Spark job', cluster_id: Optional[str] = None, connection_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs Spark job in Data Proc cluster.

   :param main_jar_file_uri: URI of jar file with job. Can be placed in HDFS or S3.
   :type main_class: Optional[str]
   :param main_class: Name of the main class of the job.
   :type main_class: Optional[str]
   :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
   :type file_uris: Optional[Iterable[str]]
   :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
   :type archive_uris: Optional[Iterable[str]]
   :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
   :type archive_uris: Optional[Iterable[str]]
   :param properties: Properties for the job.
   :type properties: Optional[Dist[str, str]]
   :param args: Arguments to be passed to the job.
   :type args: Optional[Iterable[str]]
   :param name: Name of the job. Used for labeling.
   :type name: str
   :param cluster_id: ID of the cluster to run job in.
                      Will try to take the ID from Dataproc Hook object if ot specified. (templated)
   :type cluster_id: Optional[str]
   :param connection_id: ID of the Yandex.Cloud Airflow connection.
   :type connection_id: Optional[str]

   .. attribute:: template_fields
      :annotation: = ['cluster_id']

      

   
   .. method:: execute(self, context)




.. py:class:: DataprocCreatePysparkJobOperator(*, main_python_file_uri: Optional[str] = None, python_file_uris: Optional[Iterable[str]] = None, jar_file_uris: Optional[Iterable[str]] = None, archive_uris: Optional[Iterable[str]] = None, file_uris: Optional[Iterable[str]] = None, args: Optional[Iterable[str]] = None, properties: Optional[Dict[str, str]] = None, name: str = 'Pyspark job', cluster_id: Optional[str] = None, connection_id: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs Pyspark job in Data Proc cluster.

   :param main_python_file_uri: URI of python file with job. Can be placed in HDFS or S3.
   :type main_python_file_uri: Optional[str]
   :param python_file_uris: URIs of python files used in the job. Can be placed in HDFS or S3.
   :type python_file_uris: Optional[Iterable[str]]
   :param file_uris: URIs of files used in the job. Can be placed in HDFS or S3.
   :type file_uris: Optional[Iterable[str]]
   :param archive_uris: URIs of archive files used in the job. Can be placed in HDFS or S3.
   :type archive_uris: Optional[Iterable[str]]
   :param jar_file_uris: URIs of JAR files used in the job. Can be placed in HDFS or S3.
   :type archive_uris: Optional[Iterable[str]]
   :param properties: Properties for the job.
   :type properties: Optional[Dist[str, str]]
   :param args: Arguments to be passed to the job.
   :type args: Optional[Iterable[str]]
   :param name: Name of the job. Used for labeling.
   :type name: str
   :param cluster_id: ID of the cluster to run job in.
                      Will try to take the ID from Dataproc Hook object if ot specified. (templated)
   :type cluster_id: Optional[str]
   :param connection_id: ID of the Yandex.Cloud Airflow connection.
   :type connection_id: Optional[str]

   .. attribute:: template_fields
      :annotation: = ['cluster_id']

      

   
   .. method:: execute(self, context)





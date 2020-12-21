:mod:`airflow.www.forms`
========================

.. py:module:: airflow.www.forms


Module Contents
---------------

.. py:class:: DateTimeWithTimezoneField(label=None, validators=None, datetime_format='%Y-%m-%d %H:%M:%S%Z', **kwargs)

   Bases: :class:`wtforms.fields.Field`

   A text field which stores a `datetime.datetime` matching a format.

   .. attribute:: widget
      

      

   
   .. method:: _value(self)



   
   .. method:: process_formdata(self, valuelist)



   
   .. method:: _get_default_timezone(self)




.. py:class:: DateTimeForm

   Bases: :class:`flask_wtf.FlaskForm`

   Date filter form needed for task views

   .. attribute:: execution_date
      

      


.. py:class:: DateTimeWithNumRunsForm

   Bases: :class:`flask_wtf.FlaskForm`

   Date time and number of runs form for tree view, task duration
   and landing times

   .. attribute:: base_date
      

      

   .. attribute:: num_runs
      

      


.. py:class:: DateTimeWithNumRunsWithDagRunsForm

   Bases: :class:`airflow.www.forms.DateTimeWithNumRunsForm`

   Date time and number of runs and dag runs form for graph and gantt view

   .. attribute:: execution_date
      

      


.. py:class:: DagRunForm

   Bases: :class:`flask_appbuilder.forms.DynamicForm`

   Form for editing and adding DAG Run

   .. attribute:: dag_id
      

      

   .. attribute:: start_date
      

      

   .. attribute:: end_date
      

      

   .. attribute:: run_id
      

      

   .. attribute:: state
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: external_trigger
      

      

   .. attribute:: conf
      

      

   
   .. method:: populate_obj(self, item)

      Populates the attributes of the passed obj with data from the form’s fields.




.. data:: _connection_types
   :annotation: = [['docker', 'Docker Registry'], ['elasticsearch', 'Elasticsearch'], ['exasol', 'Exasol'], ['facebook_social', 'Facebook Social'], ['fs', 'File (path)'], ['ftp', 'FTP'], ['google_cloud_platform', 'Google Cloud'], ['hdfs', 'HDFS'], ['http', 'HTTP'], ['pig_cli', 'Pig Client Wrapper'], ['hive_cli', 'Hive Client Wrapper'], ['hive_metastore', 'Hive Metastore Thrift'], ['hiveserver2', 'Hive Server 2 Thrift'], ['jdbc', 'JDBC Connection'], ['odbc', 'ODBC Connection'], ['jenkins', 'Jenkins'], ['mysql', 'MySQL'], ['postgres', 'Postgres'], ['oracle', 'Oracle'], ['vertica', 'Vertica'], ['presto', 'Presto'], ['s3', 'S3'], ['samba', 'Samba'], ['sqlite', 'Sqlite'], ['ssh', 'SSH'], ['cloudant', 'IBM Cloudant'], ['mssql', 'Microsoft SQL Server'], ['mesos_framework-id', 'Mesos Framework ID'], ['jira', 'JIRA'], ['redis', 'Redis'], ['wasb', 'Azure Blob Storage'], ['databricks', 'Databricks'], ['aws', 'Amazon Web Services'], ['emr', 'Elastic MapReduce'], ['snowflake', 'Snowflake'], ['segment', 'Segment'], ['sqoop', 'Sqoop'], ['azure_batch', 'Azure Batch Service'], ['azure_data_lake', 'Azure Data Lake'], ['azure_container_instances', 'Azure Container Instances'], ['azure_cosmos', 'Azure CosmosDB'], ['azure_data_explorer', 'Azure Data Explorer'], ['cassandra', 'Cassandra'], ['qubole', 'Qubole'], ['mongo', 'MongoDB'], ['gcpcloudsql', 'Google Cloud SQL'], ['grpc', 'GRPC Connection'], ['yandexcloud', 'Yandex Cloud'], ['livy', 'Apache Livy'], ['tableau', 'Tableau'], ['kubernetes', 'Kubernetes Cluster Connection'], ['spark', 'Spark'], ['imap', 'IMAP'], ['vault', 'Hashicorp Vault'], ['azure', 'Azure']]

   

.. py:class:: ConnectionForm

   Bases: :class:`flask_appbuilder.forms.DynamicForm`

   Form for editing and adding Connection

   .. attribute:: conn_id
      

      

   .. attribute:: conn_type
      

      

   .. attribute:: host
      

      

   .. attribute:: schema
      

      

   .. attribute:: login
      

      

   .. attribute:: password
      

      

   .. attribute:: port
      

      

   .. attribute:: extra
      

      

   .. attribute:: extra__jdbc__drv_path
      

      

   .. attribute:: extra__jdbc__drv_clsname
      

      

   .. attribute:: extra__google_cloud_platform__project
      

      

   .. attribute:: extra__google_cloud_platform__key_path
      

      

   .. attribute:: extra__google_cloud_platform__keyfile_dict
      

      

   .. attribute:: extra__google_cloud_platform__scope
      

      

   .. attribute:: extra__google_cloud_platform__num_retries
      

      

   .. attribute:: extra__grpc__auth_type
      

      

   .. attribute:: extra__grpc__credential_pem_file
      

      

   .. attribute:: extra__grpc__scopes
      

      

   .. attribute:: extra__yandexcloud__service_account_json
      

      

   .. attribute:: extra__yandexcloud__service_account_json_path
      

      

   .. attribute:: extra__yandexcloud__oauth
      

      

   .. attribute:: extra__yandexcloud__folder_id
      

      

   .. attribute:: extra__yandexcloud__public_ssh_key
      

      

   .. attribute:: extra__kubernetes__in_cluster
      

      

   .. attribute:: extra__kubernetes__kube_config_path
      

      

   .. attribute:: extra__kubernetes__kube_config
      

      

   .. attribute:: extra__kubernetes__namespace
      

      



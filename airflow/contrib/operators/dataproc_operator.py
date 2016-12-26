# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataProcPigOperator(BaseOperator):
    """
    Start a Pig query Job on a Cloud DataProc cluster. The parameters of the operation
    will be passed to the cluster.

    It's a good practice to define dataproc_* parameters in the default_args of the dag
    like the cluster name and UDFs.

    ```
    default_args = {
        'dataproc_cluster': 'cluster-1',
        'dataproc_pig_jars': [
            'gs://example/udf/jar/datafu/1.2.0/datafu.jar',
            'gs://example/udf/jar/gpig/1.2/gpig.jar'
        ]
    }
    ```

    You can pass a pig script as string or file reference. Use variables to pass on
    variables for the pig script to be resolved on the cluster or use the parameters to
    be resolved in the script as template parameters.

    ```
    t1 = DataProcPigOperator(
        task_id='dataproc_pig',
        query='a_pig_script.pig',
        variables={'out': 'gs://example/output/{{ds}}'},
    dag=dag)
    ```
    """
    template_fields = ['query', 'variables', 'job_name', 'dataproc_cluster']
    template_ext = ('.pg', '.pig',)
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            query,
            variables=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            dataproc_cluster='cluster-1',
            dataproc_pig_properties=None,
            dataproc_pig_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        Create a new DataProcPigOperator.

        For more detail on about job submission have a look at the reference:

        https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs

        :param query: The query or reference to the query file (pg or pig extension).
        :type query: string
        :param variables: Map of named parameters for the query.
        :type variables: dict
        :param job_name: The job name used in the DataProc cluster. This name by default
            is the task_id appended with the execution data, but can be templated. The
            name will always be appended with a random number to avoid name clashes.
        :type job_name: string
        :param dataproc_cluster: The id of the DataProc cluster.
        :type dataproc_cluster: string
        :param dataproc_pig_properties: Map for the Pig properties. Ideal to put in
            default arguments
        :type dataproc_pig_properties: dict
        :param dataproc_pig_jars: URIs to jars provisioned in Cloud Storage (example: for
            UDFs and libs) and are ideal to put in default arguments.
        :type dataproc_pig_jars: list
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        """
        super(DataProcPigOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.query = query
        self.variables = variables
        self.job_name = job_name
        self.dataproc_cluster = dataproc_cluster
        self.dataproc_properties = dataproc_pig_properties
        self.dataproc_jars = dataproc_pig_jars

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.dataproc_cluster, "pigJob",
                                       self.dataproc_properties)

        job.add_query(self.query)
        job.add_variables(self.variables)
        job.add_jar_file_uris(self.dataproc_jars)
        job.set_job_name(self.job_name)

        hook.submit(hook.project_id, job.build())


class DataProcHiveOperator(BaseOperator):
    """
    Start a Hive query Job on a Cloud DataProc cluster.
    """
    template_fields = ['query', 'variables', 'job_name', 'dataproc_cluster']
    template_ext = ('.q',)
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            query,
            variables=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            dataproc_cluster='cluster-1',
            dataproc_hive_properties=None,
            dataproc_hive_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        Create a new DataProcHiveOperator.

        :param query: The query or reference to the query file (q extension).
        :type query: string
        :param variables: Map of named parameters for the query.
        :type variables: dict
        :param job_name: The job name used in the DataProc cluster. This name by default
            is the task_id appended with the execution data, but can be templated. The
            name will always be appended with a random number to avoid name clashes.
        :type job_name: string
        :param dataproc_cluster: The id of the DataProc cluster.
        :type dataproc_cluster: string
        :param dataproc_hive_properties: Map for the Pig properties. Ideal to put in
            default arguments
        :type dataproc_hive_properties: dict
        :param dataproc_hive_jars: URIs to jars provisioned in Cloud Storage (example: for
            UDFs and libs) and are ideal to put in default arguments.
        :type dataproc_hive_jars: list
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        """
        super(DataProcHiveOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.query = query
        self.variables = variables
        self.job_name = job_name
        self.dataproc_cluster = dataproc_cluster
        self.dataproc_properties = dataproc_hive_properties
        self.dataproc_jars = dataproc_hive_jars

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)

        job = hook.create_job_template(self.task_id, self.dataproc_cluster, "hiveJob",
                                       self.dataproc_properties)

        job.add_query(self.query)
        job.add_variables(self.variables)
        job.add_jar_file_uris(self.dataproc_jars)
        job.set_job_name(self.job_name)

        hook.submit(hook.project_id, job.build())


class DataProcSparkSqlOperator(BaseOperator):
    """
    Start a Spark SQL query Job on a Cloud DataProc cluster.
    """
    template_fields = ['query', 'variables', 'job_name', 'dataproc_cluster']
    template_ext = ('.q',)
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            query,
            variables=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            dataproc_cluster='cluster-1',
            dataproc_spark_properties=None,
            dataproc_spark_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        Create a new DataProcSparkSqlOperator.

        :param query: The query or reference to the query file (q extension).
        :type query: string
        :param variables: Map of named parameters for the query.
        :type variables: dict
        :param job_name: The job name used in the DataProc cluster. This name by default
            is the task_id appended with the execution data, but can be templated. The
            name will always be appended with a random number to avoid name clashes.
        :type job_name: string
        :param dataproc_cluster: The id of the DataProc cluster.
        :type dataproc_cluster: string
        :param dataproc_spark_properties: Map for the Pig properties. Ideal to put in
            default arguments
        :type dataproc_spark_properties: dict
        :param dataproc_spark_jars: URIs to jars provisioned in Cloud Storage (example:
            for UDFs and libs) and are ideal to put in default arguments.
        :type dataproc_spark_jars: list
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        """
        super(DataProcSparkSqlOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.query = query
        self.variables = variables
        self.job_name = job_name
        self.dataproc_cluster = dataproc_cluster
        self.dataproc_properties = dataproc_spark_properties
        self.dataproc_jars = dataproc_spark_jars

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)

        job = hook.create_job_template(self.task_id, self.dataproc_cluster, "sparkSqlJob",
                                       self.dataproc_properties)

        job.add_query(self.query)
        job.add_variables(self.variables)
        job.add_jar_file_uris(self.dataproc_jars)
        job.set_job_name(self.job_name)

        hook.submit(hook.project_id, job.build())


class DataProcSparkOperator(BaseOperator):
    """
    Start a Spark Job on a Cloud DataProc cluster.
    """

    template_fields = ['arguments', 'job_name', 'dataproc_cluster']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            main_jar=None,
            main_class=None,
            arguments=None,
            archives=None,
            files=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            dataproc_cluster='cluster-1',
            dataproc_spark_properties=None,
            dataproc_spark_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        Create a new DataProcSparkOperator.

        :param main_jar: URI of the job jar provisioned on Cloud Storage. (use this or
            the main_class, not both together).
        :type main_jar: string
        :param main_class: Name of the job class. (use this or the main_jar, not both
            together).
        :type main_class: string
        :param arguments: Arguments for the job.
        :type arguments: list
        :param archives: List of archived files that will be unpacked in the work
            directory. Should be stored in Cloud Storage.
        :type archives: list
        :param files: List of files to be copied to the working directory
        :type files: list
        :param job_name: The job name used in the DataProc cluster. This name by default
            is the task_id appended with the execution data, but can be templated. The
            name will always be appended with a random number to avoid name clashes.
        :type job_name: string
        :param dataproc_cluster: The id of the DataProc cluster.
        :type dataproc_cluster: string
        :param dataproc_spark_properties: Map for the Pig properties. Ideal to put in
            default arguments
        :type dataproc_spark_properties: dict
        :param dataproc_spark_jars: URIs to jars provisioned in Cloud Storage (example:
            for UDFs and libs) and are ideal to put in default arguments.
        :type dataproc_spark_jars: list
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        """
        super(DataProcSparkOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.job_name = job_name
        self.dataproc_cluster = dataproc_cluster
        self.dataproc_properties = dataproc_spark_properties
        self.dataproc_jars = dataproc_spark_jars

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.dataproc_cluster, "sparkJob",
                                       self.dataproc_properties)

        job.set_main(self.main_jar, self.main_class)
        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.set_job_name(self.job_name)

        hook.submit(hook.project_id, job.build())


class DataProcHadoopOperator(BaseOperator):
    """
    Start a Hadoop Job on a Cloud DataProc cluster.
    """

    template_fields = ['arguments', 'job_name', 'dataproc_cluster']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            main_jar=None,
            main_class=None,
            arguments=None,
            archives=None,
            files=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            dataproc_cluster='cluster-1',
            dataproc_hadoop_properties=None,
            dataproc_hadoop_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        Create a new DataProcHadoopOperator.

        :param main_jar: URI of the job jar provisioned on Cloud Storage. (use this or
            the main_class, not both together).
        :type main_jar: string
        :param main_class: Name of the job class. (use this or the main_jar, not both
            together).
        :type main_class: string
        :param arguments: Arguments for the job.
        :type arguments: list
        :param archives: List of archived files that will be unpacked in the work
            directory. Should be stored in Cloud Storage.
        :type archives: list
        :param files: List of files to be copied to the working directory
        :type files: list
        :param job_name: The job name used in the DataProc cluster. This name by default
            is the task_id appended with the execution data, but can be templated. The
            name will always be appended with a random number to avoid name clashes.
        :type job_name: string
        :param dataproc_cluster: The id of the DataProc cluster.
        :type dataproc_cluster: string
        :param dataproc_hadoop_properties: Map for the Pig properties. Ideal to put in
            default arguments
        :type dataproc_hadoop_properties: dict
        :param dataproc_hadoop_jars: URIs to jars provisioned in Cloud Storage (example:
            for UDFs and libs) and are ideal to put in default arguments.
        :type dataproc_hadoop_jars: list
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        """
        super(DataProcHadoopOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.job_name = job_name
        self.dataproc_cluster = dataproc_cluster
        self.dataproc_properties = dataproc_hadoop_properties
        self.dataproc_jars = dataproc_hadoop_jars

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.dataproc_cluster, "hadoopJob",
                                       self.dataproc_properties)

        job.set_main(self.main_jar, self.main_class)
        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.set_job_name(self.job_name)

        hook.submit(hook.project_id, job.build())


class DataProcPySparkOperator(BaseOperator):
    """
    Start a PySpark Job on a Cloud DataProc cluster.
    """

    template_fields = ['arguments', 'job_name', 'dataproc_cluster']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            main,
            arguments=None,
            archives=None,
            pyfiles=None,
            files=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            dataproc_cluster='cluster-1',
            dataproc_pyspark_properties=None,
            dataproc_pyspark_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        Create a new DataProcPySparkOperator.

        :param main: [Required] The Hadoop Compatible Filesystem (HCFS) URI of the main
            Python file to use as the driver. Must be a .py file.
        :type main: string
        :param arguments: Arguments for the job.
        :type arguments: list
        :param archives: List of archived files that will be unpacked in the work
            directory. Should be stored in Cloud Storage.
        :type archives: list
        :param files: List of files to be copied to the working directory
        :type files: list
        :param pyfiles: List of Python files to pass to the PySpark framework.
            Supported file types: .py, .egg, and .zip
        :type pyfiles: list
        :param job_name: The job name used in the DataProc cluster. This name by default
            is the task_id appended with the execution data, but can be templated. The
            name will always be appended with a random number to avoid name clashes.
        :type job_name: string
        :param dataproc_cluster: The id of the DataProc cluster.
        :type dataproc_cluster: string
        :param dataproc_pyspark_properties: Map for the Pig properties. Ideal to put in
            default arguments
        :type dataproc_pyspark_properties: dict
        :param dataproc_pyspark_jars: URIs to jars provisioned in Cloud Storage (example:
            for UDFs and libs) and are ideal to put in default arguments.
        :type dataproc_pyspark_jars: list
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: string
         """
        super(DataProcPySparkOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.main = main
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.pyfiles = pyfiles
        self.job_name = job_name
        self.dataproc_cluster = dataproc_cluster
        self.dataproc_properties = dataproc_pyspark_properties
        self.dataproc_jars = dataproc_pyspark_jars

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.dataproc_cluster, "pysparkJob",
                                       self.dataproc_properties)

        job.set_python_main(self.main)
        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.add_python_file_uris(self.pyfiles)
        job.set_job_name(self.job_name)

        hook.submit(hook.project_id, job.build())

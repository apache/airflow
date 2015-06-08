from datetime import datetime
import logging
from urlparse import urlparse
from time import sleep

from airflow import settings
from airflow import hooks
from airflow.models import BaseOperator
from airflow.models import Connection as DB
from airflow.models import State
from airflow.models import TaskInstance
from airflow.utils import apply_defaults


class BaseSensorOperator(BaseOperator):
    '''
    Sensor operators are derived from this class an inherit these attributes.

    Sensor operators keep executing at a time interval and succeed when
        a criteria is met and fail if and when they time out.

    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    '''
    ui_color = '#e6f1f2'

    @apply_defaults
    def __init__(
            self,
            poke_interval=60,
            timeout=60*60*24*7,
            *args, **kwargs):
        super(BaseSensorOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout

    def poke(self):
        '''
        Function that the sensors defined while deriving this class should
        override.
        '''
        raise Exception('Override me.')

    def execute(self, context):
        started_at = datetime.now()
        while not self.poke():
            sleep(self.poke_interval)
            if (datetime.now() - started_at).seconds > self.timeout:
                raise Exception('Snap. Time is OUT.')
        logging.info("Success criteria met. Exiting.")


class SqlSensor(BaseSensorOperator):
    """
    Runs a sql statement until a criteria is met. It will keep trying until
    sql returns no row, or if the first cell in (0, '0', '').

    :param conn_id: The connection to run the sensor against
    :type conn_id: string
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    """
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(self, conn_id, sql, *args, **kwargs):

        super(SqlSensor, self).__init__(*args, **kwargs)

        self.sql = sql
        self.conn_id = conn_id

        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == conn_id).first()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
        self.hook = db.get_hook()
        session.commit()
        session.close()

    def poke(self):
        logging.info('Poking: ' + self.sql)
        records = self.hook.get_records(self.sql)
        if not records:
            return False
        else:
            if str(records[0][0]) in ('0', '',):
                return False
            else:
                return True
            print(records[0][0])


class ExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a task to complete in a different DAG

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
    """
    template_fields = ('execution_date',)

    @apply_defaults
    def __init__(self, external_dag_id, external_task_id, *args, **kwargs):
        super(ExternalTaskSensor, self).__init__(*args, **kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.execution_date = "{{ execution_date }}"

    def poke(self):
        logging.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} on '
            '{self.execution_date} ... '.format(**locals()))
        TI = TaskInstance
        session = settings.Session()
        count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state == State.SUCCESS,
            TI.execution_date == self.execution_date
        ).count()
        session.commit()
        session.close()
        return count


class HivePartitionSensor(BaseSensorOperator):
    """
    Waits for a partition to show up in Hive

    :param table: The name of the table to wait for, supports the dot
        notation (my_database.my_table)
    :type table: string
    :param partition: The partition clause to wait for. This is passed as
        is to the Metastore Thrift client "get_partitions_by_filter" method,
        and apparently supports SQL like notation as in `ds='2015-01-01'
        AND type='value'` and > < sings as in "ds>=2015-01-01"
    :type partition: string
    """
    template_fields = ('table', 'partition',)

    @apply_defaults
    def __init__(
            self,
            table, partition="ds='{{ ds }}'",
            metastore_conn_id='metastore_default',
            schema='default',
            poke_interval=60*3,
            *args, **kwargs):
        super(HivePartitionSensor, self).__init__(
            poke_interval=poke_interval, *args, **kwargs)
        if '.' in table:
            schema, table = table.split('.')
        if not partition:
            partition = "ds='{{ ds }}'"
        self.metastore_conn_id = metastore_conn_id
        self.table = table
        self.partition = partition
        self.schema = schema

    def poke(self):
        logging.info(
            'Poking for table {self.schema}.{self.table}, '
            'partition {self.partition}'.format(**locals()))
        if not hasattr(self, 'hook'):
            self.hook = hooks.HiveMetastoreHook(
                metastore_conn_id=self.metastore_conn_id)
        return self.hook.check_for_partition(
            self.schema, self.table, self.partition)


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)

    @apply_defaults
    def __init__(
            self,
            filepath,
            hdfs_conn_id='hdfs_default',
            *args, **kwargs):
        super(HdfsSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.hdfs_conn_id = hdfs_conn_id

    def poke(self):
        sb = hooks.HDFSHook(self.hdfs_conn_id).get_conn()
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        logging.info(
            'Poking for file {self.filepath} '.format(**locals()))
        try:
            files = [f for f in sb.ls([self.filepath])]
        except:
            return False
        return True


class S3KeySensor(BaseSensorOperator):
    """
    Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level.
    :type bucket_key: str
    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :type wildcard_match: bool
    """
    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(
            self, bucket_key,
            bucket_name=None,
            wildcard_match=False,
            s3_conn_id='s3_default',
            *args, **kwargs):
        super(S3KeySensor, self).__init__(*args, **kwargs)
        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == s3_conn_id).first()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
        # Parse
        if bucket_name is None:
            parsed_url = urlparse(bucket_key)
            if parsed_url.netloc == '':
                raise Exception('Please provide a bucket_name')
            else:
                bucket_name = parsed_url.netloc
                if parsed_url.path[0] == '/':
                    bucket_key = parsed_url.path[1:]
                else:
                    bucket_key = parsed_url.path
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.s3_conn_id = s3_conn_id
        session.commit()
        session.close()

    def poke(self):
        hook = hooks.S3Hook(s3_conn_id=self.s3_conn_id)
        full_url = "s3://" + self.bucket_name + self.bucket_key
        logging.info('Poking for key : {full_url}'.format(**locals()))
        if self.wildcard_match:
            return hook.check_for_wildcard_key(self.bucket_key,
                                               self.bucket_name)
        else:
            return hook.check_for_key(self.bucket_key, self.bucket_name)


class S3PrefixSensor(BaseSensorOperator):
    """
    Waits for a prefix to exist. A prefix is the first part of a key,
    thus enabling checking of constructs similar to glob airfl* or
    SQL LIKE 'airfl%'. There is the possibility to precise a delimiter to
    indicate the hierarchy or keys, meaning that the match will stop at that
    delimiter. Current code accepts sane delimiters, i.e. characters that
    are NOT special characters in the Python regex engine.


    :param prefix: The key being waited on. Supports full s3:// style url or
        or relative path from root level.
    :type prefix: str
    :param delimiter: The delimiter intended to show hierarchy.
        Defaults to '/'.
    :type delimiter: str
    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    """
    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(
            self, bucket_name,
            prefix, delimiter='/',
            s3_conn_id='s3_default',
            *args, **kwargs):
        super(S3PrefixSensor, self).__init__(*args, **kwargs)
        session = settings.Session()
        db = session.query(DB).filter(DB.conn_id == s3_conn_id).first()
        if not db:
            raise Exception("conn_id doesn't exist in the repository")
        # Parse
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.delimiter = delimiter
        self.full_url = "s3://" + bucket_name + '/' + prefix
        self.s3_conn_id = s3_conn_id
        session.commit()
        session.close()

    def poke(self):
        logging.info('Poking for prefix : {self.prefix}\n'
                     'in bucket s3://{self.bucket_name}'.format(**locals()))
        hook = hooks.S3Hook(s3_conn_id=self.s3_conn_id)
        return hook.check_for_prefix(
            prefix=self.prefix,
            delimiter=self.delimiter,
            bucket_name=self.bucket_name)


class TimeSensor(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    :param target_time: time after which the job succeeds
    :type target_time: datetime.time
    """
    template_fields = tuple()

    @apply_defaults
    def __init__(self, target_time, *args, **kwargs):
        super(TimeSensor, self).__init__(*args, **kwargs)
        self.target_time = target_time

    def poke(self):
        logging.info(
            'Checking if the time ({0}) has come'.format(self.target_time))
        return datetime.now().time() > self.target_time

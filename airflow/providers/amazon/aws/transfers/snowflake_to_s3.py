"""
Transfers data from Snowflake into a S3 Bucket.
"""
from typing import List, Optional, Union
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeToS3Operator(BaseOperator):
    """
    UNLOAD a query or a table from Snowflake to S3
    :param schema: reference to a specific schema in snowflake database
    :type schema: str
    :param table: reference to a specific table in snowflake database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket where the data will be saved
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key within the previous bucket
    :type s3_key: str
    :param file_format: can be either a previous file format created in Snowflake console or hardcoded one like
        ``type = csv
         field_delimiter = ','
         skip_header = 1
         compression=None``
    :type file_format: str
    :param sql: optional parameter to unload a customized query instead an entire table
    :type sql: str
    :param snowflake_conn_id: reference to a specific snowflake database
    :type snowflake_conn_id: str
    :param unload_options: reference to a str of UNLOAD options. For instance, a valid string would be:
          ``SINGLE=TRUE
           MAX_FILE_SIZE = 5368709119
           OVERWRITE = TRUE``
    :type unload_options: str
    """

    template_fields = ('s3_key', 's3_bucket', 'sql', 'table', 'snowflake_conn_id',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        file_format: str,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        sql: Optional[str] = None,
        snowflake_conn_id: str = "snowflake_default",
        unload_options: Optional[str] = None,
        autocommit: bool = True,
        **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.snowflake_conn_id = snowflake_conn_id
        self.schema = schema
        self.table = table
        self.sql = sql
        self.unload_options = unload_options
        self.autocommit = autocommit

    def execute(self, context):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        if not self.sql:
            self.sql = self.schema + '.' + self.table

        unload_query = f"""
                    COPY INTO 's3://{self.s3_bucket}/{self.s3_key}'
                    FROM ({self.sql})
                    STORAGE_INTEGRATION = S3
                    FILE_FORMAT = ({self.file_format})
                    {self.unload_options};
                    """

        self.log.info('Executing UNLOAD command...')
        snowflake_hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete...")

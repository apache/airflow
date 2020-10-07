import decimal

import datetime
import sys

import time

from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.utils.decorators import apply_defaults

PY3 = sys.version_info[0] == 3


class VerticaToGoogleCloudStorageOperator(BaseSQLToGCSOperator):
    """
    Copy data from Vertica to Google Cloud Storage in JSON or CSV format.

    :param vertica_conn_id: Reference to a specific Vertica hook.
    :type vertica_conn_id: str
    """

    type_map = {
        4: 'STRING',
        5: 'BOOLEAN',
        6: 'INTEGER',
        7: 'FLOAT',
        8: 'STRING',
        9: 'STRING',
        10: 'TIMESTAMP',
        11: 'TIMESTAMP',
        12: 'TIMESTAMP',
        13: 'TIMESTAMP',
        14: 'TIMESTAMP',
        114: 'TIMESTAMP',
        15: 'TIMESTAMP',
        16: 'NUMERIC',
        17: 'STRING',
        20: 'STRING',
        115: 'STRING',
        116: 'STRING',
        117: 'STRING'
    }

    @apply_defaults
    def __init__(self,
        vertica_conn_id='vertica_default',
        *args,
        **kwargs):
        super(VerticaToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id

    def query(self):
        """
        Queries Vertica and returns a cursor of results.

        :return: vertica cursor
        """
        vertica = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        conn = vertica.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor

    def field_to_bigquery(self, field):
        """Convert a DBAPI field to BigQuery schema format."""
        return {
            'name': field[0].replace(" ", "_"),
            'type': self.type_map.get(field[1], "STRING"),
            'mode': "NULLABLE",
        }

    def convert_type(self, value, schema_type):
        """
        Takes a value from Vertica, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Times are converted to seconds.
        """

        if isinstance(value, (datetime.datetime, datetime.date)):
            return time.mktime(value.timetuple())
        if isinstance(value, datetime.time):
            formated_time = time.strptime(str(value), "%H:%M:%S")
            return datetime.timedelta(
                hours=formated_time.tm_hour,
                minutes=formated_time.tm_min,
                seconds=formated_time.tm_sec).seconds
        if isinstance(value, decimal.Decimal):
            return float(value)
        return value

    def get_cursor_iterator(self, cursor):
        """Returns iterator for vertica cursor"""
        return cursor.iterate()

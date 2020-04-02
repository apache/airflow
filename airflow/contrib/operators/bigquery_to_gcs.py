#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module is deprecated. Please use `airflow.providers.google.cloud.operators.bigquery_to_gcs`."""

import warnings

from airflow.providers.google.cloud.operators.bigquery_to_gcs import BigQueryToGCSOperator

<<<<<<< HEAD
_log = logging.getLogger(__name__)

=======
warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.bigquery_to_gcs`.",
    DeprecationWarning, stacklevel=2
)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d


class BigQueryToCloudStorageOperator(BigQueryToGCSOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.bigquery_to_gcs.BigQueryToGCSOperator`.
    """

<<<<<<< HEAD
    def execute(self, context):
        _log.info('Executing extract of %s into: %s',
                     self.source_project_dataset_table,
                     self.destination_cloud_storage_uris)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_extract(
            self.source_project_dataset_table,
            self.destination_cloud_storage_uris,
            self.compression,
            self.export_format,
            self.field_delimiter,
            self.print_header)
=======
    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.bigquery_to_gcs.BigQueryToGCSOperator`.""",
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d

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
"""
This module is deprecated. Please use `airflow.providers.google.cloud.operators.gcs_to_bigquery`.
"""

import warnings

from airflow.providers.google.cloud.operators.gcs_to_bigquery import GCSToBigQueryOperator

<<<<<<< HEAD
_log = logging.getLogger(__name__)

=======
warnings.warn(
    "This module is deprecated. Please use `airflow.providers.google.cloud.operators.gcs_to_bigquery`.",
    DeprecationWarning, stacklevel=2
)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d


class GoogleCloudStorageToBigQueryOperator(GCSToBigQueryOperator):
    """
    This class is deprecated.
    Please use `airflow.providers.google.cloud.operators.gcs_to_bq.GCSToBigQueryOperator`.
    """

<<<<<<< HEAD
        if self.max_id_key:
            cursor.execute('SELECT MAX({}) FROM {}'.format(self.max_id_key, self.destination_project_dataset_table))
            row = cursor.fetchone()
            max_id = row[0] if row[0] else 0
            _log.info('Loaded BQ data with max {}.{}={}'.format(self.destination_project_dataset_table, self.max_id_key, max_id))
            return max_id
=======
    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.google.cloud.operators.gcs_to_bq.GCSToBigQueryOperator`.""",
            DeprecationWarning, stacklevel=2
        )
        super().__init__(*args, **kwargs)
>>>>>>> 0d5ecde61bc080d2c53c9021af252973b497fb7d

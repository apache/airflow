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
import pytest

from airflow.providers.google.cloud.example_dags.example_dataproc import PYSPARK_MAIN, BUCKET, SPARKR_MAIN
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_DATAPROC_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

GCS_URI = f"gs://{BUCKET}"

pyspark_file = """
#!/usr/bin/python
import pyspark
sc = pyspark.SparkContext()
rdd = sc.parallelize(['Hello,', 'world!'])
words = sorted(rdd.collect())
print(words)
"""

sparkr_file = """
#!/usr/bin/r
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
Sys.setenv(SPARK_HOME = "/home/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session()
# Create the SparkDataFrame
df <- as.DataFrame(faithful)
head(summarize(groupBy(df, df$waiting), count = n(df$waiting)))
"""


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_DATAPROC_KEY)
class DataprocExampleDagsTest(GoogleSystemTest):
    @provide_gcp_context(GCP_DATAPROC_KEY)
    def setUp(self):
        super().setUp()
        self.create_gcs_bucket(BUCKET)
        self.upload_content_to_gcs(lines=pyspark_file, bucket=GCS_URI, filename=PYSPARK_MAIN)
        self.upload_content_to_gcs(lines=sparkr_file, bucket=GCS_URI, filename=SPARKR_MAIN)

    @provide_gcp_context(GCP_DATAPROC_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(BUCKET)
        super().tearDown()

    @provide_gcp_context(GCP_DATAPROC_KEY)
    def test_run_example_dag(self):
        self.run_dag(dag_id="example_gcp_dataproc", dag_folder=CLOUD_DAG_FOLDER)

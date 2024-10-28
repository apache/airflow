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
from __future__ import annotations

# [START tutorial]
# [START import_module]
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

# [END import_module]

API = "https://opendata.fmi.fi/timeseries"

aq_fields = {
    "fmisid": "int32",
    "time": "datetime64[ns]",
    "AQINDEX_PT1H_avg": "float64",
    "PM10_PT1H_avg": "float64",
    "PM25_PT1H_avg": "float64",
    "O3_PT1H_avg": "float64",
    "CO_PT1H_avg": "float64",
    "SO2_PT1H_avg": "float64",
    "NO2_PT1H_avg": "float64",
    "TRSC_PT1H_avg": "float64",
}

# [START create_object_storage_path]
base = ObjectStoragePath("s3://aws_default@airflow-tutorial-data/")
# [END create_object_storage_path]


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def tutorial_objectstorage():
    """
    ### Object Storage Tutorial Documentation
    This is a tutorial DAG to showcase the usage of the Object Storage API.
    Documentation that goes along with the Airflow Object Storage tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/objectstorage.html)
    """

    # [START get_air_quality_data]
    @task
    def get_air_quality_data(**kwargs) -> ObjectStoragePath:
        """
        #### Get Air Quality Data
        This task gets air quality data from the Finnish Meteorological Institute's
        open data API. The data is saved as parquet.
        """
        import pandas as pd

        execution_date = kwargs["logical_date"]
        start_time = kwargs["data_interval_start"]

        params = {
            "format": "json",
            "precision": "double",
            "groupareas": "0",
            "producer": "airquality_urban",
            "area": "Uusimaa",
            "param": ",".join(aq_fields.keys()),
            "starttime": start_time.isoformat(timespec="seconds"),
            "endtime": execution_date.isoformat(timespec="seconds"),
            "tz": "UTC",
        }

        response = requests.get(API, params=params)
        response.raise_for_status()

        # ensure the bucket exists
        base.mkdir(exist_ok=True)

        formatted_date = execution_date.format("YYYYMMDD")
        path = base / f"air_quality_{formatted_date}.parquet"

        df = pd.DataFrame(response.json()).astype(aq_fields)
        with path.open("wb") as file:
            df.to_parquet(file)

        return path

    # [END get_air_quality_data]

    # [START analyze]
    @task
    def analyze(path: ObjectStoragePath, **kwargs):
        """
        #### Analyze
        This task analyzes the air quality data, prints the results
        """
        import duckdb

        conn = duckdb.connect(database=":memory:")
        conn.register_filesystem(path.fs)
        conn.execute(
            f"CREATE OR REPLACE TABLE airquality_urban AS SELECT * FROM read_parquet('{path}')"
        )

        df2 = conn.execute("SELECT * FROM airquality_urban").fetchdf()

        print(df2.head())

    # [END analyze]

    # [START main_flow]
    obj_path = get_air_quality_data()
    analyze(obj_path)
    # [END main_flow]


# [START dag_invocation]
tutorial_objectstorage()
# [END dag_invocation]
# [END tutorial]

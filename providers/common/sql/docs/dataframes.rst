.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

SQL Data Frames Integration
==============================

The :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` provides built-in integration with popular data analysis frameworks, allowing you to directly query databases and retrieve results as either Pandas or Polars dataframes. This integration simplifies data workflows by eliminating the need for manual conversion between SQL query results and data frames.

Pandas Integration
--------------------------

`Pandas <https://pandas.pydata.org/>`_ is a widely used data analysis and manipulation library. The SQL hook allows you to directly retrieve query results as Pandas DataFrames, which is particularly useful for further data transformation, analysis, or visualization within your Airflow tasks.

.. code-block:: python

    # Get complete DataFrame in a single operation
    df = hook.get_df(
        sql="SELECT * FROM my_table WHERE date_column >= %s", parameters=["2023-01-01"], df_type="pandas"
    )

    # Get DataFrame in chunks for memory-efficient processing of large results
    for chunk_df in hook.get_df_by_chunks(sql="SELECT * FROM large_table", chunksize=10000, df_type="pandas"):
        process_chunk(chunk_df)

To use this feature, install the ``pandas`` extra when installing this provider package. For installation instructions, see <index>.

Polars Integration
--------------------------

`Polars <https://pola.rs/>`_ is a modern, high-performance DataFrame library implemented in Rust with Python bindings. It's designed for speed and efficiency when working with large datasets. The SQL hook supports retrieving data directly as Polars DataFrames, which can be particularly beneficial for performance-critical data processing tasks.

.. code-block:: python

    # Get complete DataFrame in a single operation
    df = hook.get_df(
        sql="SELECT * FROM my_table WHERE date_column >= %s",
        parameters={"date_column": "2023-01-01"},
        df_type="polars",
    )

    # Get DataFrame in chunks for memory-efficient processing of large results
    for chunk_df in hook.get_df_by_chunks(sql="SELECT * FROM large_table", chunksize=10000, df_type="polars"):
        process_chunk(chunk_df)

To use this feature, install the ``polars`` extra when installing this provider package. For installation instructions, see <index>.

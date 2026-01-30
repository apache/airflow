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

from typing import TYPE_CHECKING

from airflow.sdk.module_loading import qualname

# lazy loading for performance reasons
serializers = [
    "pandas.core.frame.DataFrame",
]
deserializers = serializers

if TYPE_CHECKING:
    import pandas as pd

    from airflow.sdk.serde import U

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    import pandas as pd
    import pyarrow as pa
    from pyarrow import parquet as pq

    if not isinstance(o, pd.DataFrame):
        return "", "", 0, False

    # for now, we *always* serialize into in memory
    # until we have a generic backend that manages
    # sinks
    table = pa.Table.from_pandas(o)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")

    return buf.getvalue().hex().decode("utf-8"), qualname(o), __version__, True


def deserialize(cls: type, version: int, data: object) -> pd.DataFrame:
    if version > __version__:
        raise TypeError(f"serialized {version} of {qualname(cls)} > {__version__}")

    import pandas as pd

    if cls is not pd.DataFrame:
        raise TypeError(f"do not know how to deserialize {qualname(cls)}")

    if not isinstance(data, str):
        raise TypeError(f"serialized {qualname(cls)} has wrong data type {type(data)}")

    from io import BytesIO

    from pyarrow import parquet as pq

    with BytesIO(bytes.fromhex(data)) as buf:
        df = pq.read_table(buf).to_pandas()

    return df

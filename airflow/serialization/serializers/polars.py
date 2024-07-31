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

from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    import polars as pl

    from airflow.serialization.serde import U


serializers = ["polars.dataframe.frame.DataFrame", "polars.series.series.Series"]
deserializers = serializers
__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    from io import BytesIO

    import polars as pl

    if not isinstance(o, (pl.DataFrame, pl.Series)):
        return "", "", 0, False

    name = qualname(o)
    if isinstance(o, pl.Series):
        o = o.to_frame(o.name)

    with BytesIO() as io:
        o.write_parquet(io, compression="snappy")
        result = io.getvalue().hex()

    return result, name, __version__, True


def deserialize(classname: str, version: int, data: object) -> pl.DataFrame | pl.Series:
    if version > __version__:
        error_msg = f"serialized {version} of {classname} > {__version__}"
        raise TypeError(error_msg)

    if not isinstance(data, str):
        error_msg = f"serialized {classname} has wrong data type {type(data)}"
        raise TypeError(error_msg)

    from io import BytesIO

    import polars as pl

    with BytesIO(bytes.fromhex(data)) as io:
        frame = pl.read_parquet(io)

    if classname.split(".")[-1] == "Series":
        return frame.get_column(frame.columns[0])
    return frame

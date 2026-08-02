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
#
# ``pandas.core.frame.DataFrame`` is what a DataFrame qualifies as under pandas 2. Under
# pandas 3 the public classes moved to the ``pandas`` namespace, so the same object
# qualifies as ``pandas.DataFrame``. That name is registered too, but *only* so that the
# registry dispatches here and this module can refuse it with an actionable message --
# without it the caller gets serde's generic "cannot serialize object of type" instead.
# See ``_reject_pandas_3`` below.
serializers = [
    "pandas.core.frame.DataFrame",
    "pandas.DataFrame",
]
deserializers = serializers

# First pandas major version whose DataFrame XComs this version of Airflow does not support.
_FIRST_UNSUPPORTED_PANDAS_MAJOR = 3


def _reject_pandas_3(pd) -> None:
    """
    Refuse a DataFrame XCom when the installed pandas is 3 or newer.

    pandas is an optional dependency, so the ``<3`` constraint Airflow declares cannot be
    relied on: a deployment may install pandas 3 directly, or pull it in through another
    package. This is the runtime half of that constraint.

    The failure is deliberate rather than best-effort. Under pandas 3 a DataFrame round
    trips through parquet with different dtypes than under pandas 2 -- an ``object`` column
    comes back as ``str`` and missing values as ``nan`` rather than ``None`` -- so silently
    accepting the value would hand tasks data that differs from what was written, with no
    signal. Failing on the write is the louder and more recoverable half of that.
    """
    major = int(pd.__version__.split(".")[0])
    if major < _FIRST_UNSUPPORTED_PANDAS_MAJOR:
        return
    raise RuntimeError(
        f"DataFrame XComs are not supported with pandas {pd.__version__}: this version of "
        f"Airflow supports pandas < {_FIRST_UNSUPPORTED_PANDAS_MAJOR} for XCom serialization. "
        "Pin pandas < 3 in the environment that runs your tasks, or avoid passing DataFrames "
        "through XCom. Moving to pandas 3 is a deliberate migration -- its DataFrames read "
        "back with different dtypes -- and Airflow will enable it in a future release."
    )


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

    _reject_pandas_3(pd)

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

    # Reading is refused too, not just writing: a payload written under pandas 2 comes back
    # with pandas 3 dtypes, so the task would silently receive different data than was
    # pushed. An error is the safer outcome.
    _reject_pandas_3(pd)

    if not isinstance(data, str):
        raise TypeError(f"serialized {qualname(cls)} has wrong data type {type(data)}")

    from io import BytesIO

    from pyarrow import parquet as pq

    with BytesIO(bytes.fromhex(data)) as buf:
        df = pq.read_table(buf).to_pandas()

    return df

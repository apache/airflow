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

import numpy
import pandas


def fix_int_dtypes(df: pandas.DataFrame) -> None:
    """Mutate DataFrame to set dtypes for int columns containing NaN values."""
    for col in df:
        if "float" in df[col].dtype.name and df[col].hasnans:
            notna_series = df[col].dropna().values
            if numpy.equal(notna_series, notna_series.astype(int)).all():
                df[col] = numpy.where(df[col].isnull(), None, df[col])
                df[col] = df[col].astype('Int64')
            elif numpy.isclose(notna_series, notna_series.astype(int)).all():
                df[col] = numpy.where(df[col].isnull(), None, df[col])
                df[col] = df[col].astype('float64')

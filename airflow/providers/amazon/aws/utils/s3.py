import numpy
import pandas


def fix_int_dtypes(df: pandas.DataFrame) -> None:
    for col in df:
        if "float" in df[col].dtype.name and df[col].hasnans:
            notna_series = df[col].dropna().values
            if numpy.equal(notna_series, notna_series.astype(int)).all():
                df[col] = numpy.where(df[col].isnull(), None, df[col])
                df[col] = df[col].astype('Int64')
            elif numpy.isclose(notna_series, notna_series.astype(int)).all():
                df[col] = numpy.where(df[col].isnull(), None, df[col])
                df[col] = df[col].astype('float64')

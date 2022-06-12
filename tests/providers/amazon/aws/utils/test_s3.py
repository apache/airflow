import pandas as pd


def test_fix_int_dtypes():
    from airflow.providers.amazon.aws.utils.s3 import fix_int_dtypes
    
    dirty_df = pd.DataFrame({"strings": ["a", "b", "c"], "ints": [1, 2, None]})
    fix_int_dtypes(df=dirty_df)
    assert dirty_df["ints"].dtype.kind == "i"

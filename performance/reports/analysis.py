from typing import Optional

import pandas as pd
from matplotlib import pyplot as plt

GAUGE = "GAUGE"
CUMULATIVE = "CUMULATIVE"
CHANGE_METRIC = "{}_dt"


def count_change_speed(df: pd.DataFrame, metric: str, by_column: str):
    """
    Calculates speed of change of given cumulative metric.

    :param df: dataframe with data
    :param metric: column name of the metrics
    :param by_column: what should distinguish two different attempts
    """
    result = pd.DataFrame()
    for node in df.node_name.unique():
        tmp_df = df[(df.node_name == node) & df[metric].notnull()][
            [by_column, "uuid", "timestamp", metric]
        ].sort_values("timestamp")
        data = tmp_df[metric].values
        # Cut first rows as the change is 0 there
        tmp_df = tmp_df.iloc[1:, :]
        tmp_df[CHANGE_METRIC.format(metric)] = data[1:] - data[:-1]
        result = pd.concat([result, tmp_df])
    return result


def boxplot_metric(
    df: pd.DataFrame, metric: str, by_column: str, save: Optional[str] = None
) -> None:
    """
    Draws a boxplot for given metric, grouping the data by the provided column.
    """
    if CUMULATIVE in metric:
        df = count_change_speed(df, metric, by_column)
        metric = CHANGE_METRIC.format(metric)
    # Sort out nulls
    df = df[df[metric].notnull()]
    df.boxplot(column=[metric], by=by_column, figsize=(35, 15), showmeans=True)
    plt.title(metric, y=1.08)
    plt.suptitle("")
    if save:
        plt.savefig(save)
    else:
        plt.show()


def compare(
    df, version_a: str, version_b: str, metric: str, version_column="composer_version"
):
    data_a = df[(df[version_column] == version_a) & df[metric].notnull()]
    data_b = df[(df[version_column] == version_b) & df[metric].notnull()]

    if CUMULATIVE in metric:
        data_a = count_change_speed(data_a, metric, by_column=version_column)
        data_b = count_change_speed(data_b, metric, by_column=version_column)
        metric = CHANGE_METRIC.format(metric)

    summary = pd.concat([data_a[metric].describe(), data_b[metric].describe()], axis=1)
    summary.drop(["count"], inplace=True)
    summary.columns = [version_a, version_b]
    summary["change[%]"] = round(100 * (summary[version_b] / summary[version_a] - 1), 2)

    print(metric)
    print(summary)


def compare_all_versions(df, metric: str, version_column="composer_version"):
    ver_summaries = []

    if CUMULATIVE in metric:
        df = count_change_speed(df, metric, by_column=version_column)
        metric = CHANGE_METRIC.format(metric)

    versions = sorted(v for v in df[version_column].unique())
    for ver in versions:
        tmp = df[(df[version_column] == ver) & df[metric].notnull()]
        ver_summaries.append(tmp[metric].describe())

    data = pd.concat(ver_summaries, axis=1)
    data.drop(["count"], inplace=True)
    data.columns = versions
    data.T.plot.line(figsize=(15, 6), title=f"{metric}\nby version")
    plt.show()

    changes = pd.DataFrame()
    for col in data.T.columns:
        changes[col] = 100 * (data.T[col].values[:-1] / data.T[col].values[1:] - 1)

    changes.index = data.T.index[1:]
    print(changes)
    changes.plot.line(figsize=(15, 6), title=f"{metric}\nchanges by version")

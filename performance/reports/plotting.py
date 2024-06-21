import warnings
from typing import Dict, List, Optional

from matplotlib import pyplot as plt
from matplotlib.lines import Line2D


def draw_chart_for_time_series_metric(
    time_series_dicts: List[Dict], category: str, save: Optional[str] = None
) -> None:
    """
    Plots the time series for selected metric category using time series from given dictionaries.

    :param time_series_dicts: list of up to two dictionaries containing time series for uuids
    :type time_series_dicts: List[Dict]
    :param category: metric category to plot
    :type category: str
    :param save: optional path under which the report should be saved.
    :type save: str
    """
    uuids = []
    fig = plt.figure()

    if not time_series_dicts or len(time_series_dicts) > 2:
        raise ValueError(
            "Please provide a list with up to 2 dictionaries with time series."
        )

    def plot(ts, style: str):
        for uuid in sorted(ts):
            if ts[uuid][category] is None:
                warnings.warn(f"Nan value of {category} for uuid {uuid}.")
                continue

            x_axis = [point[0] for point in ts[uuid][category]]
            y_axis = [point[1] for point in ts[uuid][category]]
            uuids.append(uuid)
            plt.plot(x_axis, y_axis, style)

    plot(time_series_dicts[0], "r:")
    if len(time_series_dicts) > 1:
        plot(time_series_dicts[1], "b:")
        legend_elements = [
            Line2D([0], [0], color="r", lw=4, label="subject"),
            Line2D([0], [0], color="b", lw=4, label="baseline"),
        ]
        plt.legend(handles=legend_elements)
    plt.xlabel("seconds since start of test[s]")
    plt.ylabel("metric value")
    plt.title(category, y=1.08)

    if save:
        plt.savefig(save)
        plt.close(fig)
    else:
        plt.show()

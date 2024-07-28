import datetime
import hashlib
import json
import os
import tempfile
from typing import Dict
import uuid

import pandas as pd
from jinja2 import Template
from tabulate import tabulate

import matplotlib.pyplot as plt
from loguru import logger

from utils.google_cloud.storage_client import StorageClient
from reports.analysis import boxplot_metric
from reports.metrics import (
    AGGREGATED_TIME_SERIES_METRICS,
    TIME_SERIES_METRIC_TYPES_ORDER,
    GENERAL_METRICS,
    TIME_SERIES_CHART_METRIC_COLUMNS,
    BOXPLOT_METRIC_COLUMNS,
)
from reports.pdf import generate_pdf
from reports.plotting import draw_chart_for_time_series_metric
from reports.utils import (
    assign_time_series_metrics,
    CONFIGURATION_COLUMNS,
    PRECISION,
    compare_statistics,
    cache_dataframe,
    analyze_results,
    get_statistics_df,
)

plt.style.use("seaborn-poster")
REPORT_SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)))

REMOVE_FAILED_DAG_RUNS = True
REMOVE_ANOMALOUS_TEST_ATTEMPTS = True
BOXPLOT_CHARTS_DIR_NAME = "boxplot_charts"
TIME_SERIES_CHARTS_DIR_NAME = "time_series_charts"

# group filters should only include the columns from group_by_keys list
AGGREGATED_RESOURCES_DICT = {
    "k8s_node": {
        "group_by_columns": ["uuid", "node_name"],
        "group_filters": {"all": {"node_name": "nodes_combined"}},
    },
    "k8s_pod": {
        "group_by_columns": ["uuid", "pod_name"],
        "group_filters": {
            "airflow-scheduler": {"pod_name": "airflow_schedulers_combined"},
            "airflow-worker": {"pod_name": "airflow_workers_combined"},
        },
    },
    "k8s_container": {
        "group_by_columns": ["uuid", "pod_name"],
        "group_filters": {
            "airflow-scheduler": {"pod_name": "airflow_schedulers_combined"},
            "airflow-worker": {"pod_name": "airflow_workers_combined"},
        },
    },
}
AGGREGATED_RESOURCES_DICT = assign_time_series_metrics(
    AGGREGATED_RESOURCES_DICT, AGGREGATED_TIME_SERIES_METRICS
)


def get_report_template() -> Template:
    """
    Returns the report template for single study component.
    """
    report_template_path = os.path.join(REPORT_SCRIPTS_DIR, "report.md.tpl")
    with open(report_template_path) as f:
        template = f.read()

    return Template(template)


def get_comparison_report_template() -> Template:
    """
    Returns the report template for comparison of two study components.
    """
    report_template_path = os.path.join(REPORT_SCRIPTS_DIR, "comparison_report.md.tpl")
    with open(report_template_path) as f:
        template = f.read()

    return Template(template)

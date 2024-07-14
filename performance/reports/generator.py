import datetime
import hashlib
import json
import os
import tempfile
from typing import Dict
import uuid

import pandas as pd
import google.auth
from google.cloud import bigquery
from jinja2 import Template
from tabulate import tabulate

import matplotlib.pyplot as plt
from loguru import logger

from utils.google_cloud.big_query_client import BigQueryClient
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
STATISTICS_TABLE_NAME = "statistics"

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


def generate_reports_for_study(
    study_components_by_environment_type: Dict,
    storage_client: StorageClient,
    reports_bucket: str,
    study_id: str,
    elastic_dag_config_file_path: str,
):
    """
    Generates and uploads the reports for study components from provided dict.

    :param study_components_by_environment_type: dict containing information about study components
        grouped by their environment type
    :type study_components_by_environment_type: Dict
    :param storage_client: a instance of StorageClient which should be used for storing the reports.
    :type storage_client: StorageClient
    :param reports_bucket: name of the bucket where the reports should be stored.
    :type reports_bucket: str
    :param study_id: unique identifier of given study execution.
    :type study_id: str
    :param elastic_dag_config_file_path: path to file with configuration for elastic DAG.
    :type elastic_dag_config_file_path: str
    """

    analysis_components_dict = {}
    baselines = {}

    _, adc_project_id = google.auth.default()

    # TODO: environment_type can be used to control metrics which are analyzed and compared
    for environment_type in study_components_by_environment_type:

        for study_component in study_components_by_environment_type[environment_type]:

            component_name = study_component["component_name"]
            dataset_id = study_component["args"]["results_dataset"]
            table_name = study_component["args"]["results_object_name"]
            project_id = study_component["args"].get("results_project_id")

            if project_id is None:
                project_id = adc_project_id

            results_table = get_data(dataset_id=dataset_id, table_name=table_name, project_id=project_id)

            analysis_components_dict[component_name] = {}
            analysis_components_dict[component_name]["results_table"] = results_table
            analysis_components_dict[component_name]["project_id"] = project_id
            analysis_components_dict[component_name]["dataset_id"] = dataset_id
            analysis_components_dict[component_name]["table_name"] = table_name

            if study_component.get("baseline_component_name"):
                baselines[component_name] = study_component["baseline_component_name"]

    with tempfile.TemporaryDirectory() as temp_reports_dir:

        # main directories for reports
        study_dir = os.path.join(temp_reports_dir, study_id)
        individual_reports_dir = os.path.join(study_dir, "individual_reports")
        comparison_reports_dir = os.path.join(study_dir, "comparison_reports")

        for component_name in analysis_components_dict:

            results_table, statistics, configuration_information, metrics = analyze_results(
                results_table=analysis_components_dict[component_name]["results_table"],
                resources_dict=AGGREGATED_RESOURCES_DICT,
                general_metrics=GENERAL_METRICS,
                remove_test_attempts_with_failed_dag_runs=REMOVE_FAILED_DAG_RUNS,
                remove_anomalous_test_attempts=REMOVE_ANOMALOUS_TEST_ATTEMPTS,
            )

            # overwrite the results table with the with excluded failed and anomalous runs
            analysis_components_dict[component_name]["results_table"] = results_table
            analysis_components_dict[component_name]["statistics"] = statistics
            analysis_components_dict[component_name]["configuration_information"] = configuration_information
            analysis_components_dict[component_name]["metrics"] = metrics

            report_dir_path = os.path.join(individual_reports_dir, component_name)
            os.makedirs(report_dir_path)

            statistics_df = generate_report_for_results_table(
                analysis_components=analysis_components_dict[component_name],
                report_name=component_name,
                reports_dir=report_dir_path,
            )
            write_statistics_data(
                component_name=component_name,
                configuration_information=analysis_components_dict[component_name][
                    "configuration_information"
                ],
                elastic_dag_config_file_path=elastic_dag_config_file_path,
                project_id=analysis_components_dict[component_name]["project_id"],
                dataset_id=analysis_components_dict[component_name]["dataset_id"],
                statistics_df=statistics_df,
            )

        for component_name in baselines:

            baseline_component_name = baselines[component_name]

            report_name = f"{baseline_component_name}__AND__{component_name}"
            report_dir_path = os.path.join(comparison_reports_dir, report_name)
            os.makedirs(report_dir_path)

            generate_comparison_report(
                baseline_analysis_components=analysis_components_dict[baseline_component_name],
                subject_analysis_components=analysis_components_dict[component_name],
                report_name=report_name,
                reports_dir=report_dir_path,
            )

        storage_client.upload_report_dir(report_dir_path=study_dir, bucket_name=reports_bucket)


@cache_dataframe
def get_data(project_id: str, dataset_id: str, table_name: str) -> pd.DataFrame:
    """
    Collects performance tests results from BigQuery table.

    :param project_id: Google Cloud project of the BigQuery dataset.
    :type project_id: str
    :param dataset_id: name of the BigQuery dataset where the results table is located.
    :type dataset_id: str
    :param table_name: name of the table under which the results are stored.
    :type table_name: str

    :return: a pandas dataframe with collected results.
    :rtype: pd.DataFrame
    """

    logger.info(f"Getting data for project {project_id}, dataset {dataset_id} and table {table_name}.")
    client = bigquery.Client(project=project_id)

    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}`"
    data = client.query(query).to_dataframe()

    # add missing configuration columns and fill missing values
    for column in CONFIGURATION_COLUMNS:
        if column not in data.columns:
            logger.warning(f"Column {column} is missing from the data")
            data[column] = "Unknown"
        else:
            data[column] = data[column].fillna("Unknown")
    logger.info(f"Collected data size: {data.size}")
    return data


def write_statistics_data(
    component_name: str,
    configuration_information: Dict,
    elastic_dag_config_file_path: str,
    project_id: str,
    dataset_id: str,
    statistics_df: pd.DataFrame,
) -> None:
    """
    Writes statistics data to BigQuery table.

    :param component_name: study component name.
    :type component_name: str
    :param configuration_information: pandas DataFrame with configuration information.
    :type configuration_information: pd.DataFrame
    :param elastic_dag_config_file_path: path to file with configuration for elastic DAG.
    :type elastic_dag_config_file_path: str
    :param project_id: Google Cloud project of the BigQuery dataset.
    :type project_id: str
    :param dataset_id: name of the BigQuery dataset where to write statistics table.
    :type dataset_id: str
    :param statistics_df: pandas Dataframe with result statistics.
    :type statistics_df: pd.DataFrame
    """
    if len(configuration_information) > 1:
        raise ValueError(
            "Multiple configurations are present in results data. "
            "Populating statistics table is not allowed."
        )

    logger.info(
        f"Writing statistics data for component {component_name}, project {project_id}, "
        f"dataset {dataset_id}."
    )

    client = BigQueryClient(project_id)

    run_configuration_dict = configuration_information.to_dict(orient="records")[0]
    run_configuration_dict["elastic_dag_config_file_path"] = elastic_dag_config_file_path
    configuration_hash = hashlib.sha1(
        json.dumps(run_configuration_dict, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    d = {
        "id": uuid.uuid1().hex,
        "run_at": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "configuration": json.dumps(run_configuration_dict),
        "configuration_id": configuration_hash.hexdigest(),
    }
    schema = [
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("run_at", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("configuration", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("configuration_id", bigquery.enums.SqlTypeNames.STRING),
    ]
    for _, r in statistics_df.iterrows():
        category = r["category"].replace("-", "_")
        d[category] = r["average_value_float"]
        schema.append(bigquery.SchemaField(category, bigquery.enums.SqlTypeNames.FLOAT))
        d["{}_units".format(category)] = r["average_value"]
        schema.append(bigquery.SchemaField("{}_units".format(category), bigquery.enums.SqlTypeNames.STRING))

    client.upload_results(
        pd.DataFrame([d]),
        dataset_id,
        STATISTICS_TABLE_NAME,
        schema=schema,
        allow_field_addition=True,
    )


def generate_report_for_results_table(
    analysis_components: Dict, report_name: str, reports_dir: str
) -> pd.DataFrame:
    """
    Generates the report for a single study component.

    :param analysis_components: dictionary containing analysis results of given study component
        which will be used to generate the report.
    :type analysis_components: Dict
    :param report_name: name for the report.
    :type report_name: str
    :param reports_dir: local path under which the report contents should be stored.
    :type reports_dir: str

    :return: pandas DataFrame with statistics_df from get_statistics_df()
    :rtype: pd.DataFrame
    """

    statistics_df = get_statistics_df(results_statistics=analysis_components["statistics"])

    report_title = f"{report_name} performance report"
    report_file_path = os.path.join(reports_dir, f"{report_name}.md")

    template = get_report_template()

    # Plot time charts
    time_series_charts_dir_path = os.path.join(reports_dir, TIME_SERIES_CHARTS_DIR_NAME)
    os.makedirs(time_series_charts_dir_path)
    time_series_charts_paths = []
    for col in TIME_SERIES_CHART_METRIC_COLUMNS:
        chart_name = f"{col}_time_series_chart.png"
        chart_full_path = os.path.join(time_series_charts_dir_path, chart_name)
        chart_relative_path = os.path.join(TIME_SERIES_CHARTS_DIR_NAME, chart_name)

        draw_chart_for_time_series_metric([analysis_components["metrics"]], col, save=chart_full_path)
        time_series_charts_paths.append(chart_relative_path)

    box_plot_data = analysis_components["results_table"].copy()
    box_plot_data["study_component"] = "results_data"
    box_plot_data = box_plot_data[box_plot_data["node_name"] == "nodes_combined"]

    box_plots_paths = []
    if len(box_plot_data) != 0:

        boxplot_charts_dir_path = os.path.join(reports_dir, BOXPLOT_CHARTS_DIR_NAME)
        os.makedirs(boxplot_charts_dir_path)

        for col in BOXPLOT_METRIC_COLUMNS:
            chart_name = f"{col}_box_plot.png"
            chart_full_path = os.path.join(boxplot_charts_dir_path, chart_name)
            chart_relative_path = os.path.join(BOXPLOT_CHARTS_DIR_NAME, chart_name)

            boxplot_metric(
                box_plot_data,
                metric=col,
                save=chart_full_path,
                by_column="study_component",
            )
            box_plots_paths.append(chart_relative_path)

    content = template.render(
        report_title=report_title,
        creation_date=datetime.datetime.now(),
        project_id=analysis_components["project_id"],
        dataset_id=analysis_components["dataset_id"],
        table_id=analysis_components["table_name"],
        conf_table=tabulate(
            analysis_components["configuration_information"].T,
            headers="keys",
            tablefmt="github",
        ),
        statistics_df=tabulate(
            statistics_df,
            headers="keys",
            tablefmt="github",
            floatfmt=f".{PRECISION}f",
            showindex=False,
        ),
        time_series_charts_paths=time_series_charts_paths,
        box_plots_paths=box_plots_paths,
    )

    with open(report_file_path, "w+") as f:
        f.write(content)

    generate_pdf(report_file_path)

    return statistics_df


def generate_comparison_report(
    subject_analysis_components: Dict,
    baseline_analysis_components: Dict,
    report_name: str,
    reports_dir: str,
) -> None:
    """
    Generates the report comparing performance of two study components.

    :param subject_analysis_components: dictionary containing analysis results
        for the study component which is the subject of comparison.
    :type subject_analysis_components: Dict
    :param baseline_analysis_components: dictionary containing analysis results
        for the study component which will serve as a baseline of comparison.
    :type baseline_analysis_components: Dict
    :param report_name: name for the report.
    :type report_name: str
    :param reports_dir: local path under which the report contents should be stored.
    :type reports_dir: str
    """

    statistics_comparison_df = compare_statistics(
        subject_statistics=subject_analysis_components["statistics"],
        baseline_statistics=baseline_analysis_components["statistics"],
    )

    report_title = f"{report_name} performance comparison report"
    report_file_path = os.path.join(reports_dir, f"{report_name}.md")

    template = get_comparison_report_template()

    # Plot time charts
    time_series_charts_dir_path = os.path.join(reports_dir, TIME_SERIES_CHARTS_DIR_NAME)
    os.makedirs(time_series_charts_dir_path)
    time_series_charts_paths = []
    for col in TIME_SERIES_CHART_METRIC_COLUMNS:
        chart_name = f"{col}_time_series_chart.png"
        chart_full_path = os.path.join(time_series_charts_dir_path, chart_name)
        chart_relative_path = os.path.join(TIME_SERIES_CHARTS_DIR_NAME, chart_name)

        draw_chart_for_time_series_metric(
            [
                subject_analysis_components["metrics"],
                baseline_analysis_components["metrics"],
            ],
            col,
            save=chart_full_path,
        )
        time_series_charts_paths.append(chart_relative_path)

    subject_data = subject_analysis_components["results_table"].copy()
    subject_data["study_component"] = "subject_data"
    subject_data = subject_data[subject_data["node_name"] == "nodes_combined"]
    baseline_data = baseline_analysis_components["results_table"].copy()
    baseline_data["study_component"] = "baseline_data"
    baseline_data = baseline_data[baseline_data["node_name"] == "nodes_combined"]

    boxplot_data_columns = [
        "study_component",
        "uuid",
        "timestamp",
        "node_name",
    ] + BOXPLOT_METRIC_COLUMNS

    box_plot_data = pd.concat(
        [subject_data[boxplot_data_columns], baseline_data[boxplot_data_columns]],
        axis=0,
        ignore_index=True,
    )

    box_plots_paths = []
    if len(box_plot_data) != 0:

        boxplot_charts_dir_path = os.path.join(reports_dir, BOXPLOT_CHARTS_DIR_NAME)
        os.makedirs(boxplot_charts_dir_path)

        for col in BOXPLOT_METRIC_COLUMNS:
            chart_name = f"{col}_box_plot.png"
            chart_full_path = os.path.join(boxplot_charts_dir_path, chart_name)
            chart_relative_path = os.path.join(BOXPLOT_CHARTS_DIR_NAME, chart_name)
            boxplot_metric(
                box_plot_data,
                metric=col,
                save=chart_full_path,
                by_column="study_component",
            )
            box_plots_paths.append(chart_relative_path)

    content = template.render(
        report_title=report_title,
        creation_date=datetime.datetime.now(),
        baseline_project_id=baseline_analysis_components["project_id"],
        baseline_dataset_id=baseline_analysis_components["dataset_id"],
        baseline_table_id=baseline_analysis_components["table_name"],
        baseline_conf_table=tabulate(
            baseline_analysis_components["configuration_information"].T,
            headers="keys",
            tablefmt="github",
        ),
        subject_project_id=subject_analysis_components["project_id"],
        subject_dataset_id=subject_analysis_components["dataset_id"],
        subject_table_id=subject_analysis_components["table_name"],
        subject_conf_table=tabulate(
            subject_analysis_components["configuration_information"].T,
            headers="keys",
            tablefmt="github",
        ),
        statistics_comparison_df=tabulate(
            statistics_comparison_df,
            headers="keys",
            tablefmt="github",
            floatfmt=f".{PRECISION}f",
            showindex=False,
        ),
        time_series_charts_paths=time_series_charts_paths,
        box_plots_paths=box_plots_paths,
    )

    with open(report_file_path, "w+") as f:
        f.write(content)

    generate_pdf(report_file_path)


def get_metric_wise_statistics_comparison_df(statistics_comparison_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a statistics_comparison_df with changed order of rows, so that rows showing the same
    metric (but for different resource groups) appear together.
    """
    df = pd.DataFrame(columns=statistics_comparison_df.columns)
    for metric_type in TIME_SERIES_METRIC_TYPES_ORDER:

        indexes_to_drop = []

        for index, row in statistics_comparison_df.iterrows():
            if metric_type in row["category"]:
                df = df.append(row)
                indexes_to_drop.append(index)

        statistics_comparison_df.drop(indexes_to_drop, inplace=True)

    df = pd.concat([df, statistics_comparison_df])
    df.reset_index(drop=True, inplace=True)
    return statistics_comparison_df


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

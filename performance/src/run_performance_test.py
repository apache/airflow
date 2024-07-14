"""
Script responsible for executing performance test on provided environment.
"""

import argparse
import json
import logging
import os

from performance_test import PerformanceTest

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.ERROR)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

DUMPED_JSON_ARGUMENTS = ("jinja_variables", "results_columns")
FILE_PATH_ARGUMENTS = (
    "instance_specification_file_path",
    "output_path",
)
ELASTIC_DAG_PATH = os.path.join(os.path.dirname(__file__), "performance_dags/elastic_dag/elastic_dag.py")


def get_run_performance_test_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--environment-specification-file-path",
        type=str,
        required=True,
        help="Path to the json file with specification of the environment you wish to create "
        "and test. Your file can contain jinja variables, in which case you must additionally "
        "provide their values using jinja-variables argument.",
    )
    parser.add_argument(
        "-j",
        "--jinja-variables",
        type=str,
        default=None,
        required=False,
        help="String containing dumped json with jinja variables for templated specification file. "
        "For example: if your specification file contains {{project_id}} variable, you must "
        "provide its value when running the script by adding following argument to the command: "
        '--jinja-variables \'{"project_id": "my-project"}\'',
    )
    parser.add_argument(
        "-e",
        "--elastic-dag-config-file-path",
        type=str,
        default=None,
        required=False,
        help="Path to the json file with configuration of environment variables used by "
        "the elastic dag. You can provide the environment variables for the elastic dag "
        "using this argument or directly in the main specification file (the one specified "
        "with --environment-specification-file-path). Note that values from "
        "--elastic-dag-config-file path will override any variables with the same names "
        "from the main specification file.",
    )
    parser.add_argument(
        "-c",
        "--results-columns",
        type=str,
        default=None,
        required=False,
        help="String containing dumped json with column name/value pairs that should be added "
        "to the results table. Value for any column from this argument will overwrite the column "
        "with the same name already present in the results table. "
        "New columns will be added at the beginning of the table. "
        'Example: --results-columns \'{"user": "Me"}\'',
    )
    parser.add_argument(
        "-o",
        "--output-path",
        type=str,
        default=None,
        required=False,
        help="Local path to a directory or a file where results dataframe should be saved "
        "in csv format. If path to a directory is provided, then default filename will be "
        "constructed based on environment configuration.",
    )
    parser.add_argument(
        "-r",
        "--results-object-name",
        type=str,
        default=None,
        required=False,
        help="Name to use for results object (file, GCS blob, BQ table). If not provided, then "
        "object name will generated based on the contents of results table.",
    )
    parser.add_argument(
        "--reuse-if-exists",
        action="store_true",
        default=False,
        required=False,
        help="Setting this flag will cause the script to reuse an existing environment with "
        "the same name as the one specified in your specification json file rather than recreate "
        "it. By default, the script will exit if such an environment already exists (unless it is "
        "already in deleting state - in such case it will patiently wait for this environment's "
        "deletion). If --reset-environment flag was also set, then Airflow's metadata database "
        "of the environment will be burned down and rebuilt before reusing it. Note that "
        "configuration of existing environment (including elastic DAG configuration) might differ "
        "from the one specified in configuration files. This flag takes precedence over "
        "--delete-if-exists.",
    )
    parser.add_argument(
        "--delete-if-exists",
        action="store_true",
        default=False,
        required=False,
        help="Setting this flag will cause the script to delete an existing environment with "
        "the same name as the one specified in your specification json file and then recreate it. "
        "By default, the script will exit if such an environment already exists (unless it is "
        "already in deleting state - in such case it will patiently wait for this environment's "
        "deletion).",
    )
    parser.add_argument(
        "--delete-upon-finish",
        action="store_true",
        default=False,
        required=False,
        help="Setting this flag will cause the script to delete the environment "
        "once it finishes execution. It will also cause the environment to be deleted if it's "
        "creation finished with an error which may allow for investigation of an issue.",
    )
    parser.add_argument(
        "--reset-environment",
        action="store_true",
        default=False,
        required=False,
        help="Setting this flag will result in the reset of the existing environment "
        "(currently only the refresh of Airflow metadata database). Without it, the environment "
        "will be used as is (without any cleaning operations). This may result in an unexpected "
        "behaviour. This flag has effect only if --reuse-if-exists flag was set as well.",
    )

    return parser


def main() -> None:
    args = get_run_performance_test_argument_parser().parse_args()

    perf_test = initialize_performance_test(args)

    perf_test.run_test()


def initialize_performance_test(args: argparse.Namespace) -> PerformanceTest:
    """
    Initializes an instance of PerformanceTest class using parsed arguments and returns it.
    """

    if args.jinja_variables:
        args.jinja_variables = json.loads(args.jinja_variables)

    if args.results_columns:
        args.results_columns = json.loads(args.results_columns)

    perf_test = PerformanceTest(
        instance_specification_file_path=args.instance_specification_file_path,
        elastic_dag_path=ELASTIC_DAG_PATH,
        elastic_dag_config_file_path=args.elastic_dag_config_file_path,
        jinja_variables_dict=args.jinja_variables,
        results_columns=args.results_columns,
        output_path=args.output_path,
        results_object_name=args.results_object_name,
        reuse_if_exists=args.reuse_if_exists,
        delete_if_exists=args.delete_if_exists,
        delete_upon_finish=args.delete_upon_finish,
        reset_environment=args.reset_environment,
    )

    perf_test.check_outputs()

    return perf_test


if __name__ == "__main__":
    main()

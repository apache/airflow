"""
Script responsible for executing multiple attempts of performance test for multiple configurations
in parallel.
"""

import argparse
import json
import logging
import os
import random
import tempfile
import time
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from performance_test import PerformanceTest
from run_performance_test import (
    get_run_performance_test_argument_parser,
    initialize_performance_test,
    DUMPED_JSON_ARGUMENTS,
    FILE_PATH_ARGUMENTS,
)
from environments.base_environment import (
    DEFAULT_SLEEP_BETWEEN_CHECKS,
)
from environments.kubernetes.gke.composer.composer_environment import (
    ComposerEnvironment,
)
from environments.kubernetes.gke.vanilla.vanilla_gke_environment import (
    VanillaGKEEnvironment,
)
from utils.file_utils import read_json_file

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.ERROR)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

DEFAULT_ATTEMPTS_NUMBER = 1
DEFAULT_MAX_CONCURRENCY = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    # TODO: use json schema for study file, check airflow repo in helm chart dir for inspiration
    parser.add_argument(
        "-s",
        "--study-file-path",
        type=str,
        required=True,
        help="Path to the json file containing the definition of the study, which describes "
        "the test attempts that should be run "
        "(their specification, arguments and number of attempts).",
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
        "in study args). Note that values from --elastic-dag-config-file path will override "
        "any variables with the same names from the main specification file.",
    )
    parser.add_argument(
        "-m",
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        required=False,
        help="Maximal number of test attempts this script may run at the same time. "
        "Must be a positive integer.",
    )
    parser.add_argument(
        "-u",
        "--script-user",
        type=str,
        default=None,
        required=False,
        help="Name of the user who executed this script. If provided, it will override 'user' "
        "column in the results tables (it will override 'user' entry of 'results_columns' arg "
        "for every study component).",
    )
    parser.add_argument(
        "-c",
        "--ci-build-id",
        type=str,
        default=None,
        required=False,
        help="[DEPRECATED] Id of the automatic CI build that triggered this script. If provided, "
        "it will override 'ci_build_id' column in the results tables (it will override "
        "'ci_build_id' entry of 'results_columns' arg for every study component).",
    )
    parser.add_argument(
        "-d",
        "--logs-directory",
        type=str,
        required=False,
        help="[DEPRECATED] Path to the directory in which the logs of test attempts will be stored "
        "- one log for every test attempt. This argument can also be provided in the study file.",
    )
    # parser._actions
    return parser.parse_args()


def main() -> None:

    args = parse_args()

    if args.max_concurrency < 1:
        raise ValueError("Value provided via --max-concurrency argument must be higher than 0.")

    study = read_json_file(args.study_file_path)

    study_id = datetime.utcnow().strftime("%Y%m%d_%H_%M_%S")

    # TODO: add storing logs of every PerformanceTest separately so these logs can be saved in
    #  separate location
    # study_basename = os.path.splitext(os.path.basename(args.study_file_path))[0]
    #
    # logs_directory = args.logs_directory or study.get("logs_directory", None)
    # logs_bucket = args.logs_bucket or study.get("logs_bucket", None)
    # logs_project_id = args.logs_project_id or study.get("logs_project_id", None)

    (
        study_components_by_environment_type,
        test_attempts_to_run_in_parallel,
        test_attempts_to_run_in_sequence,
    ) = collect_test_attempts(
        study, study_id, args.study_file_path, args.elastic_dag_config_file_path, args.script_user
    )

    run_test_attempts(
        test_attempts_to_run_in_parallel=test_attempts_to_run_in_parallel,
        test_attempts_to_run_in_sequence=test_attempts_to_run_in_sequence,
        max_concurrency=args.max_concurrency,
    )


# pylint: disable=too-many-locals
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# TODO: this function is way too big, need to extract some of it to separate methods
def collect_test_attempts(
    study: Dict,
    study_id: str,
    study_file_path: str,
    elastic_dag_config_file_path: str,
    script_user: Optional[str] = None,
) -> Tuple[Dict, List[Dict], List[Dict]]:
    """
    Method collects individual test attempts that should be executed
    based on the contents of study dictionary.

    :param study: dictionary describing the study.
    :type study: Dict
    :param study_id: id of the study execution.
    :type study_id: str
    :param study_file_path: path to the json file with the definition of the study.
    :type study_file_path: str
    :param elastic_dag_config_file_path: optional path to file with configuration
        for elastic DAG. Environment variables from this file will override the ones
        from the main specification file.
    :type elastic_dag_config_file_path: str
    :param script_user: user who has triggered this script.
    :type script_user: str

    :return: a tuple of a dict and two lists with test attempts:
        - dict containing information about study components grouped by their environment type
        - first list containing test attempts that can be run at the same time
        - second list with test attempt that must be run in sequence (one at a time)
    :rtype: Tuple[List[Dict], List[Dict]]

    :raises: TypeError, ValueError, KeyError - if there is some issue with specification file or
        study components
    """

    default_args = study.get("default_args", {})

    if not isinstance(default_args, Dict):
        raise TypeError(
            "'default_args' should be a dictionary with default values of arguments "
            "for every study component unless they override a specific argument."
        )

    for dumped_json_argument in DUMPED_JSON_ARGUMENTS:

        if not isinstance(default_args.get(dumped_json_argument, {}), Dict):
            raise TypeError(
                f"Please provide a default value of '{dumped_json_argument}' argument " f"as a dictionary."
            )

    default_attempts = study.get("default_attempts", DEFAULT_ATTEMPTS_NUMBER)

    if not isinstance(default_attempts, int) or default_attempts < 1:
        raise TypeError(
            "'default_attempts' should be a positive integer with the default number of times "
            "every study component should be run unless they specify their own number."
        )

    default_flags = study.get("default_flags", [])

    if not isinstance(default_flags, List):
        raise TypeError(
            "'default_flags' should be a list with the default flag type arguments to be set "
            "for every study component unless they override it with their own list."
        )

    if "study_components" not in study:
        raise ValueError(
            "Study file must contain a list under key 'study_components' "
            "with at least one environment specification to run test for."
        )

    if not isinstance(study["study_components"], List):
        raise TypeError("A list is expected under 'study_components' key.")

    if not study["study_components"]:
        raise ValueError(
            "At least one environment specification to run test for "
            "must be provided under 'study_components' key."
        )

    study_components = []
    # combine explicit arguments with the default study arguments
    for index, study_component in enumerate(study["study_components"]):

        if not isinstance(study_component, Dict):
            raise TypeError("Every element from 'study_components' list should be a dictionary.")

        if not isinstance(study_component.get("args", {}), Dict):
            raise TypeError(
                f"Please provide a dictionary under 'args' key for study component number {index}."
            )

        # overwrite default arguments with the one specified for given study component explicitly
        study_component_args = {**default_args, **study_component.get("args", {})}

        for dumped_json_argument in DUMPED_JSON_ARGUMENTS:
            # if given argument is specified explicitly for given study component
            if dumped_json_argument in study_component.get("args", {}):
                if not isinstance(study_component["args"][dumped_json_argument], Dict):
                    raise TypeError(
                        f"Please provide '{dumped_json_argument}' argument for study component "
                        f"number {index} as a dictionary. It will be converted to string "
                        f"by the script."
                    )

                # in case of dict arguments we update the dict from the 'default_args' instead
                # of overwriting it completely
                study_component_args[dumped_json_argument] = {
                    **default_args.get(dumped_json_argument, {}),
                    **study_component["args"][dumped_json_argument],
                }

            if dumped_json_argument == "results_columns" and script_user is not None:
                if dumped_json_argument not in study_component_args:
                    study_component_args[dumped_json_argument] = {}
                if script_user is not None:
                    study_component_args[dumped_json_argument]["user"] = script_user

            # dump dict argument to a string to match the expected type
            if dumped_json_argument in study_component_args:
                study_component_args[dumped_json_argument] = json.dumps(
                    study_component_args[dumped_json_argument]
                )

        study_component_attempts = study_component.get("attempts", default_attempts)

        if not isinstance(study_component_attempts, int) or study_component_attempts < 1:
            raise ValueError(
                f"Wrong number of attempts ({study_component_attempts}) for study component "
                f"number {index}: number of attempts must be a positive integer."
            )

        # if study component has its own flags specified, they overwrite the default ones completely
        study_component_flags = study_component.get("flags", default_flags)

        if not isinstance(study_component_flags, list):
            raise TypeError(f"Please provide a list under 'flags' key for study component number {index}.")

        randomize_environment_name = study_component.get("randomize_environment_name", True)

        if not isinstance(randomize_environment_name, bool):
            raise TypeError(
                f"Please provide a boolean under 'randomize_environment_name' key "
                f"for study component number {index}."
            )

        component_name = study_component.get("component_name", None)

        if component_name is not None and not isinstance(component_name, str):
            raise TypeError(
                f"Please provide a string under 'component_name' key " f"for study component number {index}."
            )

        baseline_component_name = study_component.get("baseline_component_name", None)

        if baseline_component_name is not None and not isinstance(baseline_component_name, str):
            raise TypeError(
                f"Please provide a string under 'baseline_component_name' key "
                f"for study component number {index}."
            )

        study_components.append(
            {
                "component_name": component_name,
                "baseline_component_name": baseline_component_name,
                "args": study_component_args,
                "attempts": study_component_attempts,
                "flags": study_component_flags,
                "randomize_environment_name": randomize_environment_name,
            }
        )

    study_components_by_environment_type = {}

    for index, study_component in enumerate(study_components):

        check_provided_arguments(study_component["args"], study_component["flags"], index)

        for file_argument in FILE_PATH_ARGUMENTS:
            # relative paths are treated as relative to study definition file
            if file_argument in study_component["args"] and not os.path.isabs(
                study_component["args"][file_argument]
            ):
                study_component["args"][file_argument] = os.path.join(
                    os.path.dirname(study_file_path),
                    study_component["args"][file_argument],
                )

        if not os.path.isfile(study_component["args"]["environment_specification_file_path"]):
            raise ValueError(
                f"Environment specification file "
                f"'{study_component['args']['environment_specification_file_path']}' "
                f"does not exist."
            )

        environment_type = PerformanceTest.get_environment_type(
            study_component["args"]["environment_specification_file_path"]
        )

        if environment_type not in study_components_by_environment_type:
            study_components_by_environment_type[environment_type] = []

        study_components_by_environment_type[environment_type].append(study_component)

    test_attempts_to_run_in_parallel = []
    test_attempts_to_run_in_sequence = []
    all_study_component_names = []
    all_baseline_component_names = []

    for environment_type in study_components_by_environment_type:

        if environment_type == ComposerEnvironment.environment_type:
            environment_class = ComposerEnvironment
        elif environment_type == VanillaGKEEnvironment.environment_type:
            environment_class = VanillaGKEEnvironment
        else:
            files = [
                study_component["args"]["environment_specification_file_path"]
                for study_component in study_components_by_environment_type["environment_type"]
            ]
            raise ValueError(
                f"Unknown environment type: '{environment_type}' passed "
                f"in following environment specification files: {files}"
            )

        for index, study_component in enumerate(study_components_by_environment_type[environment_type]):
            # we prepare rendered specifications for every study_component; the method decides
            # if these specifications can be run in parallel or not, based on their contents

            if not study_component.get("component_name", None):
                study_component["component_name"] = f"{environment_type}_{index}".lower()

            all_study_component_names.append(study_component["component_name"])
            baseline = study_component.get("baseline_component_name", None)

            if baseline == study_component["component_name"]:
                raise ValueError(
                    f"Baseline for study component {study_component['component_name']} cannot be the same "
                    f"as component's own name."
                )

            if baseline:
                all_baseline_component_names.append(baseline)

            (
                to_run_in_parallel,
                to_run_in_sequence,
            ) = environment_class.prepare_specifications_for_multiple_test_attempts(
                study_component["attempts"],
                study_component["args"]["environment_specification_file_path"],
                elastic_dag_config_file_path,
                json.loads(study_component["args"].get("jinja_variables", "{}")),
                study_component["randomize_environment_name"],
            )

            # since we have filled specifications already,
            # we do not need below arguments anymore
            study_component["args"].pop("environment_specification_file_path", None)
            study_component["args"].pop("jinja_variables", None)
            # remove `elastic_dag_config_file_path` parameter from study to not interfere with
            # --elastic-dag-config-file-path parameter passed to script
            study_component["args"].pop("elastic_dag_config_file_path", None)

            results_object_name = get_results_object_name(
                study_id, study_component["component_name"], environment_type
            )

            # overwriting results_object_name to make sure each component has a unique one
            study_component["args"]["results_object_name"] = results_object_name

            for specification, environment_name in to_run_in_parallel:
                parallel_test_attempt = deepcopy(study_component)
                parallel_test_attempt["specification"] = specification
                parallel_test_attempt["environment_name"] = environment_name
                parallel_test_attempt["args"]["results_object_name"] = results_object_name
                test_attempts_to_run_in_parallel.append(parallel_test_attempt)

            for specification, environment_name in to_run_in_sequence:
                sequential_test_attempt = deepcopy(study_component)
                sequential_test_attempt["specification"] = specification
                sequential_test_attempt["environment_name"] = environment_name
                sequential_test_attempt["args"]["results_object_name"] = results_object_name
                test_attempts_to_run_in_sequence.append(sequential_test_attempt)

    log.info("Test attempts collected.")

    all_environment_names = [
        test_attempt["environment_name"] for test_attempt in test_attempts_to_run_in_sequence
    ] + [test_attempt["environment_name"] for test_attempt in test_attempts_to_run_in_parallel]

    if len(all_environment_names) != len(set(all_environment_names)):
        raise ValueError(
            f"Environments about to be created do not have unique names. "
            f"Please ensure the uniqueness of environment names. Names of environments: "
            f"{all_environment_names}"
        )

    if len(all_study_component_names) != len(set(all_study_component_names)):
        raise ValueError(
            f"Study components must have unique names. Names of study components: "
            f"{all_study_component_names}"
        )

    missing_baseline_component_names = [
        baseline for baseline in all_baseline_component_names if baseline not in all_study_component_names
    ]
    if missing_baseline_component_names:
        raise ValueError(
            f"Study components with following names were provided as baselines but are missing "
            f"from study definition: {missing_baseline_component_names}."
        )

    random.shuffle(test_attempts_to_run_in_parallel)
    random.shuffle(test_attempts_to_run_in_sequence)

    return (
        study_components_by_environment_type,
        test_attempts_to_run_in_parallel,
        test_attempts_to_run_in_sequence,
    )


# pylint: disable=protected-access
def check_provided_arguments(arguments: Dict, flags: List[str], index: int) -> None:
    """
    Checks the arguments and flags of a single study component in terms of their compatibility
    with run_performance_test.py script's arguments.

    :param arguments: dictionary containing values for StoreAction type arguments
        that should be set when running test attempts of this study component.
    :type arguments: Dict
    :param flags: list of flags (destinations of StoreTrueAction/StoreFalseAction
        type arguments) that should be set when running test attempts of this study component.
    :type flags: List[str]
    :param index: integer identifying study component.
    :type index: int

    :raises:
        ValueError: if destinations of run_performance_test.py script's argument are not unique
        KeyError:
            - if arguments or flags contain actions unknown to run_performance_test.py script
            - if any of the required arguments/flags was not provided for given study component
        TypeError: if any of provided arguments does not have a value in expected type.
    """

    # list of arguments of run_performance_scripts.py
    actions = get_run_performance_test_argument_parser()._actions

    destinations = [action.dest for action in actions]

    # check for unique destinations across all arguments
    if len(destinations) != len(set(destinations)):
        raise ValueError(
            "Arguments of run_performance_scripts.py have non-unique destinations. "
            "Please check this script's arguments."
        )

    allowed_args = [action for action in actions if isinstance(action, argparse._StoreAction)]

    allowed_args_destinations = [allowed_arg.dest for allowed_arg in allowed_args]

    unknown_arguments = set(arguments) - set(allowed_args_destinations)

    if unknown_arguments:
        raise KeyError(f"Unknown arguments were provided for study component {index}: {unknown_arguments}.")

    for allowed_arg in allowed_args:
        if allowed_arg.required and allowed_arg.dest not in arguments:
            raise KeyError(
                f"One of the required arguments was not provided for study component {index}. "
                f"Please provide it in the study json file using the following string: "
                f"{allowed_arg.dest}."
            )
        if allowed_arg.dest in arguments and not isinstance(arguments[allowed_arg.dest], allowed_arg.type):
            raise TypeError(
                f"Value of argument {allowed_arg.dest} has wrong type for study component {index}. "
                f"Expected type: {allowed_arg.type}, provided value: {arguments[allowed_arg.dest]}"
            )

    allowed_flags = [
        action
        for action in actions
        if isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction))
    ]

    allowed_flags_destinations = [allowed_flag.dest for allowed_flag in allowed_flags]

    unknown_flags = set(flags) - set(allowed_flags_destinations)

    if unknown_flags:
        raise KeyError(f"Unknown flags were provided for study component {index}: {unknown_flags}.")

    for allowed_flag in allowed_flags:
        if allowed_flag.required and allowed_flag.dest not in flags:
            raise KeyError(
                f"One of the required flags was not provided for study component {index}. "
                f"Please provide it in the study json file using the following string: "
                f"{allowed_flag.dest}."
            )


def get_results_object_name(study_id: str, study_component_name: str, environment_type: str):
    """
    Returns results object name based on given arguments. It will be used to store results of
    all test attempts of given study_component.
    """
    return f"{study_id}__{environment_type}__{study_component_name}"


# pylint: disable=too-many-arguments
def run_test_attempts(
    test_attempts_to_run_in_parallel: List[Dict],
    test_attempts_to_run_in_sequence: List[Dict],
    max_concurrency: int,
) -> None:
    """
    Runs the test attempts and waits until they finish their execution.

    :param test_attempts_to_run_in_parallel: list of test attempts that can be run simultaneously.
    :type test_attempts_to_run_in_parallel: List[Dict]
    :param test_attempts_to_run_in_sequence: list of test attempts that must be run one at a time.
    :type test_attempts_to_run_in_sequence: List[Dict]
    :param max_concurrency: maximal number of test attempts of any type this method should allow
        to run at the same time.
    :type max_concurrency: int
    """

    # it is assumed that:
    #   - parallel test attempts originating from different specification files
    #   can be run at the same time (even for different environment types)
    #   - only one sequential test attempt can be run at a time, regardless of its type,
    #   but it can be run together with any parallel test attempts

    log.info(
        "Starting execution of %d sequential and %d parallel test attempt(s). "
        "Running maximum of %d test attempt(s) at at time.",
        len(test_attempts_to_run_in_sequence),
        len(test_attempts_to_run_in_parallel),
        max_concurrency,
    )

    sequential_test_attempt = None
    parallel_test_attempts = []

    with tempfile.TemporaryDirectory() as temp_dir:

        while True:

            # check the status of currently executing sequential test_attempt
            if sequential_test_attempt is not None:

                is_terminal_state = check_perf_test_status(sequential_test_attempt)

                if is_terminal_state:

                    resolve_finished_perf_test(sequential_test_attempt)
                    sequential_test_attempt = None

            # start a new sequential test attempt if the previous one has finished
            # and there are any left to start
            if sequential_test_attempt is None and test_attempts_to_run_in_sequence:
                log.info("Starting new sequential test attempt.")
                sequential_test_attempt = initialize_next_test_attempts(
                    test_attempts_to_run_in_sequence, 1, temp_dir
                )[0]

            indexes_of_finished_parallel_test_attempts = []

            # check currently executing parallel processes
            for index, parallel_test_attempt in enumerate(parallel_test_attempts):

                is_terminal_state = check_perf_test_status(parallel_test_attempt)

                if is_terminal_state:

                    resolve_finished_perf_test(parallel_test_attempt)
                    indexes_of_finished_parallel_test_attempts.append(index)

            # remove finished test attempts from list
            for index in sorted(indexes_of_finished_parallel_test_attempts, reverse=True):
                parallel_test_attempts.pop(index)

            number_of_parallel_test_attempts_to_start = max_concurrency - len(parallel_test_attempts)

            # if there are no more test attempts to run in sequence,
            # then sequential_test_attempt will remain None
            if sequential_test_attempt is not None:
                number_of_parallel_test_attempts_to_start -= 1

            # start new parallel test attempts if the concurrency allows
            # and there are any left to start
            if test_attempts_to_run_in_parallel and number_of_parallel_test_attempts_to_start > 0:

                log.info(
                    "Starting %d new parallel test attempt(s).",
                    min(
                        number_of_parallel_test_attempts_to_start,
                        len(test_attempts_to_run_in_parallel),
                    ),
                )
                parallel_test_attempts += initialize_next_test_attempts(
                    test_attempts_to_run_in_parallel,
                    number_of_parallel_test_attempts_to_start,
                    temp_dir,
                )

            number_of_running_test_attempts = (
                len(parallel_test_attempts) + 1
                if sequential_test_attempt is not None
                else len(parallel_test_attempts)
            )

            # new test attempts are initialized immediately after resolving terminated ones,
            # so there is no need to check the amount of test attempts waiting for initialization
            if number_of_running_test_attempts == 0:
                log.info("All test attempts have finished their execution.")
                break

            log.info(
                "%d test attempt(s) currently executing.",
                number_of_running_test_attempts,
            )
            if test_attempts_to_run_in_sequence:
                log.info(
                    "%d sequential test attempt(s) waiting to be initialized.",
                    len(test_attempts_to_run_in_sequence),
                )
            if test_attempts_to_run_in_parallel:
                log.info(
                    "%d parallel test attempt(s) waiting to be initialized.",
                    len(test_attempts_to_run_in_parallel),
                )
            log.info("Sleeping for %.2f seconds", DEFAULT_SLEEP_BETWEEN_CHECKS)
            time.sleep(DEFAULT_SLEEP_BETWEEN_CHECKS)


# pylint: enable=too-many-arguments


def initialize_next_test_attempts(
    test_attempts: List[Dict],
    number_of_test_attempts: int,
    directory_for_specification_files: str,
) -> List[PerformanceTest]:
    """
    Initializes the specified number of PerformanceTest instances
    using arguments from the provided list.

    :param test_attempts: list of test attempts eligible to run.
    :type test_attempts: List[Dict]
    :param number_of_test_attempts: number of test attempts that should be initialized.
    :type number_of_test_attempts: int
    :param directory_for_specification_files: temporary directory where the specifications can be
        dumped into files for the purpose of initializing PerformanceTest instances.
    :type directory_for_specification_files: str

    :return: a list of initialized PerformanceTest instances
    :rtype: List[PerformanceTest]
    """

    initialized_perf_tests = []

    while len(initialized_perf_tests) < number_of_test_attempts and test_attempts:
        next_test_attempt = test_attempts.pop()

        # we save the already rendered specification prepared for this test attempt in a file
        # and overwrite the argument responsible for path to environment specification file
        specification_file = os.path.join(
            directory_for_specification_files, next_test_attempt["environment_name"]
        )
        with open(specification_file, "w") as file:
            json.dump(next_test_attempt["specification"], file)

        next_test_attempt["args"]["environment_specification_file_path"] = specification_file
        # pylint: disable=broad-except
        try:
            test_attempt_namespace = prepare_namespace_for_test_attempt(next_test_attempt)

            perf_test = initialize_performance_test(test_attempt_namespace)

        except Exception as err:
            log.error(
                "Initializing PerformanceTest for environment %s failed with error %s. "
                "Skipping this test attempt.",
                next_test_attempt["environment_name"],
                err,
            )
            continue
        # pylint: enable=broad-except
        initialized_perf_tests.append(perf_test)

    return initialized_perf_tests


def prepare_namespace_for_test_attempt(test_attempt: Dict) -> argparse.Namespace:
    """
    Prepares a namespace with arguments for PerformanceTest class.

    :param test_attempt: a dictionary describing arguments of single test attempt.
    :type test_attempt: Dict

    :return: an argument namespace that can be used to initialize instance of PerformanceTest class.
    :rtype: argparse.Namespace
    """

    parser = get_run_performance_test_argument_parser()

    actions = parser._actions

    arguments_string = []

    for argument in test_attempt["args"]:
        option_string = find_option_string_for_destination(actions, argument)

        argument_value = str(test_attempt["args"][argument])
        arguments_string += [option_string, argument_value]

    for flag in test_attempt["flags"]:
        option_string = find_option_string_for_destination(actions, flag)
        arguments_string += [option_string]

    return parser.parse_args(arguments_string)


def find_option_string_for_destination(actions: List, destination: str) -> str:
    """
    Returns first option string of the argument matching provided destination.

    :param actions: list of argparse actions of run_performance_test.py script.
    :type actions: List
    :param destination: destination of one of run_performance_test.py script's arguments.
    :type destination: str

    :return: the found option string
    :rtype: str

    :raises: ValueError: if none of arguments matches the destination.
    """

    for action in actions:
        if action.dest == destination:
            return action.option_strings[0]
    raise ValueError(f"Could not find a destination {destination} amongst script arguments.")


def check_perf_test_status(perf_test: PerformanceTest) -> bool:
    """
    Checks status and possibly progresses given PerformanceTest.
    Returns True if PerformanceTest has reached a terminal state or an exception occurred
    and False otherwise.
    """
    # pylint: disable=broad-except
    try:
        perf_test.check_state()
        is_terminal_state = perf_test.environment.is_terminal_state
    except Exception as err:
        log.error(
            "Checking status for environment %s failed: %s.",
            perf_test.environment.name,
            err,
        )
        is_terminal_state = True
    # pylint: enable=broad-except
    return is_terminal_state


def resolve_finished_perf_test(perf_test: PerformanceTest) -> None:
    """
    Saves results of a successful test attempt and deletes it, if certain conditions have been met.
    Catches and ignores any errors.
    """
    # pylint: disable=broad-except
    try:
        log.info("Environment %s has reached a terminal state", perf_test.environment.name)
        if perf_test.environment.is_successful:
            perf_test.save_results()
    except Exception as err:
        log.error(
            "Saving results for environment %s failed: %s",
            perf_test.environment.name,
            err,
        )

    if perf_test.delete_upon_finish:
        try:
            if perf_test.environment.is_terminal_state_and_deletable:
                perf_test.environment.delete_environment()
            else:
                log.warning(
                    "delete-upon-finish flag was set, " "but the environment %s cannot be deleted.",
                    perf_test.environment.name,
                )
        except Exception as err:
            log.error("Deleting environment %s failed: %s.", perf_test.environment.name, err)
    # pylint: enable=broad-except


if __name__ == "__main__":
    main()

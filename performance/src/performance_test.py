"""
Definition of class representing a single performance attempt.
"""

import functools
import logging
from time import sleep
from typing import Dict, Optional, Sequence

from environments.base_environment import (
    State,
    DEFAULT_SLEEP_BETWEEN_CHECKS,
)
from environments.kubernetes.gke.composer.composer_environment import (
    ComposerEnvironment,
)
from environments.kubernetes.gke.vanilla.vanilla_gke_environment import (
    VanillaGKEEnvironment,
)
from utils.file_utils import (
    check_output_path,
    read_templated_json_file,
    save_output_file,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def return_default_sleep_time(method):
    """
    Decorator for check_state method of PerformanceTest class, which makes sure default sleep time
    is returned, in case one of the states does not have a sleep time assigned.
    """

    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        sleep_time = method(*args, **kwargs)
        if sleep_time is None:
            sleep_time = DEFAULT_SLEEP_BETWEEN_CHECKS
        return sleep_time

    return wrapper


# pylint: disable=too-many-instance-attributes
class PerformanceTest:
    """
    Class representing a single performance test attempt.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        instance_specification_file_path: str,
        elastic_dag_path: str,
        elastic_dag_config_file_path: str,
        jinja_variables_dict: Optional[Dict[str, str]] = None,
        results_columns: Optional[Dict] = None,
        output_path: Optional[str] = None,
        results_object_name: Optional[str] = None,
        reuse_if_exists: bool = False,
        delete_if_exists: bool = False,
        delete_upon_finish: bool = False,
        reset_environment: bool = False,
    ) -> None:
        """
        Creates an instance of PerformanceTest class.

        :param instance_specification_file_path: path to the file with specification of test
            environment.
        :type instance_specification_file_path: str
        :param elastic_dag_path: a path to elastic DAG that should be uploaded to test environment.
        :type elastic_dag_path: str
        :param elastic_dag_config_file_path: path to file with configuration for elastic DAG.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template.
        :type jinja_variables_dict: Dict[str, str]
        :param results_columns: a dictionary with column name/value pairs that should be added
            to the results table
        :type results_columns: Dict
        :param output_path: local path where results should be saved in csv format.
        :type output_path: str
        :param results_object_name: name for the results object (file or table).
        :type results_object_name: str
        :param reuse_if_exists: set to True if you want an existing environment to be reused.
        :type reuse_if_exists: bool
        :param delete_if_exists: set to True if you want an existing environment to be deleted and
            recreated.
        :type delete_if_exists: bool
        :param delete_upon_finish: set to True if you want the environment to be deleted upon
            finish.
        :type delete_upon_finish: bool
        :param reset_environment: set to True if you want an existing environment to be purged
            before reusing it.
        :type reset_environment: bool

        :raises: ValueError: if environment_type does not match any of the supported types.
        """

        self.results_object_name = results_object_name
        self.output_path = output_path
        self.delete_upon_finish = delete_upon_finish

        if self.output_path is None:
            raise ValueError(
                "No output path for the result was provided. Please provide a path to a CSV file. "
            )

        jinja_variables_dict = jinja_variables_dict or {}

        instance_type = self.get_environment_type(instance_specification_file_path)

        if instance_type == ComposerEnvironment.environment_type:
            self.environment = ComposerEnvironment(
                instance_specification_file_path,
                elastic_dag_path,
                elastic_dag_config_file_path,
                jinja_variables_dict,
                results_columns,
                reuse_if_exists,
                delete_if_exists,
                delete_upon_finish,
                reset_environment,
            )
        elif instance_type == VanillaGKEEnvironment.environment_type:
            self.environment = VanillaGKEEnvironment(
                instance_specification_file_path,
                elastic_dag_path,
                elastic_dag_config_file_path,
                jinja_variables_dict,
                results_columns,
                reuse_if_exists,
                delete_if_exists,
                delete_upon_finish,
                reset_environment,
            )
        else:
            raise ValueError(
                f"Currently only Composer and Vanilla GKE environments are supported "
                f"for performance tests (provided environment type: {instance_type})."
            )

    # pylint: enable=too-many-arguments

    @staticmethod
    def get_environment_type(instance_specification_file_path: str) -> str:
        """
        Reads the contents of the instance_specification_file_path
        and returns the environment type.

        :param instance_specification_file_path: path to the file with specification
            of test environment.
        :type instance_specification_file_path: str

        :raises:
            TypeError: if the specification is not a dictionary.
            KeyError: if the specification does not contain 'environment_type' key.
        """

        # we just want to get "environment_type" that's why we fill the templated file with nulls
        environment_specification = read_templated_json_file(
            instance_specification_file_path, fill_with_nulls=True
        )

        if not isinstance(environment_specification, Dict):
            raise TypeError("Environment specification is expected to be a dictionary.")

        if "environment_type" not in environment_specification:
            raise KeyError(
                "Environment specification file must contain 'environment_type' key "
                "with type of environment."
            )

        return environment_specification["environment_type"]

    def run_test(self) -> None:
        """
        Starts the performance test, waits until all test Dag Runs finish their execution,
        saves their results and deletes environment in the end if corresponding flag was set.
        """
        # we need to catch errors in saving results as well
        try:
            while True:
                sleep_time = self.check_state()
                if self.environment.is_terminal_state:
                    break
                sleep(sleep_time)
            if self.environment.is_successful:
                self.save_results()
        finally:
            if self.delete_upon_finish:
                try:
                    # TODO: deleting environment could be a last state, this way it would be
                    #  retryable
                    if self.environment.is_terminal_state_and_deletable:
                        self.environment.delete_environment()
                    else:
                        log.warning(
                            "delete-upon-finish flag was set, " "but the environment %s cannot be deleted.",
                            self.environment.name,
                        )
                except Exception as err:
                    log.error(
                        "Deleting environment %s failed: %s.",
                        self.environment.name,
                        err,
                    )
                    raise err

    @return_default_sleep_time
    def check_state(self) -> Optional[float]:
        """
        Checks the current state of environment and, if the state is not a terminal one,
        executes its corresponding method, possibly moving it to a new state. Optionally returns
        a suggested wait time.
        """
        state = self.environment.state
        name = self.environment.name

        log.info("%s is in state %s", name, state)
        if self.environment.is_terminal_state:
            return None

        state_method = self.environment.get_state_method()

        if state_method is None:
            log.warning("Unknown state '%s' encountered in %s", state, name)
            self.environment.state = State.FAILED
            # TODO: where FAILED state is set, use state for deleting env at the end of test instead
            return None

        try:
            self.environment.state = state_method()
            if state != self.environment.state:
                # log only when there is change to avoid noise
                log.info(
                    "%s - changing state from %s to %s",
                    name,
                    state,
                    self.environment.state,
                )
            # if everything worked, reset the number of consecutive_errors
            self.environment.consecutive_errors = 0
        except Exception as err:  # pylint: disable=broad-except
            log.exception("%s - checking state failed: %s.", name, err)
            self.environment.consecutive_errors += 1
            if self.environment.is_retryable:
                log.info(
                    "%s - attempting to retry the state %s.",
                    name,
                    self.environment.state,
                )
            else:
                log.error(
                    "%s - state %s either cannot be retried "
                    "or maximum number of consecutive errors has been reached.",
                    name,
                    self.environment.state,
                )
                # TODO: set the state to the one responsible for deleting env at the end of test,
                #  unless it is already the current state, then raise err
                self.environment.state = State.FAILED
                raise err
        return self.environment.get_state_wait_time()

    def check_outputs(self) -> None:
        """
        Checks if provided ways of saving test results can be accessed by the script.
        Currently the result can be only saved to a CSV file to `output_path` folder.
        """

        if self.output_path is not None:
            log.info("Checking output path: %s", self.output_path)
            check_output_path(self.output_path)

    def save_results(self) -> None:
        """
        Saves the performance test results in local file.
        Returns immediately if none of them was provided.
        """

        log.info("Saving the results for environment %s", self.environment.name)

        results_df, results_object_name_components = self.environment.results

        results_object_name = self.results_object_name or self.get_results_object_name(
            results_object_name_components
        )

        if self.output_path is not None:
            save_output_file(
                results_df=results_df,
                output_path=self.output_path,
                default_file_name=results_object_name,
            )

    @staticmethod
    def get_results_object_name(results_object_name_components: Sequence[str]) -> str:
        """
        Gets the name for the results object (file, GCS blob, BQ table) based on contents
        of the results dataframe.

        :param results_object_name_components: a sequence of strings which should be combined
            to create the name of the object where the results will be saved.
        :type results_object_name_components: Sequence[str]

        :return: name of the object in which results should be stored.
        :rtype: str
        """

        results_object_name = "__".join(results_object_name_components)
        # replacing periods and dashes as these characters are not allowed in Big Query table names
        results_object_name = results_object_name.replace(".", "_").replace("-", "_")

        return results_object_name

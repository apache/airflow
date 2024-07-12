"""
This module contains the definition of an abstract class representing base requirements for
environment needed in order to run performance tests on it.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Dict, List, NamedTuple, Optional, Sequence, Tuple, Union

from pandas import DataFrame

FINISHED_DAG_RUN_STATES = ["success", "failed"]
MAX_CONSECUTIVE_ERRORS = 10
DEFAULT_SLEEP_BETWEEN_CHECKS = 30.0


def is_state(state: str, expected_states: Union[Enum, Tuple[Enum, ...]]) -> bool:
    """
    Checks if provided state is equal to any of expected_states.

    :param state: string with the state of environment or cluster.
    :type state: str
    :param expected_states: a single instance or a tuple of Enum instances to compare
        the state with.
    :type expected_states: Union[Enum, Tuple[Enum, ...]

    :return: True if state matches at least one value from expected_states and False otherwise.
    :rtype: bool
    """
    if isinstance(expected_states, Enum):
        expected_states = (expected_states,)

    return any(state == expected_state.value for expected_state in expected_states)


class State(Enum):
    NONE = "NONE"
    WAIT_UNTIL_READY = "WAIT_UNTIL_READY"
    WAIT_UNTIL_CAN_BE_DELETED = "WAIT_UNTIL_CAN_BE_DELETED"
    DELETING_ENV = "DELETING_ENV"
    UPDATE_ENV_INFO = "UPDATE_ENV_INFO"
    DEPLOY_STATS_COLLECTOR = "DEPLOY_STATS_COLLECTOR"
    RESET_ENV = "RESET_ENV"
    UPLOAD_DAG = "UPLOAD_DAG"
    WAIT_FOR_DAG = "WAIT_FOR_DAG"
    UNPAUSE_DAG = "UNPAUSE_DAG"
    WAIT_FOR_DAG_RUN_EXEC = "WAIT_FOR_DAG_RUN_EXEC"
    COLLECT_RESULTS = "COLLECT_RESULTS"
    DONE = "DONE"
    FAILED = "FAILED"
    FAILED_DO_NOT_DELETE = "FAILED_DO_NOT_DELETE"


class Action(NamedTuple):
    """
    A NamedTuple, which defines an action that should be executed to progress the performance test.
    This named tuple consists of:
        method - class method that must return the next state of the environment
            (which can be the same state it is currently at). This method will be called
            without providing any arguments.
        sleep_time - float with proposed sleep time while waiting for the action to finish.
        retryable - boolean telling whether action can be retried or not.
    """

    method: Optional[Callable] = None
    sleep_time: Optional[float] = DEFAULT_SLEEP_BETWEEN_CHECKS
    retryable: bool = False


class BaseEnvironment(ABC):
    """
    An abstract class defining an interface for an environment that supports running performance
    tests on it.

    :param state: current state of test environment.
    :type state: State
    :param consecutive_errors: current number of consecutive errors. Should be reset if
        a state method has been executed successfully.
    :type consecutive_errors: int
    :param results: a tuple consisting of:
        - pandas Dataframe containing information about tested environment configuration and
        performance metrics
        - a sequence of strings which should be combined to create the name of the object
        where the results will be saved (for example a file, GCS blob or BQ table)
    :type results: Optional[Tuple[DataFrame, Sequence[str]]]
    """

    terminal_states = {State.DONE, State.FAILED, State.FAILED_DO_NOT_DELETE}
    terminal_states_success = {State.DONE}
    terminal_states_deletable = {State.DONE, State.FAILED}

    def __init__(self):
        self.state = State.NONE
        self.consecutive_errors = 0
        self.results: Optional[Tuple[DataFrame, Sequence[str]]] = None

    @property
    @abstractmethod
    def states_map(self) -> Dict[State, Action]:
        """
        This method should return a map that for every applicable state holds an instance of Action
        class.
        """
        pass

    @property
    def is_terminal_state(self) -> bool:
        """
        Returns True if environment is has finished execution of performance test.
        """
        return self.state in self.terminal_states

    @property
    def is_retryable(self) -> bool:
        """
        Returns True if the retry attempt can be made and False otherwise.
        """
        return self.consecutive_errors < MAX_CONSECUTIVE_ERRORS and self.is_in_retryable_state

    @property
    def is_in_retryable_state(self) -> bool:
        """
        Returns True if environment is in a state that can be retried.
        """
        return self.state in self.states_map and self.states_map[self.state].retryable

    @property
    def is_successful(self) -> bool:
        """
        Returns True if environment is has finished performance test with a success.
        """
        return self.state in self.terminal_states_success

    @property
    @abstractmethod
    def is_terminal_state_and_deletable(self) -> bool:
        """
        This method should return True if environment is in one of its terminal states and it is
        possible to delete it.
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """
        This method should return the environment name.
        """
        pass

    def get_state_method(self) -> Optional[Callable]:
        """
        Returns method assigned to given state, if any.
        """

        return self.states_map[self.state].method if self.state in self.states_map else None

    def get_state_wait_time(self) -> Optional[float]:
        """
        Returns wait time assigned to given state, if any.
        """
        return self.states_map[self.state].sleep_time if self.state in self.states_map else None

    # pylint: disable=too-many-arguments
    @classmethod
    @abstractmethod
    def prepare_specifications_for_multiple_test_attempts(
        cls,
        number_of_copies: int,
        instance_specification_file_path: str,
        elastic_dag_config_file_path: Optional[str] = None,
        jinja_variables_dict: Optional[Dict[str, str]] = None,
        randomize_environment_name: bool = True,
    ) -> Tuple[List[Tuple[Dict, str]], List[Tuple[Dict, str]]]:
        """
        This method should load the specification using provided arguments and duplicate it into
        number_of_copies duplicates, preferably in such a manner so that these duplicates can be
        run in parallel (provided that's possible).

        :param number_of_copies: number of copies of the specification that should be prepared.
        :type number_of_copies: int
        :param instance_specification_file_path: path to the file with specification
            of environment to duplicate.
        :type instance_specification_file_path: str
        :param elastic_dag_config_file_path: optional path to file with configuration
            for elastic DAG. Environment variables from this file will override the ones
            from instance_specification_file_path.
        :type elastic_dag_config_file_path: str
        :param jinja_variables_dict: a dictionary with values for jinja variables to use
            when filling the templated specification file. Must contain all variables
            present in the template which do not have default values defined.
        :type jinja_variables_dict: Dict[str, str]
        :param randomize_environment_name: if set to True, then a random part will be added to
            environment name of every specification copy.
        :type randomize_environment_name: bool

        :return: a tuple of two lists. Both lists must contain pairs consisting of specification
            dicts and environment names:
            - first list contains specifications that can be run in parallel
            - second list contains specifications that should be run sequentially
        :rtype: Tuple[List[Tuple[Dict, str]], List[Tuple[Dict, str]]]
        """
        pass

    # pylint: enable=too-many-arguments

    @abstractmethod
    def delete_environment(self) -> None:
        """
        This method should delete the test environment.
        """
        pass

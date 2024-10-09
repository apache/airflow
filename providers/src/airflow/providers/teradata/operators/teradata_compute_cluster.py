#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import re
from abc import abstractmethod
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.utils.constants import Constants

if TYPE_CHECKING:
    from airflow.utils.context import Context

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Sequence, cast

from airflow.providers.teradata.triggers.teradata_compute_cluster import TeradataComputeClusterSyncTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.exceptions import AirflowException


# Represents
# 1. Compute Cluster Setup - Provision and Decomission operations
# 2. Compute Cluster State - Resume and Suspend operations
class _Operation(Enum):
    SETUP = 1
    STATE = 2


# Handler to handle single result set of a SQL query
def _single_result_row_handler(cursor):
    records = cursor.fetchone()
    if isinstance(records, list):
        return records[0]
    if records is None:
        return records
    raise TypeError(f"Unexpected results: {cursor.fetchone()!r}")


# Providers given operation is setup or state operation
def _determine_operation_context(operation):
    if operation == Constants.CC_CREATE_OPR or operation == Constants.CC_DROP_OPR:
        return _Operation.SETUP
    return _Operation.STATE


class _TeradataComputeClusterOperator(BaseOperator):
    """
    Teradata Compute Cluster Base Operator to set up and status operations of compute cluster.

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param teradata_conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails.
    """

    template_fields: Sequence[str] = (
        "compute_profile_name",
        "compute_group_name",
        "teradata_conn_id",
        "timeout",
    )

    ui_color = "#e07c24"

    def __init__(
        self,
        compute_profile_name: str,
        compute_group_name: str | None = None,
        teradata_conn_id: str = TeradataHook.default_conn_name,
        timeout: int = Constants.CC_OPR_TIME_OUT,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.compute_profile_name = compute_profile_name
        self.compute_group_name = compute_group_name
        self.teradata_conn_id = teradata_conn_id
        self.timeout = timeout

    @cached_property
    def hook(self) -> TeradataHook:
        return TeradataHook(teradata_conn_id=self.teradata_conn_id)

    @abstractmethod
    def execute(self, context: Context):
        pass

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        self._compute_cluster_execute_complete(event)

    def _compute_cluster_execute(self):
        # Verifies the provided compute profile name.
        if (
            self.compute_profile_name is None
            or self.compute_profile_name == "None"
            or self.compute_profile_name == ""
        ):
            self.log.info("Invalid compute cluster profile name")
            raise AirflowException(Constants.CC_OPR_EMPTY_PROFILE_ERROR_MSG)
        # Verifies if the provided Teradata instance belongs to Vantage Cloud Lake.
        lake_support_find_sql = "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'"
        lake_support_result = self.hook.run(lake_support_find_sql, handler=_single_result_row_handler)
        if lake_support_result is None:
            raise AirflowException(Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)
        # Getting teradata db version. Considering teradata instance is Lake when db version is 20 or above
        db_version_get_sql = "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'"
        try:
            db_version_result = self.hook.run(db_version_get_sql, handler=_single_result_row_handler)
            if db_version_result is not None:
                db_version_result = str(db_version_result)
                db_version = db_version_result.split(".")[0]
                if db_version is not None and int(db_version) < 20:
                    raise AirflowException(Constants.CC_GRP_LAKE_SUPPORT_ONLY_MSG)
            else:
                raise AirflowException("Error occurred while getting teradata database version")
        except Exception as ex:
            self.log.error("Error occurred while getting teradata database version: %s ", str(ex))
            raise AirflowException("Error occurred while getting teradata database version")

    def _compute_cluster_execute_complete(self, event: dict[str, Any]) -> None:
        if event["status"] == "success":
            return event["message"]
        elif event["status"] == "error":
            raise AirflowException(event["message"])

    def _handle_cc_status(self, operation_type, sql):
        create_sql_result = self._hook_run(sql, handler=_single_result_row_handler)
        self.log.info(
            "%s query ran successfully. Differing to trigger to check status in db. Result from sql: %s",
            operation_type,
            create_sql_result,
        )
        self.defer(
            timeout=timedelta(minutes=self.timeout),
            trigger=TeradataComputeClusterSyncTrigger(
                teradata_conn_id=cast(str, self.teradata_conn_id),
                compute_profile_name=self.compute_profile_name,
                compute_group_name=self.compute_group_name,
                operation_type=operation_type,
                poll_interval=Constants.CC_POLL_INTERVAL,
            ),
            method_name="execute_complete",
        )

        return create_sql_result

    def _hook_run(self, query, handler=None):
        try:
            if handler is not None:
                return self.hook.run(query, handler=handler)
            else:
                return self.hook.run(query)
        except Exception as ex:
            self.log.error(str(ex))
            raise

    def _get_initially_suspended(self, create_cp_query):
        initially_suspended = "FALSE"
        pattern = r"INITIALLY_SUSPENDED\s*\(\s*'(TRUE|FALSE)'\s*\)"
        # Search for the pattern in the input string
        match = re.search(pattern, create_cp_query, re.IGNORECASE)
        if match:
            # Get the value of INITIALLY_SUSPENDED
            initially_suspended = match.group(1).strip().upper()
        return initially_suspended


class TeradataComputeClusterProvisionOperator(_TeradataComputeClusterOperator):
    """

    Creates the new Computer Cluster with specified Compute Group Name and Compute Profile Name.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterProvisionOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param query_strategy: Query strategy to use. Refers to the approach or method used by the
        Teradata Optimizer to execute SQL queries efficiently within a Teradata computer cluster.
        Valid query_strategy value is either 'STANDARD' or 'ANALYTIC'. Default at database level is STANDARD.
    :param compute_map: ComputeMapName of the compute map. The compute_map in a compute cluster profile refers
        to the mapping of compute resources to a specific node or set of nodes within the cluster.
    :param compute_attribute: Optional attributes of compute profile. Example compute attribute
        MIN_COMPUTE_COUNT(1) MAX_COMPUTE_COUNT(5) INITIALLY_SUSPENDED('FALSE')
    :param teradata_conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails.
    """

    template_fields: Sequence[str] = (
        "compute_profile_name",
        "compute_group_name",
        "query_strategy",
        "compute_map",
        "compute_attribute",
        "teradata_conn_id",
        "timeout",
    )

    ui_color = "#e07c24"

    def __init__(
        self,
        query_strategy: str | None = None,
        compute_map: str | None = None,
        compute_attribute: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query_strategy = query_strategy
        self.compute_map = compute_map
        self.compute_attribute = compute_attribute

    def _build_ccp_setup_query(self):
        create_cp_query = "CREATE COMPUTE PROFILE " + self.compute_profile_name
        if self.compute_group_name:
            create_cp_query = create_cp_query + " IN " + self.compute_group_name
        if self.compute_map is not None:
            create_cp_query = create_cp_query + ", INSTANCE = " + self.compute_map
        if self.query_strategy is not None:
            create_cp_query = create_cp_query + ", INSTANCE TYPE = " + self.query_strategy
        if self.compute_attribute is not None:
            create_cp_query = create_cp_query + " USING " + self.compute_attribute
        return create_cp_query

    def execute(self, context: Context):
        """
        Initiate the execution of CREATE COMPUTE SQL statement.

        Initiate the execution of the SQL statement for provisioning the compute cluster within Teradata Vantage
        Lake, effectively creates the compute cluster.
        Airflow runs this method on the worker and defers using the trigger.
        """
        super().execute(context)
        return self._compute_cluster_execute()

    def _compute_cluster_execute(self):
        super()._compute_cluster_execute()
        if self.compute_group_name:
            cg_status_query = (
                "SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('"
                + self.compute_group_name
                + "')"
            )
            cg_status_result = self._hook_run(cg_status_query, _single_result_row_handler)
            if cg_status_result is not None:
                cg_status_result = str(cg_status_result)
            else:
                cg_status_result = 0
            if int(cg_status_result) == 0:
                create_cg_query = "CREATE COMPUTE GROUP " + self.compute_group_name
                if self.query_strategy is not None:
                    create_cg_query = (
                        create_cg_query + " USING QUERY_STRATEGY ('" + self.query_strategy + "')"
                    )
                self._hook_run(create_cg_query, _single_result_row_handler)
        cp_status_query = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('"
            + self.compute_profile_name
            + "')"
        )
        if self.compute_group_name:
            cp_status_query += " AND UPPER(ComputeGroupName) = UPPER('" + self.compute_group_name + "')"
        cp_status_result = self._hook_run(cp_status_query, handler=_single_result_row_handler)
        if cp_status_result is not None:
            cp_status_result = str(cp_status_result)
            msg = f"Compute Profile {self.compute_profile_name} is already exists under Compute Group {self.compute_group_name}. Status is {cp_status_result}"
            self.log.info(msg)
            return cp_status_result
        else:
            create_cp_query = self._build_ccp_setup_query()
            operation = Constants.CC_CREATE_OPR
            initially_suspended = self._get_initially_suspended(create_cp_query)
            if initially_suspended == "TRUE":
                operation = Constants.CC_CREATE_SUSPEND_OPR
            return self._handle_cc_status(operation, create_cp_query)


class TeradataComputeClusterDecommissionOperator(_TeradataComputeClusterOperator):
    """
    Drops the compute cluster with specified Compute Group Name and Compute Profile Name.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterDecommissionOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param delete_compute_group: Indicates whether the compute group should be deleted.
        When set to True, it signals the system to remove the specified compute group.
        Conversely, when set to False, no action is taken on the compute group.
    :param teradata_conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails.
    """

    template_fields: Sequence[str] = (
        "compute_profile_name",
        "compute_group_name",
        "delete_compute_group",
        "teradata_conn_id",
        "timeout",
    )

    ui_color = "#e07c24"

    def __init__(
        self,
        delete_compute_group: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.delete_compute_group = delete_compute_group

    def execute(self, context: Context):
        """
        Initiate the execution of DROP COMPUTE SQL statement.

        Initiate the execution of the SQL statement for decommissioning the compute cluster within Teradata Vantage
        Lake, effectively drops the compute cluster.
        Airflow runs this method on the worker and defers using the trigger.
        """
        super().execute(context)
        return self._compute_cluster_execute()

    def _compute_cluster_execute(self):
        super()._compute_cluster_execute()
        cp_drop_query = "DROP COMPUTE PROFILE " + self.compute_profile_name
        if self.compute_group_name:
            cp_drop_query = cp_drop_query + " IN COMPUTE GROUP " + self.compute_group_name
        self._hook_run(cp_drop_query, handler=_single_result_row_handler)
        self.log.info(
            "Compute Profile %s IN Compute Group %s is successfully dropped",
            self.compute_profile_name,
            self.compute_group_name,
        )
        if self.delete_compute_group:
            cg_drop_query = "DROP COMPUTE GROUP " + self.compute_group_name
            self._hook_run(cg_drop_query, handler=_single_result_row_handler)
            self.log.info("Compute Group %s is successfully dropped", self.compute_group_name)


class TeradataComputeClusterResumeOperator(_TeradataComputeClusterOperator):
    """
    Teradata Compute Cluster Operator to Resume the specified Teradata Vantage Cloud Lake Compute Cluster.

    Resumes the Teradata Vantage Lake Computer Cluster by employing the RESUME SQL statement within the
    Teradata Vantage Lake Compute Cluster SQL Interface.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterResumeOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param teradata_conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails. Time is in minutes.
    """

    template_fields: Sequence[str] = (
        "compute_profile_name",
        "compute_group_name",
        "teradata_conn_id",
        "timeout",
    )

    ui_color = "#e07c24"

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context):
        """
        Initiate the execution of RESUME COMPUTE SQL statement.

        Initiate the execution of the SQL statement for resuming the compute cluster within Teradata Vantage
        Lake, effectively resumes the compute cluster.
        Airflow runs this method on the worker and defers using the trigger.
        """
        super().execute(context)
        return self._compute_cluster_execute()

    def _compute_cluster_execute(self):
        super()._compute_cluster_execute()
        cc_status_query = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('"
            + self.compute_profile_name
            + "')"
        )
        if self.compute_group_name:
            cc_status_query += " AND UPPER(ComputeGroupName) = UPPER('" + self.compute_group_name + "')"
        cc_status_result = self._hook_run(cc_status_query, handler=_single_result_row_handler)
        if cc_status_result is not None:
            cp_status_result = str(cc_status_result)
        # Generates an error message if the compute cluster does not exist for the specified
        # compute profile and compute group.
        else:
            self.log.info(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
            raise AirflowException(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
        if cp_status_result != Constants.CC_RESUME_DB_STATUS:
            cp_resume_query = f"RESUME COMPUTE FOR COMPUTE PROFILE {self.compute_profile_name}"
            if self.compute_group_name:
                cp_resume_query = f"{cp_resume_query} IN COMPUTE GROUP {self.compute_group_name}"
            return self._handle_cc_status(Constants.CC_RESUME_OPR, cp_resume_query)
        else:
            self.log.info(
                "Compute Cluster %s already %s", self.compute_profile_name, Constants.CC_RESUME_DB_STATUS
            )


class TeradataComputeClusterSuspendOperator(_TeradataComputeClusterOperator):
    """
    Teradata Compute Cluster Operator to suspend the specified Teradata Vantage Cloud Lake Compute Cluster.

    Suspends the Teradata Vantage Lake Computer Cluster by employing the SUSPEND SQL statement within the
    Teradata Vantage Lake Compute Cluster SQL Interface.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataComputeClusterSuspendOperator`

    :param compute_profile_name: Name of the Compute Profile to manage.
    :param compute_group_name: Name of compute group to which compute profile belongs.
    :param teradata_conn_id: The :ref:`Teradata connection id <howto/connection:teradata>`
        reference to a specific Teradata database.
    :param timeout: Time elapsed before the task times out and fails.
    """

    template_fields: Sequence[str] = (
        "compute_profile_name",
        "compute_group_name",
        "teradata_conn_id",
        "timeout",
    )

    ui_color = "#e07c24"

    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context):
        """
        Initiate the execution of SUSPEND COMPUTE SQL statement.

        Initiate the execution of the SQL statement for suspending the compute cluster within Teradata Vantage
        Lake, effectively suspends the compute cluster.
        Airflow runs this method on the worker and defers using the trigger.
        """
        super().execute(context)
        return self._compute_cluster_execute()

    def _compute_cluster_execute(self):
        super()._compute_cluster_execute()
        sql = (
            "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('"
            + self.compute_profile_name
            + "')"
        )
        if self.compute_group_name:
            sql += " AND UPPER(ComputeGroupName) = UPPER('" + self.compute_group_name + "')"
        result = self._hook_run(sql, handler=_single_result_row_handler)
        if result is not None:
            result = str(result)
        # Generates an error message if the compute cluster does not exist for the specified
        # compute profile and compute group.
        else:
            self.log.info(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
            raise AirflowException(Constants.CC_GRP_PRP_NON_EXISTS_MSG)
        if result != Constants.CC_SUSPEND_DB_STATUS:
            sql = f"SUSPEND COMPUTE FOR COMPUTE PROFILE {self.compute_profile_name}"
            if self.compute_group_name:
                sql = f"{sql} IN COMPUTE GROUP {self.compute_group_name}"
            return self._handle_cc_status(Constants.CC_SUSPEND_OPR, sql)
        else:
            self.log.info(
                "Compute Cluster %s already %s", self.compute_profile_name, Constants.CC_SUSPEND_DB_STATUS
            )

from __future__ import annotations

import json
from typing import Any

import structlog

from airflow.sdk.execution_time.comms import SetXCom, GetXCom, XComResult
from airflow.utils.json import XComEncoder, XComDecoder

log = structlog.get_logger(logger_name="task")


class BaseXCom:
    @classmethod
    def set(
        cls,
        key: str,
        value: Any,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int = -1,
    ) -> None:
        """
        Store an XCom value.

        :param key: Key to store the XCom.
        :param value: XCom value to store.
        :param dag_id: DAG ID.
        :param task_id: Task ID.
        :param run_id: DAG run ID for the task.
        :param map_index: Optional map index to assign XCom for a mapped task.
            The default is ``-1`` (set for a non-mapped task).
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS
        value = cls.serialize_value(
            value=value,
            key=key,
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            map_index=map_index,
        )

        # TODO: call delete api here
        # session.execute(
        #     delete(cls).where(
        #         cls.key == key,
        #         cls.run_id == run_id,
        #         cls.task_id == task_id,
        #         cls.dag_id == dag_id,
        #         cls.map_index == map_index,
        #     )
        # )

        SUPERVISOR_COMMS.send_request(
            log=log,
            msg=SetXCom(
                key=key,
                value=value,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
            ),
        )

    # @staticmethod
    # def get_value(
    #     *,
    #     ti_key: TaskInstanceKey,
    #     key: str | None = None,
    # ) -> Any:
    #     """
    #     Retrieve an XCom value for a task instance.
    #
    #     This method returns "full" XCom values (i.e. uses ``deserialize_value``
    #     from the XCom backend). Use :meth:`get_many` if you want the "shortened"
    #     value via ``orm_deserialize_value``.
    #
    #     If there are no results, *None* is returned. If multiple XCom entries
    #     match the criteria, an arbitrary one is returned.
    #
    #     :param ti_key: The TaskInstanceKey to look up the XCom for.
    #     :param key: A key for the XCom. If provided, only XCom with matching
    #         keys will be returned. Pass *None* (default) to remove the filter.
    #     :param session: Database session. If not given, a new session will be
    #         created for this function.
    #     """
    #     return BaseXCom.get_one(
    #         key=key,
    #         task_id=ti_key.task_id,
    #         dag_id=ti_key.dag_id,
    #         run_id=ti_key.run_id,
    #         map_index=ti_key.map_index,
    #         session=session,
    #     )

    @staticmethod
    def get_one(
        *,
        key: str | None = None,
        dag_id: str | None = None,
        task_id: str | None = None,
        run_id: str,
        map_index: int | None = None,
        include_prior_dates: bool = False,
    ) -> Any | None:
        """
        Retrieve an XCom value, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend). Use :meth:`get_many` if you want the "shortened"
        value via ``orm_deserialize_value``.

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        .. seealso:: ``get_value()`` is a convenience function if you already
            have a structured TaskInstance or TaskInstanceKey object available.

        :param run_id: DAG run ID for the task.
        :param dag_id: Only pull XCom from this DAG. Pass *None* (default) to
            remove the filter.
        :param task_id: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param map_index: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        :param include_prior_dates: If *False* (default), only XCom from the
            specified DAG run is returned. If *True*, the latest matching XCom is
            returned regardless of the run it belongs to.
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS
        SUPERVISOR_COMMS.send_request(
            log=log,
            msg=GetXCom(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
            ),
        )

        msg = SUPERVISOR_COMMS.get_message()
        if not isinstance(msg, XComResult):
            raise TypeError(f"Expected XComResult, received: {type(msg)} {msg}")

        if msg.value is not None:
            from airflow.serialization.serde import deserialize

            # TODO: Move XCom serialization & deserialization to Task SDK
            #   https://github.com/apache/airflow/issues/45231

            # The execution API server deals in json compliant types now.
            # serde's deserialize can handle deserializing primitive, collections, and complex objects too
            print("I am retruning XCom.", BaseXCom.deserialize_value(msg.value))
            return deserialize(msg.value)

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> str:
        """Serialize XCom value to JSON str."""
        try:
            return json.dumps(value, cls=XComEncoder)
        except (ValueError, TypeError):
            raise ValueError("XCom value must be JSON serializable")

    @staticmethod
    def _deserialize_value(result, orm: bool) -> Any:
        object_hook = None
        if orm:
            object_hook = XComDecoder.orm_object_hook

        if result.value is None:
            return None

        return json.loads(result.value, cls=XComDecoder, object_hook=object_hook)

    @staticmethod
    def deserialize_value(result) -> Any:
        """Deserialize XCom value from str or pickle object."""
        return BaseXCom._deserialize_value(result, False)

    def orm_deserialize_value(self) -> Any:
        """
        Deserialize method which is used to reconstruct ORM XCom object.

        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom orm model. This is used when viewing XCom listing
        in the webserver, for example.
        """
        return BaseXCom._deserialize_value(self, True)

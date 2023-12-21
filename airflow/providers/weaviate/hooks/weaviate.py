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

import contextlib
import json
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, List, cast

import requests
import weaviate.exceptions
from tenacity import Retrying, retry, retry_if_exception, retry_if_exception_type, stop_after_attempt
from weaviate import Client as WeaviateClient
from weaviate.auth import AuthApiKey, AuthBearerToken, AuthClientCredentials, AuthClientPassword
from weaviate.exceptions import ObjectAlreadyExistsException
from weaviate.util import generate_uuid5

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from typing import Callable, Collection, Literal, Sequence

    import pandas as pd
    from weaviate import ConsistencyLevel
    from weaviate.types import UUID

    ExitingSchemaOptions = Literal["replace", "fail", "ignore"]

HTTP_RETRY_STATUS_CODE = [429, 500, 503, 504]
REQUESTS_EXCEPTIONS_TYPES = (
    requests.RequestException,
    requests.exceptions.ConnectionError,
    requests.exceptions.HTTPError,
    requests.exceptions.ConnectTimeout,
)


def check_http_error_is_retryable(exc: BaseException):
    return (
        isinstance(exc, requests.exceptions.RequestException)
        and exc.response
        and exc.response.status_code
        and exc.response.status_code in HTTP_RETRY_STATUS_CODE
    )


class WeaviateHook(BaseHook):
    """
    Interact with Weaviate database to store vectors. This hook uses the `conn_id`.

    :param conn_id: The connection id to use when connecting to Weaviate. <howto/connection:weaviate>
    """

    conn_name_attr = "conn_id"
    default_conn_name = "weaviate_default"
    conn_type = "weaviate"
    hook_name = "Weaviate"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        retry_status_codes: list[int] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField

        return {
            "token": PasswordField(lazy_gettext("Weaviate API Key"), widget=BS3PasswordFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {
                "login": "OIDC Username",
                "password": "OIDC Password",
            },
        }

    def get_conn(self) -> WeaviateClient:
        conn = self.get_connection(self.conn_id)
        url = conn.host
        username = conn.login or ""
        password = conn.password or ""
        extras = conn.extra_dejson
        access_token = extras.get("access_token", None)
        refresh_token = extras.get("refresh_token", None)
        expires_in = extras.get("expires_in", 60)
        # previously token was used as api_key(backwards compatibility)
        api_key = extras.get("api_key", None) or extras.get("token", None)
        client_secret = extras.get("client_secret", None)
        additional_headers = extras.pop("additional_headers", {})
        scope = extras.get("scope", None) or extras.get("oidc_scope", None)
        if api_key:
            auth_client_secret = AuthApiKey(api_key)
        elif access_token:
            auth_client_secret = AuthBearerToken(
                access_token, expires_in=expires_in, refresh_token=refresh_token
            )
        elif client_secret:
            auth_client_secret = AuthClientCredentials(client_secret=client_secret, scope=scope)
        else:
            auth_client_secret = AuthClientPassword(username=username, password=password, scope=scope)

        return WeaviateClient(
            url=url, auth_client_secret=auth_client_secret, additional_headers=additional_headers
        )

    @cached_property
    def conn(self) -> WeaviateClient:
        """Returns a Weaviate client."""
        return self.get_conn()

    def get_client(self) -> WeaviateClient:
        """Returns a Weaviate client."""
        # Keeping this for backwards compatibility
        warnings.warn(
            "The `get_client` method has been renamed to `get_conn`",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return self.conn

    def test_connection(self) -> tuple[bool, str]:
        try:
            client = self.conn
            client.schema.get()
            return True, "Connection established!"
        except Exception as e:
            self.log.error("Error testing Weaviate connection: %s", e)
            return False, str(e)

    def create_class(self, class_json: dict[str, Any]) -> None:
        """Create a new class."""
        client = self.conn
        client.schema.create_class(class_json)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception(lambda exc: check_http_error_is_retryable(exc))
            | retry_if_exception_type(REQUESTS_EXCEPTIONS_TYPES)
        ),
    )
    def create_schema(self, schema_json: dict[str, Any] | str) -> None:
        """
        Create a new Schema.

        Instead of adding classes one by one , you can upload a full schema in JSON format at once.

        :param schema_json: Schema as a Python dict or the path to a JSON file, or the URL of a JSON file.
        """
        client = self.conn
        client.schema.create(schema_json)

    @staticmethod
    def _convert_dataframe_to_list(data: list[dict[str, Any]] | pd.DataFrame) -> list[dict[str, Any]]:
        """Helper function to convert dataframe to list of dicts.

        In scenario where Pandas isn't installed and we pass data as a list of dictionaries, importing
        Pandas will fail, which is invalid. This function handles this scenario.
        """
        with contextlib.suppress(ImportError):
            import pandas

            if isinstance(data, pandas.DataFrame):
                data = json.loads(data.to_json(orient="records"))
        return cast(List[Dict[str, Any]], data)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception(lambda exc: check_http_error_is_retryable(exc))
            | retry_if_exception_type(REQUESTS_EXCEPTIONS_TYPES)
        ),
    )
    def get_schema(self, class_name: str | None = None):
        """Get the schema from Weaviate.

        :param class_name: The class for which to return the schema. If NOT provided the whole schema is
            returned, otherwise only the schema of this class is returned. By default None.
        """
        client = self.get_client()
        return client.schema.get(class_name)

    def delete_classes(self, class_names: list[str] | str, if_error: str = "stop") -> list[str] | None:
        """Deletes all or specific classes if class_names are provided.

        :param class_names: list of class names to be deleted.
        :param if_error: define the actions to be taken if there is an error while deleting a class, possible
         options are `stop` and `continue`
        :return: if `if_error=continue` return list of classes which we failed to delete.
            if `if_error=stop` returns None.
        """
        client = self.get_client()
        class_names = [class_names] if class_names and isinstance(class_names, str) else class_names

        failed_class_list = []
        for class_name in class_names:
            try:
                for attempt in Retrying(
                    stop=stop_after_attempt(3),
                    retry=(
                        retry_if_exception(lambda exc: check_http_error_is_retryable(exc))
                        | retry_if_exception_type(REQUESTS_EXCEPTIONS_TYPES)
                    ),
                ):
                    with attempt:
                        print(attempt)
                        client.schema.delete_class(class_name)
            except Exception as e:
                if if_error == "continue":
                    self.log.error(e)
                    failed_class_list.append(class_name)
                elif if_error == "stop":
                    raise e

        if if_error == "continue":
            return failed_class_list
        return None

    def delete_all_schema(self):
        """Remove the entire schema from the Weaviate instance and all data associated with it."""
        client = self.get_client()
        return client.schema.delete_all()

    def update_config(self, class_name: str, config: dict):
        """Update a schema configuration for a specific class."""
        client = self.get_client()
        client.schema.update_config(class_name=class_name, config=config)

    def create_or_replace_classes(
        self, schema_json: dict[str, Any] | str, existing: ExitingSchemaOptions = "ignore"
    ):
        """
        Create or replace the classes in schema of Weaviate database.

        :param schema_json: Json containing the schema. Format {"class_name": "class_dict"}
            .. seealso:: `example of class_dict <https://weaviate-python-client.readthedocs.io/en/v3.25.2/weaviate.schema.html#weaviate.schema.Schema.create>`_.
        :param existing: Options to handle the case when the classes exist, possible options
            'replace', 'fail', 'ignore'.
        """
        existing_schema_options = ["replace", "fail", "ignore"]
        if existing not in existing_schema_options:
            raise ValueError(f"Param 'existing' should be one of the {existing_schema_options} values.")
        if isinstance(schema_json, str):
            schema_json = cast(dict, json.load(open(schema_json)))
        set__exiting_classes = {class_object["class"] for class_object in self.get_schema()["classes"]}
        set__to_be_added_classes = {key for key, _ in schema_json.items()}
        intersection_classes = set__exiting_classes.intersection(set__to_be_added_classes)
        classes_to_create = set()
        if existing == "fail" and intersection_classes:
            raise ValueError(
                f"Trying to create class {intersection_classes}" f" but this class already exists."
            )
        elif existing == "ignore":
            classes_to_create = set__to_be_added_classes - set__exiting_classes
        elif existing == "replace":
            error_list = self.delete_classes(class_names=list(intersection_classes))
            if error_list:
                raise ValueError(error_list)
            classes_to_create = intersection_classes.union(set__to_be_added_classes)
        classes_to_create_list = [schema_json[item] for item in sorted(list(classes_to_create))]
        self.create_schema({"classes": classes_to_create_list})

    def _compare_schema_subset(self, subset_object: Any, superset_object: Any) -> bool:
        """
        Recursively check if requested subset_object is a subset of the superset_object.

        Example 1:
        superset_object = {"a": {"b": [1, 2, 3], "c": "d"}}
        subset_object = {"a": {"c": "d"}}
        _compare_schema_subset(subset_object, superset_object) # will result in True

        superset_object = {"a": {"b": [1, 2, 3], "c": "d"}}
        subset_object = {"a": {"d": "e"}}
        _compare_schema_subset(subset_object, superset_object) # will result in False

        :param subset_object: The object to be checked
        :param superset_object: The object to check against
        """
        # Direct equality check
        if subset_object == superset_object:
            return True

        # Type mismatch early return
        if type(subset_object) != type(superset_object):
            return False

        # Dictionary comparison
        if isinstance(subset_object, dict):
            for k, v in subset_object.items():
                if (k not in superset_object) or (not self._compare_schema_subset(v, superset_object[k])):
                    return False
            return True

        # List or Tuple comparison
        if isinstance(subset_object, (list, tuple)):
            for sub, sup in zip(subset_object, superset_object):
                if len(subset_object) > len(superset_object) or not self._compare_schema_subset(sub, sup):
                    return False
            return True

        # Default case for non-matching types or unsupported types
        return False

    @staticmethod
    def _convert_properties_to_dict(classes_objects, key_property: str = "name"):
        """
        Helper function to convert list of class properties into dict by using a `key_property` as key.

        This is done to avoid class properties comparison as list of properties.

        Case 1:
        A = [1, 2, 3]
        B = [1, 2]
        When comparing list we check for the length, but it's not suitable for subset check.

        Case 2:
        A = [1, 2, 3]
        B = [1, 3, 2]
        When we compare two lists, we compare item 1 of list A with item 1 of list B and
         pass if the two are same, but there can be scenarios when the properties are not in same order.
        """
        for cls in classes_objects:
            cls["properties"] = {p[key_property]: p for p in cls["properties"]}
        return classes_objects

    def check_subset_of_schema(self, classes_objects: list) -> bool:
        """Check if the class_objects is a subset of existing schema.

        Note - weaviate client's `contains()` don't handle the class properties mismatch, if you want to
         compare `Class A` with `Class B` they must have exactly same properties. If `Class A` has fewer
          numbers of properties than Class B, `contains()` will result in False.

        .. seealso:: `contains <https://weaviate-python-client.readthedocs.io/en/v3.25.3/weaviate.schema.html#weaviate.schema.Schema.contains>`_.
        """
        # When the class properties are not in same order or not the same length. We convert them to dicts
        # with property `name` as the key. This way we ensure, the subset is checked.

        classes_objects = self._convert_properties_to_dict(classes_objects)
        exiting_classes_list = self._convert_properties_to_dict(self.get_schema()["classes"])

        exiting_classes = {cls["class"]: cls for cls in exiting_classes_list}
        exiting_classes_set = set(exiting_classes.keys())
        input_classes_set = {cls["class"] for cls in classes_objects}
        if not input_classes_set.issubset(exiting_classes_set):
            return False
        for cls in classes_objects:
            if not self._compare_schema_subset(cls, exiting_classes[cls["class"]]):
                return False
        return True

    def batch_data(
        self,
        class_name: str,
        data: list[dict[str, Any]] | pd.DataFrame,
        insertion_errors: list,
        batch_config_params: dict[str, Any] | None = None,
        vector_col: str = "Vector",
        uuid_col: str = "id",
        retry_attempts_per_object: int = 5,
        tenant: str | None = None,
    ) -> list:
        """
        Add multiple objects or object references at once into weaviate.

        :param class_name: The name of the class that objects belongs to.
        :param data: list or dataframe of objects we want to add.
        :param batch_config_params: dict of batch configuration option.
            .. seealso:: `batch_config_params options <https://weaviate-python-client.readthedocs.io/en/v3.25.3/weaviate.batch.html#weaviate.batch.Batch.configure>`__
        :param vector_col: name of the column containing the vector.
        :param retry_attempts_per_object: number of time to try in case of failure before giving up.
        :param tenant: The tenant to which the object will be added.
        :param uuid_col: Name of the column containing the UUID.
        :param insertion_errors: list to hold errors while inserting.
        """
        data = self._convert_dataframe_to_list(data)
        total_results = 0
        error_results = 0

        def _process_batch_errors(
            results: list,
            verbose: bool = True,
        ) -> None:
            """
            Helper function to processes the results from insert or delete batch operation and collects any errors.

            :param results: Results from the batch operation.
            :param verbose: Flag to enable verbose logging.
            """
            nonlocal total_results
            nonlocal error_results
            total_batch_results = len(results)
            error_batch_results = 0
            for item in results:
                if "errors" in item["result"]:
                    error_batch_results = error_batch_results + 1
                    item_error = {"uuid": item["id"], "errors": item["result"]["errors"]}
                    if verbose:
                        self.log.info(
                            f"Error occurred in batch process for {item['id']} with error {item['result']['errors']}"
                        )
                    insertion_errors.append(item_error)
            if verbose:
                total_results = total_results + (total_batch_results - error_batch_results)
                error_results = error_results + error_batch_results

                self.log.info(
                    "Total Objects %s / Objects %s successfully inserted and Objects %s had errors.",
                    len(data),
                    total_results,
                    error_results,
                )

        client = self.conn
        if not batch_config_params:
            batch_config_params = {}

        # configuration for context manager for __exit__ method to callback on errors for weaviate
        # batch ingestion.
        if not batch_config_params.get("callback"):
            batch_config_params.update({"callback": _process_batch_errors})

        if not batch_config_params.get("timeout_retries"):
            batch_config_params.update({"timeout_retries": 5})

        if not batch_config_params.get("connection_error_retries"):
            batch_config_params.update({"connection_error_retries": 5})

        client.batch.configure(**batch_config_params)
        with client.batch as batch:
            # Batch import all data
            for index, data_obj in enumerate(data):
                for attempt in Retrying(
                    stop=stop_after_attempt(retry_attempts_per_object),
                    retry=(
                        retry_if_exception(lambda exc: check_http_error_is_retryable(exc))
                        | retry_if_exception_type(REQUESTS_EXCEPTIONS_TYPES)
                    ),
                ):
                    with attempt:
                        vector = data_obj.pop(vector_col, None)
                        uuid = data_obj.pop(uuid_col, None)
                        self.log.debug(
                            "Attempt %s of inserting object with uuid: %s",
                            attempt.retry_state.attempt_number,
                            uuid,
                        )
                        batch.add_data_object(
                            data_object=data_obj,
                            class_name=class_name,
                            vector=vector,
                            uuid=uuid,
                            tenant=tenant,
                        )
                        self.log.debug("Inserted object with uuid: %s into batch", uuid)
        return insertion_errors

    def query_with_vector(
        self,
        embeddings: list[float],
        class_name: str,
        *properties: list[str],
        certainty: float = 0.7,
        limit: int = 1,
    ) -> dict[str, dict[Any, Any]]:
        """
        Query weaviate database with near vectors.

        This method uses a vector search using a Get query. we are using a with_near_vector to provide
        weaviate with a query with vector itself. This is needed for query a  Weaviate class with a custom,
        external vectorizer. Weaviate then converts this into a vector through the inference API
        (OpenAI in this particular example) and uses that vector as the basis for a vector search.
        """
        client = self.conn
        results: dict[str, dict[Any, Any]] = (
            client.query.get(class_name, properties[0])
            .with_near_vector({"vector": embeddings, "certainty": certainty})
            .with_limit(limit)
            .do()
        )
        return results

    def query_without_vector(
        self, search_text: str, class_name: str, *properties: list[str], limit: int = 1
    ) -> dict[str, dict[Any, Any]]:
        """
        Query using near text.

        This method uses a vector search using a Get query. we are using a nearText operator to provide
        weaviate with a query search_text. Weaviate then converts this into a vector through the inference
        API (OpenAI in this particular example) and uses that vector as the basis for a vector search.
        """
        client = self.conn
        results: dict[str, dict[Any, Any]] = (
            client.query.get(class_name, properties[0])
            .with_near_text({"concepts": [search_text]})
            .with_limit(limit)
            .do()
        )
        return results

    def create_object(
        self, data_object: dict | str, class_name: str, **kwargs
    ) -> str | dict[str, Any] | None:
        """Create a new object.

        :param data_object: Object to be added. If type is str it should be either a URL or a file.
        :param class_name: Class name associated with the object given.
        :param kwargs: Additional parameters to be passed to weaviate_client.data_object.create()
        """
        client = self.conn
        # generate deterministic uuid if not provided
        uuid = kwargs.pop("uuid", generate_uuid5(data_object))
        try:
            return client.data_object.create(data_object, class_name, uuid=uuid, **kwargs)
        except ObjectAlreadyExistsException:
            self.log.warning("Object with the UUID %s already exists", uuid)
            return None

    def get_or_create_object(
        self,
        data_object: dict | str | None = None,
        class_name: str | None = None,
        vector: Sequence | None = None,
        consistency_level: ConsistencyLevel | None = None,
        tenant: str | None = None,
        **kwargs,
    ) -> str | dict[str, Any] | None:
        """Get or Create a new object.

        Returns the object if already exists

        :param data_object: Object to be added. If type is str it should be either a URL or a file. This is required
            to create a new object.
        :param class_name: Class name associated with the object given. This is required to create a new object.
        :param vector: Vector associated with the object given. This argument is only used when creating object.
        :param consistency_level: Consistency level to be used. Applies to both create and get operations.
        :tenant: Tenant to be used. Applies to both create and get operations.
        :param kwargs: Additional parameters to be passed to weaviate_client.data_object.create() and
            weaviate_client.data_object.get()
        """
        obj = self.get_object(
            class_name=class_name, consistency_level=consistency_level, tenant=tenant, **kwargs
        )
        if not obj:
            if not (data_object and class_name):
                raise ValueError("data_object and class_name are required to create a new object")
            uuid = kwargs.pop("uuid", generate_uuid5(data_object))
            return self.create_object(
                data_object,
                class_name,
                vector=vector,
                uuid=uuid,
                consistency_level=consistency_level,
                tenant=tenant,
            )
        return obj

    def get_object(self, **kwargs) -> dict[str, Any] | None:
        """Get objects or an object from weaviate.

        :param kwargs: parameters to be passed to weaviate_client.data_object.get() or
            weaviate_client.data_object.get_by_id()
        """
        client = self.conn
        return client.data_object.get(**kwargs)

    def get_all_objects(
        self, after: str | UUID | None = None, as_dataframe: bool = False, **kwargs
    ) -> list[dict[str, Any]] | pd.DataFrame:
        """Get all objects from weaviate.

        if after is provided, it will be used as the starting point for the listing.

        :param after: uuid of the object to start listing from
        :param as_dataframe: if True, returns a pandas dataframe
        :param kwargs: parameters to be passed to weaviate_client.data_object.get()
        """
        all_objects = []
        after = kwargs.pop("after", after)
        while True:
            results = self.get_object(after=after, **kwargs) or {}
            if not results.get("objects"):
                break
            all_objects.extend(results["objects"])
            after = results["objects"][-1]["id"]
        if as_dataframe:
            import pandas

            return pandas.DataFrame(all_objects)
        return all_objects

    def delete_object(self, uuid: UUID | str, **kwargs) -> None:
        """Delete an object from weaviate.

        :param uuid: uuid of the object to be deleted
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.delete()
        """
        client = self.conn
        client.data_object.delete(uuid, **kwargs)

    def update_object(self, data_object: dict | str, class_name: str, uuid: UUID | str, **kwargs) -> None:
        """Update an object in weaviate.

        :param data_object: The object states the fields that should be updated. Fields not specified in the
            'data_object' remain unchanged. Fields that are None will not be changed.
            If type is str it should be either an URL or a file.
        :param class_name: Class name associated with the object given.
        :param uuid: uuid of the object to be updated
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.update()
        """
        client = self.conn
        client.data_object.update(data_object, class_name, uuid, **kwargs)

    def replace_object(self, data_object: dict | str, class_name: str, uuid: UUID | str, **kwargs) -> None:
        """Replace an object in weaviate.

        :param data_object: The object states the fields that should be updated. Fields not specified in the
            'data_object' will be set to None. If type is str it should be either an URL or a file.
        :param class_name: Class name associated with the object given.
        :param uuid: uuid of the object to be replaced
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.replace()
        """
        client = self.conn
        client.data_object.replace(data_object, class_name, uuid, **kwargs)

    def validate_object(self, data_object: dict | str, class_name: str, **kwargs):
        """Validate an object in weaviate.

        :param data_object: The object to be validated. If type is str it should be either an URL or a file.
        :param class_name: Class name associated with the object given.
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.validate()
        """
        client = self.conn
        client.data_object.validate(data_object, class_name, **kwargs)

    def object_exists(self, uuid: str | UUID, **kwargs) -> bool:
        """Check if an object exists in weaviate.

        :param uuid: The UUID of the object that may or may not exist within Weaviate.
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.exists()
        """
        client = self.conn
        return client.data_object.exists(uuid, **kwargs)

    def _delete_objects(self, uuids: Collection, class_name: str, retry_attempts_per_object: int = 5):
        """
        Helper function for `create_or_replace_objects()` to delete multiple objects.

        :param uuids: Collection of uuids.
        :param class_name: Name of the class in Weaviate schema where data is to be ingested.
        :param retry_attempts_per_object: number of times to try in case of failure before giving up.
        """
        for uuid in uuids:
            for attempt in Retrying(
                stop=stop_after_attempt(retry_attempts_per_object),
                retry=(
                    retry_if_exception(lambda exc: check_http_error_is_retryable(exc))
                    | retry_if_exception_type(REQUESTS_EXCEPTIONS_TYPES)
                ),
            ):
                with attempt:
                    try:
                        self.delete_object(uuid=uuid, class_name=class_name)
                        self.log.debug("Deleted object with uuid %s", uuid)
                    except weaviate.exceptions.UnexpectedStatusCodeException as e:
                        if e.status_code == 404:
                            self.log.debug("Tried to delete a non existent object with uuid %s", uuid)
                        else:
                            self.log.debug("Error occurred while trying to delete object with uuid %s", uuid)
                            raise e

        self.log.info("Deleted %s objects.", len(uuids))

    def _generate_uuids(
        self,
        df: pd.DataFrame,
        class_name: str,
        unique_columns: list[str],
        vector_column: str | None = None,
        uuid_column: str | None = None,
    ) -> tuple[pd.DataFrame, str]:
        """
        Adds UUIDs to a DataFrame, useful for replace operations where UUIDs must be known before ingestion.

        By default, UUIDs are generated using a custom function if 'uuid_column' is not specified.
        The function can potentially ingest the same data multiple times with different UUIDs.

        :param df: A dataframe with data to generate a UUID from.
        :param class_name: The name of the class use as part of the uuid namespace.
        :param uuid_column: Name of the column to create. Default is 'id'.
        :param unique_columns: A list of columns to use for UUID generation. By default, all columns except
            vector_column will be used.
        :param vector_column: Name of the column containing the vector data.  If specified the vector will be
            removed prior to generating the uuid.
        """
        column_names = df.columns.to_list()

        difference_columns = set(unique_columns).difference(set(df.columns.to_list()))
        if difference_columns:
            raise ValueError(f"Columns {', '.join(difference_columns)} don't exist in dataframe")

        if uuid_column is None:
            self.log.info("No uuid_column provided. Generating UUIDs as column name `id`.")
            if "id" in column_names:
                raise ValueError(
                    "Property 'id' already in dataset. Consider renaming or specify 'uuid_column'."
                )
            else:
                uuid_column = "id"

        if uuid_column in column_names:
            raise ValueError(
                f"Property {uuid_column} already in dataset. Consider renaming or specify a different"
                f" 'uuid_column'."
            )

        df[uuid_column] = (
            df[unique_columns]
            .drop(columns=[vector_column], inplace=False, errors="ignore")
            .apply(lambda row: generate_uuid5(identifier=row.to_dict(), namespace=class_name), axis=1)
        )

        return df, uuid_column

    def _get_documents_to_uuid_map(
        self, data: pd.DataFrame, document_column: str, uuid_column: str, class_name: str
    ) -> dict[str, set]:
        """Helper function to get the document to uuid map of existing objects in db.

        :param data: A single pandas DataFrame.
        :param document_column: The name of the property to query.
        :param class_name: The name of the class to query.
        :param uuid_column: The name of the column containing the UUID.
        """
        offset = 0
        limit = 2000
        documents_to_uuid: dict = {}
        document_keys = set(data[document_column])
        while True:
            data_objects = (
                self.conn.query.get(properties=[document_column], class_name=class_name)
                .with_additional([uuid_column])
                .with_where(
                    {
                        "operator": "Or",
                        "operands": [
                            {"valueText": key, "path": document_column, "operator": "Equal"}
                            for key in document_keys
                        ],
                    }
                )
                .with_offset(offset)
                .with_limit(limit)
                .do()["data"]["Get"][class_name]
            )
            if len(data_objects) == 0:
                break
            offset = offset + limit
            documents_to_uuid.update(
                self._prepare_document_to_uuid_map(
                    data=data_objects,
                    group_key=document_column,
                    get_value=lambda x: x["_additional"][uuid_column],
                )
            )
        return documents_to_uuid

    @staticmethod
    def _prepare_document_to_uuid_map(
        data: list[dict], group_key: str, get_value: Callable[[dict], str]
    ) -> dict[str, set]:
        """Helper function to prepare the map of grouped_key to set."""
        grouped_key_to_set: dict = {}
        for item in data:
            document_url = item[group_key]

            if document_url not in grouped_key_to_set:
                grouped_key_to_set[document_url] = set()

            grouped_key_to_set[document_url].add(get_value(item))
        return grouped_key_to_set

    def _check_existing_documents(
        self, data: pd.DataFrame, document_column: str, class_name: str, uuid_column: str
    ) -> tuple[dict[str, set], set, set, set]:
        """
        Get all object uuids belonging to a document.

        :param data: A single pandas DataFrame.
        :param document_column: The name of the property to query.
        :param class_name: The name of the class to query.
        :param uuid_column: The name of the column containing the UUID.
        """
        changed_documents = set()
        unchanged_docs = set()
        non_existing_documents = set()
        documents_to_uuid = self._get_documents_to_uuid_map(
            data=data, uuid_column=uuid_column, document_column=document_column, class_name=class_name
        )

        insert_documents_to_uuid = self._prepare_document_to_uuid_map(
            data=data.to_dict("records"),
            group_key=document_column,
            get_value=lambda x: x[uuid_column],
        )

        for doc_url, doc_set in insert_documents_to_uuid.items():
            if doc_url in documents_to_uuid:
                if documents_to_uuid[doc_url].difference(doc_set) or doc_set.difference(
                    documents_to_uuid[doc_url]
                ):
                    changed_documents.add(doc_url)
                else:
                    unchanged_docs.add(doc_url)
            else:
                non_existing_documents.add(doc_url)

        return documents_to_uuid, changed_documents, unchanged_docs, non_existing_documents

    def _delete_all_documents_objects(
        self,
        document_keys: list[str],
        document_column: str,
        class_name: str,
        total_objects_count: int = 1,
        batch_delete_error: list | None = None,
        tenant: str | None = None,
        batch_config_params: dict[str, Any] | None = None,
        verbose: bool = False,
    ):
        if not batch_config_params:
            batch_config_params = {}

        # This limit is imposed by Weavaite database
        MAX_LIMIT_ON_TOTAL_DELETABLE_OBJECTS = 10000

        self.conn.batch.configure(**batch_config_params)
        with self.conn.batch as batch:
            batch.consistency_level = weaviate.data.replication.ConsistencyLevel.ALL
            while total_objects_count > 0:
                document_objects = batch.delete_objects(
                    class_name=class_name,
                    where={
                        "operator": "Or",
                        "operands": [
                            {
                                "path": [document_column],
                                "operator": "Equal",
                                "valueText": key,
                            }
                            for key in document_keys
                        ],
                    },
                    output="verbose",
                    dry_run=False,
                    tenant=tenant,
                )
                total_objects_count = total_objects_count - MAX_LIMIT_ON_TOTAL_DELETABLE_OBJECTS
                matched_objects = document_objects["results"]["matches"]
                batch_delete_error = [
                    {"uuid": obj["id"]}
                    for obj in document_objects["results"]["objects"]
                    if "error" in obj["status"]
                ]
                if verbose:
                    self.log.info("Deleted %s Objects", matched_objects)

        return batch_delete_error

    def create_or_replace_document_objects(
        self,
        data: pd.DataFrame | list[dict[str, Any]] | list[pd.DataFrame],
        class_name: str,
        document_column: str,
        existing: str = "skip",
        uuid_column: str | None = None,
        vector_column: str = "Vector",
        batch_config_params: dict | None = None,
        tenant: str | None = None,
        verbose: bool = False,
    ):
        """
        create or replace objects belonging to documents.

        In real-world scenarios, information sources like Airflow docs, Stack Overflow, or other issues
        are considered 'documents' here. It's crucial to keep the database objects in sync with these sources.
        If any changes occur in these documents, this function aims to reflect those changes in the database.

        .. note::
            This function assumes responsibility for identifying changes in documents, dropping relevant
            database objects, and recreating them based on updated information. It's crucial to handle this
            process with care, ensuring backups and validation are in place to prevent data loss or
            inconsistencies.

        Provides users with multiple ways of dealing with existing values.
            1. replace: replace the existing objects with new objects. This option requires to identify the
             objects belonging to a document. which by default is done by using document_column field.
            2. skip: skip the existing objects and only add the missing objects of a document.
            3. error: raise an error if an object belonging to a existing document is tried to be created.

        :param data: A single pandas DataFrame or a list of dicts to be ingested.
        :param class_name: Name of the class in Weaviate schema where data is to be ingested.
        :param existing: Strategy for handling existing data: 'skip', or 'replace'. Default is 'skip'.
        :param document_column: Column in DataFrame that identifying source document.
        :param uuid_column: Column with pre-generated UUIDs. If not provided, UUIDs will be generated.
        :param vector_column: Column with embedding vectors for pre-embedded data.
        :param batch_config_params: Additional parameters for Weaviate batch configuration.
        :param tenant: The tenant to which the object will be added.
        :param verbose: Flag to enable verbose output during the ingestion process.
        :return: list of UUID which failed to create
        """
        import pandas as pd

        if existing not in ["skip", "replace", "error"]:
            raise ValueError("Invalid parameter for 'existing'. Choices are 'skip', 'replace', 'error'.")

        # data = list(data)

        if isinstance(data, list) and len(data) and isinstance(data[0], dict):
            data = cast(pd.DataFrame, pd.json_normalize(data))
        elif isinstance(data, list) and len(data) and isinstance(data[0], pd.DataFrame):
            data = cast(pd.DataFrame, pd.concat(data, ignore_index=True))
        else:
            data = cast(pd.DataFrame, data)

        unique_columns = sorted(data.columns.to_list())

        if verbose:
            self.log.info("%s objects came in for insertion.", data.shape[0])

        if uuid_column is None or uuid_column not in data.columns:
            (
                data,
                uuid_column,
            ) = self._generate_uuids(
                df=data,
                class_name=class_name,
                unique_columns=unique_columns,
                vector_column=vector_column,
                uuid_column=uuid_column,
            )

        # drop duplicate rows, using uuid_column and unique_columns. Removed  `None` as it can be added to
        # set when `uuid_column` is None.
        data = data.drop_duplicates(subset=[document_column, uuid_column], keep="first")
        if verbose:
            self.log.info("%s objects remain after deduplication.", data.shape[0])

        batch_delete_error: list = []
        (
            documents_to_uuid_map,
            changed_documents,
            unchanged_docs,
            non_existing_documents,
        ) = self._check_existing_documents(
            data=data,
            document_column=document_column,
            uuid_column=uuid_column,
            class_name=class_name,
        )
        if verbose:
            self.log.info(
                "Found %s changed documents, %s unchanged documents and %s non-existing documents",
                len(changed_documents),
                len(unchanged_docs),
                len(non_existing_documents),
            )
            for document in changed_documents:
                self.log.info(
                    "Changed document: %s has %s objects.", document, len(documents_to_uuid_map[document])
                )

            self.log.info("Non-existing document: %s", ", ".join(non_existing_documents))

        if existing == "error" and len(changed_documents):
            raise ValueError(
                f"Documents {', '.join(changed_documents)} already exists. You can either skip or replace"
                f" them by passing 'existing=skip' or 'existing=replace' respectively."
            )
        elif existing == "skip":
            data = data[data[document_column].isin(non_existing_documents)]
            if verbose:
                self.log.info(
                    "Since existing=skip, ingesting only non-existing document's object %s", data.shape[0]
                )
        elif existing == "replace":
            total_objects_count = sum([len(documents_to_uuid_map[doc]) for doc in changed_documents])
            if verbose:
                self.log.info(
                    "Since existing='replace', deleting %s objects belonging changed documents %s",
                    total_objects_count,
                    changed_documents,
                )
            batch_delete_error = self._delete_all_documents_objects(
                document_keys=list(changed_documents),
                total_objects_count=total_objects_count,
                document_column=document_column,
                class_name=class_name,
                batch_delete_error=batch_delete_error,
                tenant=tenant,
                batch_config_params=batch_config_params,
                verbose=verbose,
            )
            data = data[data[document_column].isin(non_existing_documents.union(changed_documents))]

        insertion_errors: list = []
        if data.shape[0]:
            self.log.info("Batch inserting %s objects.", data.shape[0])
            insertion_errors = self.batch_data(
                class_name=class_name,
                data=data,
                insertion_errors=insertion_errors,
                batch_config_params=batch_config_params,
                vector_col=vector_column,
                uuid_col=uuid_column,
                tenant=tenant,
            )
            if insertion_errors or batch_delete_error:
                if insertion_errors:
                    self.log.info("Failed to insert %s objects.", len(insertion_errors))
                if batch_delete_error:
                    self.log.info("Failed to delete %s objects.", len(insertion_errors))
                # Rollback object that were not created properly
                self._delete_objects(
                    [item["uuid"] for item in insertion_errors + batch_delete_error], class_name=class_name
                )

        if verbose:
            self.log.info(
                "Total objects in class %s : %s ",
                class_name,
                self.conn.query.aggregate(class_name).with_meta_count().do(),
            )
        return insertion_errors, batch_delete_error

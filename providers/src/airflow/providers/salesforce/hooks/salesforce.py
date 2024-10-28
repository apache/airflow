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
"""
Connect to your Salesforce instance, retrieve data from it, and write that data to a file for other uses.

.. note:: this hook also relies on the simple_salesforce package:
      https://github.com/simple-salesforce/simple-salesforce
"""

from __future__ import annotations

import logging
import time
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterable, cast

from simple_salesforce import Salesforce, api

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    import pandas as pd
    from requests import Session
    from simple_salesforce.api import SFType

log = logging.getLogger(__name__)


class SalesforceHook(BaseHook):
    """
    Creates new connection to Salesforce and allows you to pull data out of SFDC and save it to a file.

    You can then use that file with other Airflow operators to move the data into another data source.

    :param conn_id: The name of the connection that has the parameters needed to connect to Salesforce.
        The connection should be of type `Salesforce`.
    :param session_id: The access token for a given HTTP request session.
    :param session: A custom HTTP request session. This enables the use of requests Session features not
        otherwise exposed by `simple_salesforce`.

    .. note::
        A connection to Salesforce can be created via several authentication options:

        * Password: Provide Username, Password, and Security Token
        * Direct Session: Provide a `session_id` and either Instance or Instance URL
        * OAuth 2.0 JWT: Provide a Consumer Key and either a Private Key or Private Key File Path
        * IP Filtering: Provide Username, Password, and an Organization ID

        If in sandbox, enter a Domain value of 'test'.
    """

    conn_name_attr = "salesforce_conn_id"
    default_conn_name = "salesforce_default"
    conn_type = "salesforce"
    hook_name = "Salesforce"

    def __init__(
        self,
        salesforce_conn_id: str = default_conn_name,
        session_id: str | None = None,
        session: Session | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = salesforce_conn_id
        self.session_id = session_id
        self.session = session

    def _get_field(self, extras: dict, field_name: str):
        """Get field from extra, first checking short name, then for backcompat we check for prefixed name."""
        backcompat_prefix = "extra__salesforce__"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
                "when using this method."
            )
        if field_name in extras:
            return extras[field_name] or None
        prefixed_name = f"{backcompat_prefix}{field_name}"
        return extras.get(prefixed_name) or None

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import (
            BS3PasswordFieldWidget,
            BS3TextFieldWidget,
        )
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "security_token": PasswordField(
                lazy_gettext("Security Token"), widget=BS3PasswordFieldWidget()
            ),
            "domain": StringField(lazy_gettext("Domain"), widget=BS3TextFieldWidget()),
            "consumer_key": StringField(
                lazy_gettext("Consumer Key"), widget=BS3TextFieldWidget()
            ),
            "private_key_file_path": PasswordField(
                lazy_gettext("Private Key File Path"), widget=BS3PasswordFieldWidget()
            ),
            "private_key": PasswordField(
                lazy_gettext("Private Key"), widget=BS3PasswordFieldWidget()
            ),
            "organization_id": StringField(
                lazy_gettext("Organization ID"), widget=BS3TextFieldWidget()
            ),
            "instance": StringField(
                lazy_gettext("Instance"), widget=BS3TextFieldWidget()
            ),
            "instance_url": StringField(
                lazy_gettext("Instance URL"), widget=BS3TextFieldWidget()
            ),
            "proxies": StringField(lazy_gettext("Proxies"), widget=BS3TextFieldWidget()),
            "version": StringField(
                lazy_gettext("API Version"), widget=BS3TextFieldWidget()
            ),
            "client_id": StringField(
                lazy_gettext("Client ID"), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra", "host"],
            "relabeling": {
                "login": "Username",
            },
        }

    @cached_property
    def conn(self) -> api.Salesforce:
        """Returns a Salesforce instance. (cached)."""
        connection = self.get_connection(self.conn_id)
        extras = connection.extra_dejson
        # all extras below (besides the version one) are explicitly defaulted to None
        # because simple-salesforce has a built-in authentication-choosing method that
        # relies on which arguments are None and without "or None" setting this connection
        # in the UI will result in the blank extras being empty strings instead of None,
        # which would break the connection if "get" was used on its own.
        conn = Salesforce(
            username=connection.login,
            password=connection.password,
            security_token=self._get_field(extras, "security_token") or None,
            domain=self._get_field(extras, "domain") or None,
            session_id=self.session_id,
            instance=self._get_field(extras, "instance") or None,
            instance_url=self._get_field(extras, "instance_url") or None,
            organizationId=self._get_field(extras, "organization_id") or None,
            version=self._get_field(extras, "version") or api.DEFAULT_API_VERSION,
            proxies=self._get_field(extras, "proxies") or None,
            session=self.session,
            client_id=self._get_field(extras, "client_id") or None,
            consumer_key=self._get_field(extras, "consumer_key") or None,
            privatekey_file=self._get_field(extras, "private_key_file_path") or None,
            privatekey=self._get_field(extras, "private_key") or None,
        )
        return conn

    def get_conn(self) -> api.Salesforce:
        """Return a Salesforce instance. (cached)."""
        return self.conn

    def make_query(
        self, query: str, include_deleted: bool = False, query_params: dict | None = None
    ) -> dict:
        """
        Make a query to Salesforce.

        :param query: The query to make to Salesforce.
        :param include_deleted: True if the query should include deleted records.
        :param query_params: Additional optional arguments
        :return: The query result.
        """
        conn = self.get_conn()

        self.log.info("Querying for all objects")
        query_params = query_params or {}
        query_results = conn.query_all(
            query, include_deleted=include_deleted, **query_params
        )

        self.log.info(
            "Received results: Total size: %s; Done: %s",
            query_results["totalSize"],
            query_results["done"],
        )

        return query_results

    def describe_object(self, obj: str) -> dict:
        """
        Get the description of an object from Salesforce.

        This description is the object's schema and
        some extra metadata that Salesforce stores for each object.

        :param obj: The name of the Salesforce object that we are getting a description of.
        :return: the description of the Salesforce object.
        """
        conn = self.get_conn()
        sftype: SFType = cast("SFType", conn.__getattr__(obj))

        return sftype.describe()

    def get_available_fields(self, obj: str) -> list[str]:
        """
        Get a list of all available fields for an object.

        :param obj: The name of the Salesforce object that we are getting a description of.
        :return: the names of the fields.
        """
        obj_description = self.describe_object(obj)

        return [field["name"] for field in obj_description["fields"]]

    def get_object_from_salesforce(self, obj: str, fields: Iterable[str]) -> dict:
        """
        Get all instances of the `object` from Salesforce.

        For each model, only get the fields specified in fields.

        All we really do underneath the hood is run:
            SELECT <fields> FROM <obj>;

        :param obj: The object name to get from Salesforce.
        :param fields: The fields to get from the object.
        :return: all instances of the object from Salesforce.
        """
        query = f"SELECT {','.join(fields)} FROM {obj}"

        self.log.info(
            "Making query to Salesforce: %s",
            query if len(query) < 30 else " ... ".join([query[:15], query[-15:]]),
        )

        return self.make_query(query)

    @classmethod
    def _to_timestamp(cls, column: pd.Series) -> pd.Series:
        """
        Convert a column of a dataframe to UNIX timestamps if applicable.

        :param column: A Series object representing a column of a dataframe.
        :return: a new series that maintains the same index as the original
        """
        # try and convert the column to datetimes
        # the column MUST have a four digit year somewhere in the string
        # there should be a better way to do this,
        # but just letting pandas try and convert every column without a format
        # caused it to convert floats as well
        # For example, a column of integers
        # between 0 and 10 are turned into timestamps
        # if the column cannot be converted,
        # just return the original column untouched
        import numpy as np
        import pandas as pd

        try:
            column = pd.to_datetime(column)
        except ValueError:
            log.error("Could not convert field to timestamps: %s", column.name)
            return column

        # now convert the newly created datetimes into timestamps
        # we have to be careful here
        # because NaT cannot be converted to a timestamp
        # so we have to return NaN
        converted = []
        for value in column:
            try:
                converted.append(value.timestamp())
            except (ValueError, AttributeError):
                converted.append(np.NaN)

        return pd.Series(converted, index=column.index)

    def write_object_to_file(
        self,
        query_results: list[dict],
        filename: str,
        fmt: str = "csv",
        coerce_to_timestamp: bool = False,
        record_time_added: bool = False,
    ) -> pd.DataFrame:
        """
        Write query results to file.

        Acceptable formats are:
            - csv:
                comma-separated-values file. This is the default format.
            - json:
                JSON array. Each element in the array is a different row.
            - ndjson:
                JSON array but each element is new-line delimited instead of comma delimited like in `json`

        This requires a significant amount of cleanup.
        Pandas doesn't handle output to CSV and json in a uniform way.
        This is especially painful for datetime types.
        Pandas wants to write them as strings in CSV, but as millisecond Unix timestamps.

        By default, this function will try and leave all values as they are represented in Salesforce.
        You use the `coerce_to_timestamp` flag to force all datetimes to become Unix timestamps (UTC).
        This is can be greatly beneficial as it will make all of your datetime fields look the same,
        and makes it easier to work with in other database environments

        :param query_results: the results from a SQL query
        :param filename: the name of the file where the data should be dumped to
        :param fmt: the format you want the output in. Default:  'csv'
        :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
            False if you want them to be left in the same format as they were in Salesforce.
            Leaving the value as False will result in datetimes being strings. Default: False
        :param record_time_added: True if you want to add a Unix timestamp field
            to the resulting data that marks when the data was fetched from Salesforce. Default: False
        :return: the dataframe that gets written to the file.
        """
        fmt = fmt.lower()
        if fmt not in ["csv", "json", "ndjson"]:
            raise ValueError(f"Format value is not recognized: {fmt}")

        df = self.object_to_df(
            query_results=query_results,
            coerce_to_timestamp=coerce_to_timestamp,
            record_time_added=record_time_added,
        )

        # write the CSV or JSON file depending on the option
        # NOTE:
        #   datetimes here are an issue.
        #   There is no good way to manage the difference
        #   for to_json, the options are an epoch or a ISO string
        #   but for to_csv, it will be a string output by datetime
        #   For JSON we decided to output the epoch timestamp in seconds
        #   (as is fairly standard for JavaScript)
        #   And for csv, we do a string
        if fmt == "csv":
            # there are also a ton of newline objects that mess up our ability to write to csv
            # we remove these newlines so that the output is a valid CSV format
            self.log.info("Cleaning data and writing to CSV")
            possible_strings = df.columns[df.dtypes == "object"]
            df[possible_strings] = (
                df[possible_strings]
                .astype(str)
                .apply(lambda x: x.str.replace("\r\n", "").str.replace("\n", ""))
            )
            # write the dataframe
            df.to_csv(filename, index=False)
        elif fmt == "json":
            df.to_json(filename, "records", date_unit="s")
        elif fmt == "ndjson":
            df.to_json(filename, "records", lines=True, date_unit="s")

        return df

    def object_to_df(
        self,
        query_results: list[dict],
        coerce_to_timestamp: bool = False,
        record_time_added: bool = False,
    ) -> pd.DataFrame:
        """
        Export query results to dataframe.

        By default, this function will try and leave all values as they are represented in Salesforce.
        You use the `coerce_to_timestamp` flag to force all datetimes to become Unix timestamps (UTC).
        This is can be greatly beneficial as it will make all of your datetime fields look the same,
        and makes it easier to work with in other database environments

        :param query_results: the results from a SQL query
        :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
            False if you want them to be left in the same format as they were in Salesforce.
            Leaving the value as False will result in datetimes being strings. Default: False
        :param record_time_added: True if you want to add a Unix timestamp field
            to the resulting data that marks when the data was fetched from Salesforce. Default: False
        :return: the dataframe.
        """
        import pandas as pd

        # this line right here will convert all integers to floats
        # if there are any None/np.nan values in the column
        # that's because None/np.nan cannot exist in an integer column
        # we should write all of our timestamps as FLOATS in our final schema
        df = pd.DataFrame.from_records(query_results, exclude=["attributes"])
        df.rename(columns=str.lower, inplace=True)

        # convert columns with datetime strings to datetimes
        # not all strings will be datetimes, so we ignore any errors that occur
        # we get the object's definition at this point and only consider
        # features that are DATE or DATETIME
        if coerce_to_timestamp and df.shape[0] > 0:
            # get the object name out of the query results
            # it's stored in the "attributes" dictionary
            # for each returned record
            object_name = query_results[0]["attributes"]["type"]

            self.log.info("Coercing timestamps for: %s", object_name)

            schema = self.describe_object(object_name)

            # possible columns that can be converted to timestamps
            # are the ones that are either date or datetime types
            # strings are too general and we risk unintentional conversion
            possible_timestamp_cols = [
                field["name"].lower()
                for field in schema["fields"]
                if field["type"] in ["date", "datetime"]
                and field["name"].lower() in df.columns
            ]
            df[possible_timestamp_cols] = df[possible_timestamp_cols].apply(
                self._to_timestamp
            )

        if record_time_added:
            fetched_time = time.time()
            df["time_fetched_from_salesforce"] = fetched_time

        return df

    def test_connection(self):
        """Test the Salesforce connectivity."""
        try:
            self.describe_object("Account")
            status = True
            message = "Connection successfully tested"
        except Exception as e:
            status = False
            message = str(e)

        return status, message

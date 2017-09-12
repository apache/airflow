# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This module contains a Salesforce Hook
which allows you to connect to your Salesforce instance,
retrieve data from it, and write that data to a file
for other uses.

NOTE:   this hook also relies on the simple_salesforce package:
        https://github.com/simple-salesforce/simple-salesforce
"""
from simple_salesforce import Salesforce
from airflow.hooks.base_hook import BaseHook

import json

import pandas as pd
import time

from airflow.utils.log.LoggingMixin import LoggingMixin


class SalesforceHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs
    ):
        """
        Create new connection to Salesforce
        and allows you to pull data out of SFDC and save it to a file.

        You can then use that file with other
        Airflow operators to move the data into another data source

        :param conn_id:     the name of the connection that has the parameters
                            we need to connect to Salesforce.
                            The conenction shoud be type `http` and include a
                            user's security token in the `Extras` field.
        .. note::
            For the HTTP connection type, you can include a
            JSON structure in the `Extras` field.
            We need a user's security token to connect to Salesforce.
            So we define it in the `Extras` field as:
                `{"security_token":"YOUR_SECRUITY_TOKEN"}`
        """
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def sign_in(self):
        """
        Sign into Salesforce.

        If we have already signed it, this will just return the original object
        """
        if hasattr(self, 'sf'):
            return self.sf

        # connect to Salesforce
        sf = Salesforce(
            username=self.connection.login,
            password=self.connection.password,
            security_token=self.extras['security_token'],
            instance_url=self.connection.host
        )
        self.sf = sf
        return sf

    def make_query(self, query):
        """
        Make a query to Salesforce.  Returns result in dictionary

        :param query:    The query to make to Salesforce
        """
        self.sign_in()

        self.logger.info("Querying for all objects")
        query = self.sf.query_all(query)

        self.logger.info(
            "Received results: Total size: %s; Done: %s",
            query['totalSize'], query['done']
        )

        query = json.loads(json.dumps(query))
        return query

    def describe_object(self, obj):
        """
        Get the description of an object from Salesforce.

        This description is the object's schema
        and some extra metadata that Salesforce stores for each object

        :param obj:     Name of the Salesforce object
                        that we are getting a description of.
        """
        self.sign_in()

        return json.loads(json.dumps(self.sf.__getattr__(obj).describe()))

    def get_available_fields(self, obj):
        """
        Get a list of all available fields for an object.

        This only returns the names of the fields.
        """
        self.sign_in()

        desc = self.describe_object(obj)

        return [f['name'] for f in desc['fields']]

    def _build_field_list(self, fields):
        # join all of the fields in a comma seperated list
        return ",".join(fields)

    def get_object_from_salesforce(self, obj, fields):
        """
        Get all instances of the `object` from Salesforce.
        For each model, only get the fields specified in fields.

        All we really do underneath the hood is run:
            SELECT <fields> FROM <obj>;
        """
        field_string = self._build_field_list(fields)

        query = "SELECT {0} FROM {1}".format(field_string, obj)
        self.logger.info(
            "Making query to Salesforce: %s",
            query if len(query) < 30 else " ... ".join([query[:15], query[-15:]])
        )
        return self.make_query(query)

    @classmethod
    def _to_timestamp(cls, col):
        """
        Convert a column of a dataframe to UNIX timestamps if applicable

        :param col:     A Series object representing a column of a dataframe.
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
        try:
            col = pd.to_datetime(col)
        except ValueError:
            log = LoggingMixin().logger
            log.warning(
                "Could not convert field to timestamps: %s", col.name
            )
            return col

        # now convert the newly created datetimes into timestamps
        # we have to be careful here
        # because NaT cannot be converted to a timestamp
        # so we have to return NaN
        converted = []
        for i in col:
            try:
                converted.append(i.timestamp())
            except ValueError:
                converted.append(pd.np.NaN)
            except AttributeError:
                converted.append(pd.np.NaN)

        # return a new series that maintains the same index as the original
        return pd.Series(converted, index=col.index)

    def write_object_to_file(
        self,
        query_results,
        filename,
        fmt="csv",
        coerce_to_timestamp=False,
        record_time_added=False
    ):
        """
        Write query results to file.

        Acceptable formats are:
            - csv:
                comma-seperated-values file.  This is the default format.
            - json:
                JSON array.  Each element in the array is a different row.
            - ndjson:
                JSON array but each element is new-line deliminated
                instead of comman deliminated like in `json`

        This requires a significant amount of cleanup.
        Pandas doesn't handle output to CSV and json in a uniform way.
        This is especially painful for datetime types.
        Pandas wants to write them as strings in CSV,
        but as milisecond Unix timestamps.

        By default, this function will try and leave all values as
        they are represented in Salesforce.
        You use the `coerce_to_timestamp` flag to force all datetimes
        to become Unix timestamps (UTC).
        This is can be greatly beneficial as it will make all of your
        datetime fields look the same,
        and makes it easier to work with in other database environments

        :param query_results:       the results from a SQL query
        :param filename:            the name of the file where the data
                                    should be dumped to
        :param fmt:                 the format you want the output in.
                                    *Default:* csv.
        :param coerce_to_timestamp: True if you want all datetime fields to be
                                    converted into Unix timestamps.
                                    False if you want them to be left in the
                                    same format as they were in Salesforce.
                                    Leaving the value as False will result
                                    in datetimes being strings.
                                    *Defaults to False*
        :param record_time_added:   *(optional)* True if you want to add a
                                    Unix timestamp field to the resulting data
                                    that marks when the data
                                    was fetched from Salesforce.
                                    *Default: False*.
        """
        fmt = fmt.lower()
        if fmt not in ['csv', 'json', 'ndjson']:
            raise ValueError("Format value is not recognized: {0}".format(fmt))

        # this line right here will convert all integers to floats if there are
        # any None/np.nan values in the column
        # that's because None/np.nan cannot exist in an integer column
        # we should write all of our timestamps as FLOATS in our final schema
        df = pd.DataFrame.from_records(query_results, exclude=["attributes"])

        df.columns = [c.lower() for c in df.columns]

        # convert columns with datetime strings to datetimes
        # not all strings will be datetimes, so we ignore any errors that occur
        # we get the object's definition at this point and only consider
        # features that are DATE or DATETIME
        if coerce_to_timestamp and df.shape[0] > 0:
            # get the object name out of the query results
            # it's stored in the "attributes" dictionary
            # for each returned record
            object_name = query_results[0]['attributes']['type']

            self.logger.info("Coercing timestamps for: %s", object_name)

            schema = self.describe_object(object_name)

            # possible columns that can be convereted to timestamps
            # are the ones that are either date or datetime types
            # strings are too general and we risk unintentional conversion
            possible_timestamp_cols = [
                i['name'].lower()
                for i in schema['fields']
                if i['type'] in ["date", "datetime"] and
                i['name'].lower() in df.columns
            ]
            df[possible_timestamp_cols] = df[possible_timestamp_cols].apply(
                lambda x: self._to_timestamp(x)
            )

        if record_time_added:
            fetched_time = time.time()
            df["time_fetched_from_salesforce"] = fetched_time

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
            # there are also a ton of newline objects
            # that mess up our ability to write to csv
            # we remove these newlines so that the output is a valid CSV format
            self.logger.info("Cleaning data and writing to CSV")
            possible_strings = df.columns[df.dtypes == "object"]
            df[possible_strings] = df[possible_strings].apply(
                lambda x: x.str.replace("\r\n", "")
            )
            df[possible_strings] = df[possible_strings].apply(
                lambda x: x.str.replace("\n", "")
            )

            # write the dataframe
            df.to_csv(filename, index=False)
        elif fmt == "json":
            df.to_json(filename, "records", date_unit="s")
        elif fmt == "ndjson":
            df.to_json(filename, "records", lines=True, date_unit="s")

        return df

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

from datetime import datetime
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.google.cloud.transfers.gcs_to_gcs import WILDCARD, GCSToGCSOperator

TASK_ID = "test-gcs-to-gcs-operator"
TEST_BUCKET = "test-bucket"
PREFIX = "TEST"
SOURCE_FOLDER = "test_object"
SOURCE_OBJECTS_NO_FILE = [""]
SOURCE_OBJECTS_TWO_EMPTY_STRING = ["", ""]
SOURCE_OBJECTS_SINGLE_FILE = [f"{SOURCE_FOLDER}/file1.txt"]
SOURCE_OBJECTS_MULTIPLE_FILES = [
    f"{SOURCE_FOLDER}/file1.txt",
    f"{SOURCE_FOLDER}/file2.txt",
]
SOURCE_OBJECTS_LIST = [
    f"{SOURCE_FOLDER}/file1.txt",
    f"{SOURCE_FOLDER}/file2.txt",
    f"{SOURCE_FOLDER}/file3.json",
]

SOURCE_OBJECT_WILDCARD_PREFIX = "*test_object"
SOURCE_OBJECT_WILDCARD_SUFFIX = "test_object*"
SOURCE_OBJECT_WILDCARD_MIDDLE = "test*object"
SOURCE_OBJECT_WILDCARD_FILENAME = "test_object*.txt"
SOURCE_OBJECT_WILDCARD_NESTED = "test_object/file1.txt"
SOURCE_OBJECT_NO_WILDCARD = "test_object.txt"
SOURCE_OBJECT_MULTIPLE_WILDCARDS = "csv/*/test_*.csv"
DESTINATION_BUCKET = "archive"
DESTINATION_OBJECT = "foo/bar/"
DESTINATION_OBJECT_PREFIX = "foo/bar"
DELIMITER = ".json"

MOD_TIME_1 = datetime(2016, 1, 1)
MOD_TIME_2 = datetime(2019, 1, 1)


# TODO: After deprecating delimiter and wildcards in source objects,
#       implement reverted changes from the first commit of PR #31261
class TestGoogleCloudStorageToCloudStorageOperator:
    """
    Tests the three use-cases for the wildcard operator. These are
    no_prefix: *test_object
    no_suffix: test_object*
    prefix_and_suffix: test*object
    Also tests the destination_object as prefix when the wildcard is used.
    """

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_no_prefix(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_PREFIX,
            destination_bucket=DESTINATION_BUCKET,
        )

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="", delimiter="test_object"
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_no_suffix(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_SUFFIX,
            destination_bucket=DESTINATION_BUCKET,
        )

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="test_object", delimiter=""
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_wildcard_with_replace_flag_false(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_SUFFIX,
            destination_bucket=DESTINATION_BUCKET,
            replace=False,
        )

        operator.execute(None)
        mock_calls = [
            mock.call(TEST_BUCKET, prefix="test_object", delimiter=""),
            mock.call(
                DESTINATION_BUCKET, prefix="test_object", delimiter="", match_glob=None
            ),
        ]
        mock_hook.return_value.list.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_no_wildcard_with_replace_flag_false(self, mock_hook):
        mock_hook.return_value.list.side_effect = [[], [SOURCE_OBJECT_NO_WILDCARD]]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            replace=False,
        )

        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET,
                prefix=SOURCE_OBJECT_NO_WILDCARD,
                delimiter=None,
                match_glob=None,
            ),
            mock.call(
                DESTINATION_BUCKET,
                prefix=SOURCE_OBJECT_NO_WILDCARD,
                delimiter=None,
                match_glob=None,
            ),
        ]
        mock_hook.return_value.list.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_copy_file_with_exact_match(self, mock_hook):
        SOURCE_FILES = [
            "test_object.txt",
            "test_object.txt.abc",
            "test_object.txt.copy/",
            "test_object.txt.folder/",
        ]
        mock_hook.return_value.list.return_value = SOURCE_FILES
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            exact_match=True,
        )

        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET, prefix="test_object.txt", delimiter=None, match_glob=None
            ),
        ]
        mock_hook.return_value.list.assert_has_calls(mock_calls)
        mock_hook.return_value.rewrite.assert_has_calls(
            [
                mock.call(
                    TEST_BUCKET, "test_object.txt", DESTINATION_BUCKET, "test_object.txt"
                ),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_copy_file_with_exact_match_destination(self, mock_hook):
        SOURCE_FILES = [
            "test_object.txt",
            "test_object.txt.abc",
            "test_object.txt.copy/",
            "test_object.txt.folder/",
        ]
        DESTINATION_OBJ = f"{DESTINATION_OBJECT_PREFIX}/test_object.txt"

        mock_hook.return_value.list.return_value = SOURCE_FILES
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJ,
            exact_match=True,
        )

        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET, prefix="test_object.txt", delimiter=None, match_glob=None
            ),
        ]
        mock_hook.return_value.list.assert_has_calls(mock_calls)

        mock_calls_rewrite = [
            mock.call(
                TEST_BUCKET, "test_object.txt", DESTINATION_BUCKET, DESTINATION_OBJ
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_rewrite)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_prefix_and_suffix(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_MIDDLE,
            destination_bucket=DESTINATION_BUCKET,
        )

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="test", delimiter="object"
        )

    # copy with wildcard
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_wildcard_with_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX,
        )

        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET,
                "test_object/file1.txt",
                DESTINATION_BUCKET,
                "foo/bar/file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file2.txt",
                DESTINATION_BUCKET,
                "foo/bar/file2.txt",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_wildcard_with_destination_object_retained_prefix(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=f"{DESTINATION_OBJECT_PREFIX}/{SOURCE_OBJECT_WILDCARD_SUFFIX[:-1]}",
        )

        operator.execute(None)
        mock_calls_retained = [
            mock.call(
                TEST_BUCKET,
                "test_object/file1.txt",
                DESTINATION_BUCKET,
                "foo/bar/test_object/file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file2.txt",
                DESTINATION_BUCKET,
                "foo/bar/test_object/file2.txt",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_retained)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_wildcard_without_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
        )

        operator.execute(None)
        mock_calls_none = [
            mock.call(
                TEST_BUCKET,
                "test_object/file1.txt",
                DESTINATION_BUCKET,
                "test_object/file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file2.txt",
                DESTINATION_BUCKET,
                "test_object/file2.txt",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_wildcard_empty_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            destination_object="",
        )

        operator.execute(None)
        mock_calls_empty = [
            mock.call(
                TEST_BUCKET, "test_object/file1.txt", DESTINATION_BUCKET, "/file1.txt"
            ),
            mock.call(
                TEST_BUCKET, "test_object/file2.txt", DESTINATION_BUCKET, "/file2.txt"
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_empty)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_wildcard_with_destination_object_rename(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_SINGLE_FILE
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_NESTED,
            destination_bucket=DESTINATION_BUCKET,
            destination_object="test_file.txt",
        )

        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET, "test_object/file1.txt", DESTINATION_BUCKET, "test_file.txt"
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_last_modified_time(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=None,
        )

        operator.execute(None)
        mock_calls_none = [
            mock.call(
                TEST_BUCKET,
                "test_object/file1.txt",
                DESTINATION_BUCKET,
                "test_object/file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file2.txt",
                DESTINATION_BUCKET,
                "test_object/file2.txt",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_wc_with_last_modified_time_with_all_true_cond(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        mock_hook.return_value.is_updated_after.side_effect = [True, True, True]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=MOD_TIME_1,
        )

        operator.execute(None)
        mock_calls_none = [
            mock.call(
                TEST_BUCKET,
                "test_object/file1.txt",
                DESTINATION_BUCKET,
                "test_object/file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file2.txt",
                DESTINATION_BUCKET,
                "test_object/file2.txt",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_wc_with_last_modified_time_with_one_true_cond(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        mock_hook.return_value.is_updated_after.side_effect = [True, False, False]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=MOD_TIME_1,
        )

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET,
            "test_object/file1.txt",
            DESTINATION_BUCKET,
            "test_object/file1.txt",
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_wc_with_no_last_modified_time(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST[:-1]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=None,
        )

        operator.execute(None)
        mock_calls_none = [
            mock.call(
                TEST_BUCKET,
                "test_object/file1.txt",
                DESTINATION_BUCKET,
                "test_object/file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file2.txt",
                DESTINATION_BUCKET,
                "test_object/file2.txt",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_no_prefix_with_last_modified_time_with_true_cond(self, mock_hook):
        mock_hook.return_value.is_updated_after.return_value = True
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1,
        )

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
            DESTINATION_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_no_prefix_with_maximum_modified_time_with_true_cond(self, mock_hook):
        mock_hook.return_value.is_updated_before.return_value = True
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            maximum_modified_time=MOD_TIME_1,
        )

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
            DESTINATION_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_exe_last_modified_time_and_maximum_modified_time_with_true_cond(
        self, mock_hook
    ):
        mock_hook.return_value.is_updated_between.return_value = True
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1,
            maximum_modified_time=MOD_TIME_2,
        )

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
            DESTINATION_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_no_prefix_with_no_last_modified_time(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=None,
        )

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
            DESTINATION_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_no_prefix_with_last_modified_time_with_false_cond(self, mock_hook):
        mock_hook.return_value.is_updated_after.return_value = False
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1,
        )

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_is_older_than_with_true_cond(self, mock_hook):
        mock_hook.return_value.is_older_than.return_value = True
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1,
            maximum_modified_time=MOD_TIME_2,
            is_older_than=3600,
        )

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
            DESTINATION_BUCKET,
            SOURCE_OBJECT_NO_WILDCARD,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_more_than_1_wildcard(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_MULTIPLE_WILDCARDS,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX,
        )

        total_wildcards = operator.source_object.count(WILDCARD)

        error_msg = f"Only one wildcard '[*]' is allowed in source_object parameter. Found {total_wildcards}"

        with pytest.raises(AirflowException, match=error_msg):
            operator.execute(None)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_with_empty_destination_bucket(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=None,
            destination_object=DESTINATION_OBJECT_PREFIX,
        )

        with mock.patch.object(operator.log, "warning") as mock_warn:
            operator.execute(None)
            mock_warn.assert_called_once_with(
                "destination_bucket is None. Defaulting it to source_bucket (%s)",
                TEST_BUCKET,
            )
            assert operator.destination_bucket == operator.source_bucket

    # Tests the use of delimiter and source object as list
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_empty_source_objects(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_NO_FILE,
        )

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="", delimiter=None, match_glob=None
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_raises_exception_with_two_empty_list_inside_source_objects(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_TWO_EMPTY_STRING,
        )

        with pytest.raises(
            AirflowException,
            match="You can't have two empty strings inside source_object",
        ):
            operator.execute(None)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_single_item_in_source_objects(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_SINGLE_FILE,
        )
        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET,
            prefix=SOURCE_OBJECTS_SINGLE_FILE[0],
            delimiter=None,
            match_glob=None,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_multiple_items_in_source_objects(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_MULTIPLE_FILES,
        )
        operator.execute(None)
        mock_hook.return_value.list.assert_has_calls(
            [
                mock.call(
                    TEST_BUCKET,
                    prefix="test_object/file1.txt",
                    delimiter=None,
                    match_glob=None,
                ),
                mock.call(
                    TEST_BUCKET,
                    prefix="test_object/file2.txt",
                    delimiter=None,
                    match_glob=None,
                ),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_a_delimiter(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_NO_FILE,
            delimiter=DELIMITER,
        )
        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="", delimiter=DELIMITER, match_glob=None
        )

    # COPY
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_delimiter_and_destination_object(self, mock_hook):
        mock_hook.return_value.list.side_effect = [[], [], [SOURCE_OBJECTS_LIST[2]]]
        mock_hook.return_value.exists.return_value = False

        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_LIST,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT,
            delimiter=DELIMITER,
        )

        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET,
                SOURCE_OBJECTS_LIST[2],
                DESTINATION_BUCKET,
                DESTINATION_OBJECT + "file3.json",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_different_delimiter_and_destination_object(self, mock_hook):
        mock_hook.return_value.list.side_effect = [
            [SOURCE_OBJECTS_LIST[0]],
            [SOURCE_OBJECTS_LIST[1]],
            [],
        ]
        mock_hook.return_value.exists.return_value = False

        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_LIST,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT,
            delimiter=".txt",
        )

        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET,
                SOURCE_OBJECTS_LIST[0],
                DESTINATION_BUCKET,
                DESTINATION_OBJECT + "file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                SOURCE_OBJECTS_LIST[1],
                DESTINATION_BUCKET,
                DESTINATION_OBJECT + "file2.txt",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_executes_with_no_destination_bucket_and_no_destination_object(
        self, mock_hook
    ):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET, source_objects=SOURCE_OBJECTS_LIST
        )
        operator.execute(None)
        mock_calls = [
            mock.call(
                TEST_BUCKET, "test_object/file1.txt", TEST_BUCKET, "test_object/file1.txt"
            ),
            mock.call(
                TEST_BUCKET, "test_object/file2.txt", TEST_BUCKET, "test_object/file2.txt"
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file3.json",
                TEST_BUCKET,
                "test_object/file3.json",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_wc_with_last_modified_time_with_all_true_cond_no_file(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_OBJECTS_LIST
        mock_hook.return_value.is_updated_after.side_effect = [True, True, True]
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_NO_FILE,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=MOD_TIME_1,
        )

        operator.execute(None)
        mock_calls_none = [
            mock.call(
                TEST_BUCKET,
                "test_object/file1.txt",
                DESTINATION_BUCKET,
                "test_object/file1.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file2.txt",
                DESTINATION_BUCKET,
                "test_object/file2.txt",
            ),
            mock.call(
                TEST_BUCKET,
                "test_object/file3.json",
                DESTINATION_BUCKET,
                "test_object/file3.json",
            ),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_wildcard_with_replace_flag_false_with_destination_object(
        self, mock_hook
    ):
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_SUFFIX,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX,
            replace=False,
        )

        operator.execute(None)
        mock_calls = [
            mock.call(TEST_BUCKET, prefix="test_object", delimiter=""),
            mock.call(
                DESTINATION_BUCKET, prefix="foo/bar", delimiter="", match_glob=None
            ),
        ]
        mock_hook.return_value.list.assert_has_calls(mock_calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_execute_source_object_required_flag_true(self, mock_hook):
        mock_hook.return_value.exists.return_value = False
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_objects=SOURCE_OBJECTS_SINGLE_FILE,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX,
            source_object_required=True,
        )

        with pytest.raises(
            AirflowException,
            match=f"{SOURCE_OBJECTS_SINGLE_FILE} does not exist in bucket {TEST_BUCKET}",
        ):
            operator.execute(None)

    @pytest.mark.parametrize(
        "existing_objects, source_object, match_glob, exact_match, expected_source_objects, "
        "expected_destination_objects",
        [
            (
                ["source/foo.txt"],
                "source/foo.txt",
                None,
                True,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt"],
                "source/foo.txt",
                None,
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt"],
                "source",
                None,
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt"],
                "source/",
                None,
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt"],
                "source/*",
                None,
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt"],
                "source/foo.*",
                None,
                False,
                ["source/foo.txt"],
                ["{prefix}/txt"],
            ),
            (
                ["source/foo.txt"],
                "source/",
                "**/foo*",
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt"],
                "source/",
                "**/foo.txt",
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source/foo.txt",
                None,
                True,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source/foo.txt",
                None,
                False,
                ["source/foo.txt", "source/foo.txt.abc"],
                ["{prefix}/foo.txt", "{prefix}/foo.txt.abc"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source",
                None,
                False,
                ["source/foo.txt", "source/foo.txt.abc"],
                ["{prefix}/foo.txt", "{prefix}/foo.txt.abc"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source/",
                None,
                False,
                ["source/foo.txt", "source/foo.txt.abc"],
                ["{prefix}/foo.txt", "{prefix}/foo.txt.abc"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source/*",
                None,
                False,
                ["source/foo.txt", "source/foo.txt.abc"],
                ["{prefix}/foo.txt", "{prefix}/foo.txt.abc"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source/foo.*",
                None,
                False,
                ["source/foo.txt", "source/foo.txt.abc"],
                ["{prefix}/txt", "{prefix}/txt.abc"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source/",
                "**/foo*",
                False,
                ["source/foo.txt", "source/foo.txt.abc"],
                ["{prefix}/foo.txt", "{prefix}/foo.txt.abc"],
            ),
            (
                ["source/foo.txt", "source/foo.txt.abc"],
                "source/",
                "**/foo.txt",
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source/foo.txt",
                None,
                True,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source/foo.txt",
                None,
                False,
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                [
                    "{prefix}/foo.txt",
                    "{prefix}/foo.txt.abc",
                    "{prefix}/foo.txt/subfolder/file.txt",
                ],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source",
                None,
                False,
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                [
                    "{prefix}/foo.txt",
                    "{prefix}/foo.txt.abc",
                    "{prefix}/foo.txt/subfolder/file.txt",
                ],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source/",
                None,
                False,
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                [
                    "{prefix}/foo.txt",
                    "{prefix}/foo.txt.abc",
                    "{prefix}/foo.txt/subfolder/file.txt",
                ],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source/*",
                None,
                False,
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                [
                    "{prefix}/foo.txt",
                    "{prefix}/foo.txt.abc",
                    "{prefix}/foo.txt/subfolder/file.txt",
                ],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source/foo.*",
                None,
                False,
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                ["{prefix}/txt", "{prefix}/txt.abc", "{prefix}/txt/subfolder/file.txt"],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source/",
                "**/foo*",
                False,
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                [
                    "{prefix}/foo.txt",
                    "{prefix}/foo.txt.abc",
                    "{prefix}/foo.txt/subfolder/file.txt",
                ],
            ),
            (
                [
                    "source/foo.txt",
                    "source/foo.txt.abc",
                    "source/foo.txt/subfolder/file.txt",
                ],
                "source/",
                "**/foo.txt",
                False,
                ["source/foo.txt"],
                ["{prefix}/foo.txt"],
            ),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_copy_files_into_a_folder(
        self,
        mock_hook,
        existing_objects,
        source_object,
        match_glob,
        exact_match,
        expected_source_objects,
        expected_destination_objects,
    ):
        mock_hook.return_value.list.return_value = existing_objects
        operator = GCSToGCSOperator(
            task_id=TASK_ID,
            source_bucket=TEST_BUCKET,
            source_object=source_object,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX + "/",
            exact_match=exact_match,
            match_glob=match_glob,
        )
        operator.execute(None)

        mock_calls = [
            mock.call(
                TEST_BUCKET,
                src,
                DESTINATION_BUCKET,
                dst.format(prefix=DESTINATION_OBJECT_PREFIX),
            )
            for src, dst in zip(expected_source_objects, expected_destination_objects)
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls)

    @pytest.mark.parametrize(
        ("source_objects", "destination_object", "inputs", "outputs"),
        (
            (
                SOURCE_OBJECTS_SINGLE_FILE,
                None,
                [
                    Dataset(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=SOURCE_OBJECTS_SINGLE_FILE[0],
                    )
                ],
                [
                    Dataset(
                        namespace=f"gs://{DESTINATION_BUCKET}",
                        name=SOURCE_OBJECTS_SINGLE_FILE[0],
                    )
                ],
            ),
            (
                SOURCE_OBJECTS_SINGLE_FILE,
                "target.txt",
                [
                    Dataset(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=SOURCE_OBJECTS_SINGLE_FILE[0],
                    )
                ],
                [Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="target.txt")],
            ),
            (
                SOURCE_OBJECTS_SINGLE_FILE,
                "target_pre",
                [
                    Dataset(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=SOURCE_OBJECTS_SINGLE_FILE[0],
                    )
                ],
                [Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="/")],
            ),
            (
                SOURCE_OBJECTS_SINGLE_FILE,
                "dir/",
                [
                    Dataset(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=SOURCE_OBJECTS_SINGLE_FILE[0],
                    )
                ],
                [Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="dir")],
            ),
            (
                SOURCE_OBJECTS_LIST,
                "",
                [
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name=SOURCE_OBJECTS_LIST[0]),
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name=SOURCE_OBJECTS_LIST[1]),
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name=SOURCE_OBJECTS_LIST[2]),
                ],
                [
                    Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="/"),
                ],
            ),
            (
                [*SOURCE_OBJECTS_LIST, "dir/*"],
                "parent/pre_",
                [
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name=SOURCE_OBJECTS_LIST[0]),
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name=SOURCE_OBJECTS_LIST[1]),
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name=SOURCE_OBJECTS_LIST[2]),
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name="dir"),
                ],
                [Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="parent")],
            ),
            (
                SOURCE_OBJECTS_NO_FILE,
                "no_ending_slash",
                [Dataset(namespace=f"gs://{TEST_BUCKET}", name="/")],
                [Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="/")],
            ),
            (
                [
                    f"dir/{SOURCE_OBJECT_WILDCARD_PREFIX}",
                    f"dir/{SOURCE_OBJECT_WILDCARD_SUFFIX}",
                    f"dir/{SOURCE_OBJECT_WILDCARD_MIDDLE}",
                    f"dir/{SOURCE_OBJECT_WILDCARD_FILENAME}",
                    "dir/*",
                    "dir/",
                    "dir/pre_",
                ],
                "/",
                [
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name="dir"),
                ],
                [
                    Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="/"),
                ],
            ),
            (
                ["", "dir/pre", SOURCE_OBJECTS_SINGLE_FILE[0]],
                DESTINATION_OBJECT,
                [
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name="/"),
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name="dir"),
                    Dataset(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=SOURCE_OBJECTS_SINGLE_FILE[0],
                    ),
                ],
                [
                    Dataset(
                        namespace=f"gs://{DESTINATION_BUCKET}",
                        name=DESTINATION_OBJECT_PREFIX,
                    ),
                ],
            ),
            (
                [
                    "",
                    "dir/",
                ],
                None,
                [
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name="/"),
                    Dataset(namespace=f"gs://{TEST_BUCKET}", name="dir"),
                ],
                [
                    Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="/"),
                    Dataset(namespace=f"gs://{DESTINATION_BUCKET}", name="dir"),
                ],
            ),
        ),
        ids=(
            "single file without output",
            "single file with single file output",
            "single file with prefix output",
            "single file with dir output",
            "multiple file with empty output",
            "multiple file with prefix as output",
            "empty prefix with prefix as output",
            "directory + prefix or wildcard without output",
            "mixed prefixes and file paths with output dir",
            "empty prefix + directory without output",
        ),
    )
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
    def test_get_openlineage_facets_on_complete(
        self, mock_hook, source_objects, destination_object, inputs, outputs
    ):
        if source_objects and any(WILDCARD in obj for obj in source_objects):
            with pytest.warns(
                AirflowProviderDeprecationWarning, match="Usage of wildcard"
            ):
                operator = GCSToGCSOperator(
                    task_id=TASK_ID,
                    source_bucket=TEST_BUCKET,
                    source_objects=source_objects,
                    destination_bucket=DESTINATION_BUCKET,
                    destination_object=destination_object,
                )
        else:
            operator = GCSToGCSOperator(
                task_id=TASK_ID,
                source_bucket=TEST_BUCKET,
                source_objects=source_objects,
                destination_bucket=DESTINATION_BUCKET,
                destination_object=destination_object,
            )

        operator.execute(None)

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == len(inputs)
        assert len(lineage.outputs) == len(outputs)
        assert all(element in lineage.inputs for element in inputs)
        assert all(element in inputs for element in lineage.inputs)
        assert all(element in lineage.outputs for element in outputs)
        assert all(element in outputs for element in lineage.outputs)

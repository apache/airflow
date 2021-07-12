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

import unittest
from datetime import timedelta

from airflow.models import TaskNote
from airflow.utils import timezone
from tests.test_utils import db


class TestTaskNote(unittest.TestCase):
    def setUp(self):
        db.clear_db_task_notes()

    def tearDown(self):
        db.clear_db_task_notes()

    def test_task_note_retrieval(self):
        """
        Test basic task note retrieval
        """
        user = "user1"
        execution_date = timezone.utcnow()
        timestamp = timezone.utcnow()
        dag_id = "test_dag1"
        task_id = "test_task1"
        task_note = "the task note"
        TaskNote.set(
            user_name=user,
            timestamp=timestamp,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note,
        )

        ret_value = TaskNote.get_many(
            user_names=user,
            dag_ids=dag_id,
            task_ids=task_id,
            execution_date=execution_date,
            timestamp=timestamp,
        ).first()

        assert ret_value.task_note == task_note

    def test_task_note_delete(self):
        """
        Test basic task note retrieval
        """
        user = "user1"
        execution_date = timezone.utcnow()
        timestamp = timezone.utcnow()
        dag_id = "test_dag1"
        task_id = "test_task1"
        task_note = "the task note"

        note = TaskNote(
            user_name=user,
            timestamp=timestamp,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note,
        )

        retrieved_note = TaskNote.get_one(
            user_names=user,
            dag_ids=dag_id,
            task_ids=task_id,
            execution_date=execution_date,
            timestamp=timestamp,
        )

        # Checks that the task not has been correctly added
        assert retrieved_note == note

        TaskNote.delete(retrieved_note)

        assert (
            TaskNote.get_many(
                user_names=user,
                dag_ids=dag_id,
                task_ids=task_id,
                execution_date=execution_date,
                timestamp=timestamp,
            ).count()
            == 0
        )

    def test_task_note_get_many_filter_by_execution_date(self):
        """
        Test basic task note retrieval
        """
        user = "user1"
        execution_date1 = timezone.utcnow()
        execution_date2 = timezone.utcnow()
        timestamp = timezone.utcnow()
        dag_id = "test_dag1"
        task_id = "test_task1"
        task_note1 = "the task note1"
        task_note2 = "the task note2"

        TaskNote.set(
            user_name=user,
            timestamp=timestamp,
            execution_date=execution_date1,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note1,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp,
            execution_date=execution_date2,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note2,
        )

        ret_value = TaskNote.get_many(
            user_names=user,
            dag_ids=dag_id,
            task_ids=task_id,
            execution_date=execution_date1,
            timestamp=timestamp,
        )

        assert ret_value.count() == 1 and ret_value[0].task_note == task_note1

    def test_task_note_get_many_filter_by_timestamp(self):
        """
        Test basic task note retrieval
        """
        user = "user1"
        execution_date = timezone.utcnow()
        timestamp1 = timezone.utcnow()
        timestamp2 = timezone.utcnow()

        dag_id = "test_dag1"
        task_id = "test_task1"
        task_note1 = "the task note1"
        task_note2 = "the task note2"

        TaskNote.set(
            user_name=user,
            timestamp=timestamp1,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note1,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp2,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note2,
        )

        ret_value = TaskNote.get_many(
            user_names=user,
            dag_ids=dag_id,
            task_ids=task_id,
            execution_date=execution_date,
            timestamp=timestamp1,
        )

        assert ret_value.count() == 1 and ret_value[0].task_note == task_note1

    def test_task_note_get_many_filter_by_usernames(self):
        """
        Test basic task note retrieval
        """
        user1 = "user1"
        user2 = "user2"
        user3 = "user3"

        execution_date = timezone.utcnow()
        timestamp1 = timezone.utcnow()
        timestamp2 = timezone.utcnow()
        timestamp3 = timezone.utcnow()

        dag_id = "test_dag1"
        task_id = "test_task1"
        task_note1 = "the task note1"
        task_note2 = "the task note2"
        task_note3 = "the task note3"

        TaskNote.set(
            user_name=user1,
            timestamp=timestamp1,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note1,
        )

        TaskNote.set(
            user_name=user2,
            timestamp=timestamp2,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note2,
        )

        TaskNote.set(
            user_name=user3,
            timestamp=timestamp3,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note3,
        )

        user_1_and_2_notes = TaskNote.get_many(
            user_names=[user1, user2], dag_ids=dag_id, task_ids=task_id, execution_date=execution_date
        )
        user_3_notes = TaskNote.get_many(
            user_names=user3, dag_ids=dag_id, task_ids=task_id, execution_date=execution_date
        )

        assert user_3_notes.count() == 1 and user_3_notes[0].task_note == task_note3
        assert user_1_and_2_notes.count() == 2 and {task_note2, task_note1} == {
            user_1_and_2_notes[0].task_note,
            user_1_and_2_notes[1].task_note,
        }

    def test_task_note_get_many_filter_by_dag_ids(self):
        """
        Test basic task note retrieval
        """
        dag_id1 = "dag1"
        dag_id2 = "dag2"
        dag_id3 = "dag3"

        execution_date = timezone.utcnow()
        timestamp1 = timezone.utcnow()
        timestamp2 = timezone.utcnow()
        timestamp3 = timezone.utcnow()

        user = "user"
        task_id = "test_task1"
        task_note1 = "the task note1"
        task_note2 = "the task note2"
        task_note3 = "the task note3"

        TaskNote.set(
            user_name=user,
            timestamp=timestamp1,
            execution_date=execution_date,
            dag_id=dag_id1,
            task_id=task_id,
            task_note=task_note1,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp2,
            execution_date=execution_date,
            dag_id=dag_id2,
            task_id=task_id,
            task_note=task_note2,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp3,
            execution_date=execution_date,
            dag_id=dag_id3,
            task_id=task_id,
            task_note=task_note3,
        )

        dag_1_2_notes = TaskNote.get_many(
            user_names=user, dag_ids=[dag_id1, dag_id2], task_ids=task_id, execution_date=execution_date
        )
        dag_3_notes = TaskNote.get_many(
            user_names=user, dag_ids=dag_id3, task_ids=task_id, execution_date=execution_date
        )

        assert dag_3_notes.count() == 1 and dag_3_notes[0].task_note == task_note3
        assert dag_1_2_notes.count() == 2 and {task_note2, task_note1} == {
            dag_1_2_notes[0].task_note,
            dag_1_2_notes[1].task_note,
        }

    def test_task_note_get_many_filter_by_task_ids(self):
        """
        Test basic task note retrieval
        """
        task_id1 = "task1"
        task_id2 = "task2"
        task_id3 = "task3"

        execution_date = timezone.utcnow()
        timestamp1 = timezone.utcnow()
        timestamp2 = timezone.utcnow()
        timestamp3 = timezone.utcnow()

        user = "user"
        dag_id = "dag"

        task_note1 = "the task note1"
        task_note2 = "the task note2"
        task_note3 = "the task note3"

        TaskNote.set(
            user_name=user,
            timestamp=timestamp1,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id1,
            task_note=task_note1,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp2,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id2,
            task_note=task_note2,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp3,
            execution_date=execution_date,
            dag_id=dag_id,
            task_id=task_id3,
            task_note=task_note3,
        )

        task_1_2_notes = TaskNote.get_many(
            user_names=user, dag_ids=dag_id, task_ids=[task_id1, task_id2], execution_date=execution_date
        )
        task_3_notes = TaskNote.get_many(
            user_names=user, dag_ids=dag_id, task_ids=task_id3, execution_date=execution_date
        )

        assert task_3_notes.count() == 1 and task_3_notes[0].task_note == task_note3
        assert task_1_2_notes.count() == 2 and {task_1_2_notes[0].task_note, task_1_2_notes[1].task_note} == {
            task_note2,
            task_note1,
        }

    def test_task_note_get_many_include_prior_dates(self):
        """
        Test basic task note retrieval
        """
        execution_date1 = timezone.utcnow()
        execution_date2 = execution_date1 - timedelta(days=1)
        execution_date3 = execution_date1 - timedelta(days=2)

        timestamp1 = timezone.utcnow()
        timestamp2 = timezone.utcnow()
        timestamp3 = timezone.utcnow()

        user = "user"
        dag_id = "dag"
        task_id = "task1"

        task_note1 = "the task note1"
        task_note2 = "the task note2"
        task_note3 = "the task note3"

        TaskNote.set(
            user_name=user,
            timestamp=timestamp1,
            execution_date=execution_date1,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note1,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp2,
            execution_date=execution_date2,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note2,
        )

        TaskNote.set(
            user_name=user,
            timestamp=timestamp3,
            execution_date=execution_date3,
            dag_id=dag_id,
            task_id=task_id,
            task_note=task_note3,
        )

        execution_date_2_3_notes = TaskNote.get_many(
            user_names=user,
            dag_ids=dag_id,
            task_ids=task_id,
            execution_date=execution_date2,
            include_prior_dates=True,
        )

        execution_date_1_notes = TaskNote.get_many(
            user_names=user,
            dag_ids=dag_id,
            task_ids=task_id,
            execution_date=execution_date1,
            include_prior_dates=False,
        )

        assert execution_date_1_notes.count() == 1 and execution_date_1_notes[0].task_note == task_note1
        assert (
            execution_date_2_3_notes.count() == 2
            and {
                execution_date_2_3_notes[0].task_note,
                execution_date_2_3_notes[1].task_note,
            }
            == {task_note2, task_note3}
        )

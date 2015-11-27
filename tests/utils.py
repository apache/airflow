import unittest

from airflow.utils import TriggerRule, TriggerType, State, AirflowException


class TriggerRuleTest(unittest.TestCase):

    def test_trigger_type(self):

        TR = TriggerRule
        TT = TriggerType

        assert TR.trigger_type(TR.ALL_SUCCESS) == TT.ALL
        assert TR.trigger_type(TR.ALL_FAILED) == TT.ALL
        assert TR.trigger_type(TR.ALL_DONE) == TT.ALL
        assert TR.trigger_type(TR.ONE_SUCCESS) == TT.ANY
        assert TR.trigger_type(TR.ONE_FAILED) == TT.ANY
        assert TR.trigger_type(TR.DUMMY) == TT.DUMMY

    def test_trigger_state(self):

        TR = TriggerRule

        assert TR.target_state(TR.ALL_SUCCESS) == State.SUCCESS
        assert TR.target_state(TR.ALL_FAILED) == State.FAILED
        assert TR.target_state(TR.ONE_SUCCESS) == State.SUCCESS
        assert TR.target_state(TR.ONE_FAILED) == State.FAILED

        regexp = "(.*)should not be called with `ALL_DONE`(.*)"
        with self.assertRaisesRegexp(AirflowException, regexp):
            TR.target_state(TR.ALL_DONE)

        regexp = "(.*)should not be called with `DUMMY`(.*)"
        with self.assertRaisesRegexp(AirflowException, regexp):
            TR.target_state(TR.DUMMY)

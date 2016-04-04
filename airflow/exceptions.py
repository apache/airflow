

class AirflowBaseException(Exception):
    pass

#
# Any AirflowException raised is expected to cause the TaskInstance to be marked in an ERROR state
#
class AirflowException(AirflowBaseException):
    pass


class AirflowSensorTimeout(AirflowException):
    pass


class AirflowTaskTimeout(AirflowException):
    pass


#
# Any AirflowSkipException raised is expected to cause the TaskInstance to be marked in an SKIPPED state
#
class AirflowSkipException(AirflowBaseException):
    pass


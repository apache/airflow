import logging
import json


class JsonFormatter(logging.Formatter):
    def __init__(self, record_labels, extras={}):
        super(JsonFormatter, self).__init__()
        self.record_labels = record_labels
        self.extras = extras

    def _merge_dicts(self, d1, d2):
        merged = d1.copy()
        merged.update(d2)
        return merged

    def format(self, record):
        record_dict = {label: getattr(record, label)
                       for label in self.record_labels}
        merged_record = self._merge_dicts(record_dict, self.extras)
        return json.dumps(merged_record)


def create_formatter(record_labels, extras={}):
    return JsonFormatter(record_labels, extras)

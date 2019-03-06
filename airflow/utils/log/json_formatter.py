import logging
import json


class JSONFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', json_fields=[], extras={}):
        super(JSONFormatter, self).__init__(fmt, datefmt, style)
        self.json_fields = json_fields
        self.extras = extras

    def _merge_dicts(self, d1, d2):
        merged = d1.copy()
        merged.update(d2)
        return merged

    def format(self, record):
        super(JSONFormatter, self).format(record)
        record_dict = {label: getattr(record, label, None)
                       for label in self.json_fields}
        merged_record = self._merge_dicts(record_dict, self.extras)
        return json.dumps(merged_record)

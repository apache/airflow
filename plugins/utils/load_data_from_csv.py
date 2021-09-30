# -*- coding:utf-8 -*-
import csv


def load_data_from_csv(file_name: str, data_keys=None) -> list:
    with open(file_name, newline='') as file:
        reader = csv.DictReader(file)
        data = []
        if not data_keys:
            for row in reader:
                data.append(dict(row))
            return data
        for row in reader:
            row_data = {}
            for k, v in data_keys.items():
                row_data.update({
                    k: row[v]
                })
            data.append(row_data)
        return data

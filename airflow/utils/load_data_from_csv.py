# -*- coding:utf-8 -*-
import csv


def load_data_from_csv(file_name: str, data_keys=None) -> list:
    if not data_keys:
        return []

    with open(file_name, newline='') as file:
        reader = csv.DictReader(file)
        data = []
        for row in reader:
            row_data = {}
            for k, v in data_keys.items():
                row_data.update({
                    k: row[v]
                })
            data.append(row_data)
        return data

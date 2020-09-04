# -*- coding:utf-8 -*-
import csv


def get_controllers(file_name: str) -> list:
    print('reading file: {}'.format(file_name))
    with open(file_name, newline='') as file:
        reader = csv.DictReader(file)
        controllers = []
        for row in reader:
            controllers.append({
                'controller_name': row['控制器名称'],
                'line_code': row['工段编号'],
                'work_center_code': row['工位编号'],
                'line_name': row['工段名称'],
                'work_center_name': row['工位名称'],
            })
        return controllers


FILE_NAME = './default_controllers.csv'

if __name__ == '__main__':
    print(get_controllers(FILE_NAME))
